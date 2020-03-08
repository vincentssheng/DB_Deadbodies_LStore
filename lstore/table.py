from lstore.index import Index
from lstore.config import *
import os, json, threading, time
from lstore.page import *
from collections import defaultdict
from datetime import datetime
import itertools

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class LockManager:

    def __init__(self):
        self.manager = {}

    def acquire(self, rid, thread_id, lock_type):
        if rid not in self.manager.keys():
            self.manager[rid] = {}

        
        if lock_type == 'S':
            for (tid, l) in self.manager[rid].items():
                if tid != thread_id:
                    if l >= 2:
                        #print(str(thread_id) + ", " + str(rid) + ", " + lock_type + " denied")
                        return False
            
            if thread_id not in self.manager[rid].keys():
                self.manager[rid][thread_id] = 1
            #print(str(thread_id) + ", " + str(rid) + ", " + lock_type + " granted")
        else:
            for (tid, l) in self.manager[rid].items():
                if tid != thread_id:
                    if l >= 1:
                        #print(str(thread_id) + ", " + str(rid) + ", " + lock_type + " denied")
                        return False
            if thread_id not in self.manager[rid].keys():
                #print(str(thread_id) + ", " + str(rid) + ", " + lock_type + " granted")
                self.manager[rid][thread_id] = 2
            else:
                if self.manager[rid][thread_id] == 1:
                    #print(str(thread_id) + ", " + str(rid) + ", " + lock_type + "-> X")
                    self.manager[rid][thread_id] = 2

        return True

    def release(self, thread_id):
        for (_, locks) in self.manager.items():
            if thread_id in locks.keys():
                #print("releasing locks")
                del locks[thread_id]

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, key, num_columns, 
                    bufferpool, latest_range_index, base_current_rid, 
                    tail_current_rid, tail_tracker, 
                    merge_tracker, base_tracker, method='create', verbose=False):
        
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # dictionary that maps rid to (range #, page_set #, offset)
        self.key_directory = {} # dictionary that maps key to (range #, page_set #, offset)
        self.index = Index(self)
        self.latest_range_index = latest_range_index
        self.bufferpool = bufferpool
        self.base_current_rid = base_current_rid
        self.tail_current_rid = tail_current_rid
        self.tail_tracker = tail_tracker
        self.merge_tracker = merge_tracker
        self.base_tracker = base_tracker
        self.pd_lock = threading.Lock()
        self.verbose = verbose
        self.lock = LockManager()
        self.lm_lock = threading.Lock()

        if method == 'create':
            if not os.path.exists(os.getcwd() + "/" + name):
                os.makedirs(name)
        os.chdir(name)

        if method == 'get':
            pgdir_file = os.getcwd() + "/pgdir.json"
            if not os.path.exists(pgdir_file):
                file = open(pgdir_file, "w+")
                file.close()
            else:
                with open(pgdir_file, "rb") as fp:
                    pgdir_data = json.loads(fp.read())
                    self.page_directory = {int(k):v for k,v in pgdir_data.items()}
                fp.close()
            
            keydir_file = os.getcwd() + "/keydir.json"
            if not os.path.exists(keydir_file):
                file = open(keydir_file, "w+")
                file.close()
            else:
                with open(keydir_file, "rb") as fp:
                    key_data = json.loads(fp.read())
                    self.key_directory = {int(k):v for k,v in key_data.items()}
                fp.close()

            pri_key_file = os.getcwd() + '/pri_index.json'
            if not os.path.exists(pri_key_file):
                file = open(pri_key_file, 'w+')
                file.close()
            else:
                with open(pri_key_file, 'rb') as fp:
                    pri_key_data = json.loads(fp.read())
                    self.index.indexes[Config.NUM_META_COLS+self.key] = {int(k):v for k,v in pri_key_data.items()}
                fp.close()

        # Background thread stuff
        self.interval = Config.MERGE_INTERVAL
        self.thread = threading.Thread(target=self.__merge, args=())
        self.thread.daemon = True
        self.thread.start()

    def unload_meta(self):

        meta_dict = {}
        meta_dict.update({'key': self.key})
        meta_dict.update({'num_columns': self.num_columns})
        meta_dict.update({'latest_range': self.latest_range_index})
        meta_dict.update({'base_rid': self.base_current_rid})
        meta_dict.update({'tail_rid': self.tail_current_rid})
        meta_dict.update({'tail_tracker': self.tail_tracker})
        meta_dict.update({'merge_tracker': self.merge_tracker})
        meta_dict.update({'base_tracker': self.base_tracker})
        with open(os.getcwd()+'/metadata.json', "w") as fp:
            json.dump(meta_dict, fp)
        fp.close()

        with open(os.getcwd()+'/pgdir.json', "w") as fp:
            json.dump(self.page_directory, fp)
        fp.close()

        with open(os.getcwd()+'/keydir.json', "w") as fp:
            json.dump(self.key_directory, fp)
        fp.close()

        with open(os.getcwd()+'/pri_index.json', 'w') as fp:
            json.dump(self.index.indexes[Config.NUM_META_COLS+self.key], fp)
        fp.close()

    # validate and assigns rid
    def assign_rid(self, method):
        if method == 'insert':
            if self.base_current_rid + 1 < self.tail_current_rid:
                self.base_current_rid += 1
            else:
                print("Maximum capacity reached, cannot insert.")
        else: # method == 'update'
            if self.tail_current_rid - 1 > self.base_current_rid:
                self.tail_current_rid -= 1
            else:
                print("Maximum capacity reached, cannot update.")
                
    # calculate physical location based on RID
    def calculate_base_location(self, rid):
        range_number = (rid - 1) / Config.NUM_BASE_RECORDS_PER_RANGE
        set_number = ((rid - 1) % Config.NUM_BASE_RECORDS_PER_RANGE) / Config.NUM_RECORDS_PER_SET
        offset = (rid - 1) % Config.NUM_RECORDS_PER_SET

        return (int(range_number), 0, int(set_number), int(offset))

    def create_range(self, range):
        path = os.getcwd() + "/r_" + str(range) + "/" + str(0)
        if not os.path.exists(path):
            os.makedirs(path)

    def create_sets(self, set, bt):
        path = os.getcwd() + "/" + self.name + "/r_" + str(range) + "/" + str(bt) + "/s_" + str(set)
        if not os.path.exists(path):
            os.makedirs(path)
        for i in range(Config.NUM_META_COLS+self.num_columns):
            file = open(path + "/p_" + str(i) + ".txt", "w+")
            file.close()

    """
    bp and tp are parallel lists of base pages and tail pages
    """
    def merge(self, bp, tp):
        base_rids = defaultdict(lambda:-1)
        # get latest tail record RID and assign it to lineage
        latest_tps = int.from_bytes(tp[0].read(tp[0].num_records-1), sys.byteorder)
        for i in range(bp[0].num_records):
            bp[Config.TIMESTAMP_COLUMN-1].write(i, 
                                int(time.mktime(datetime.now().timetuple())).to_bytes(Config.ENTRY_SIZE, sys.byteorder))
            bp[Config.SCHEMA_ENCODING_COLUMN-1].write(i, int(0).to_bytes(Config.ENTRY_SIZE, sys.byteorder))
            bp[Config.BASE_RID_COLUMN-1].write(i, bp[0].read(i))

        # merge records
        for offset in range(tp[1].num_records-1, -1, -1):
            base_rid = int.from_bytes(tp[1].read(offset), sys.byteorder) # base_rid is always at 1st element
            # if we have not merge this record
            if base_rids[base_rid] == -1:
                base_rids.update({base_rid: 1})
                (_, _, _, base_offset) = self.page_directory[base_rid]               
                for i in range(2, len(tp)): # copy all columns over
                    bp[i+2].write(base_offset, tp[i].read(offset))

        for page in bp:
            page.lineage = latest_tps
        return bp

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        while True:
            mergeQ = [] # Merge Queue contains indexes of ranges that are ready for merging
            tp = [[] for i in range(len(self.merge_tracker))] # Pages is the list of tail pages
            consolidated_bp = []

            if self.verbose:
                print("Merge Tracker")
                print(self.merge_tracker)
                print("Tail Tracker")
                print(self.tail_tracker)
                print("Merge Queue")
                print(mergeQ)
                print("==================================")

            # Check each tail page's capacity and insert into merge queue if the tail page is full
            for (i, set) in enumerate(self.merge_tracker):

                # Check if there are tail pages available for merging.
                if set >= self.tail_tracker[i]:
                    continue

                # Check capacity of tail page
                page = self.bufferpool.find_page(self.name, i, 1, set, Config.RID_COLUMN)
                if page.has_capacity():
                    continue # do not merge non-full pages

                page.pin_count += 1 # if we need to do the merge, make sure it stays in pool
                tp[i].append(page)
                
                """
                if tail page is full and
                if we have not merged the latest tail page in the range
                """
                for j in range(Config.BASE_RID_COLUMN, Config.NUM_META_COLS+self.num_columns):
                    page = self.bufferpool.find_page(self.name, i, 1, set, j)
                    page.pin_count += 1
                    tp[i].append(page) # retrieve all tail pages

                mergeQ.append(i) # append range index to merge queue

            # while we have outstanding indexes to merge
            # merge them one set at a time
            while len(mergeQ) > 0:
                index = mergeQ.pop(0)
                bp = []
                """
                for each range's base page
                merge only if the base page is not full
                """
                page = self.bufferpool.find_page(self.name, index, 0, self.base_tracker[index], Config.RID_COLUMN)
                if page.has_capacity(): # if base page is not full, skip
                    continue
                page.pin_count += 1
                bp.append(page)

                for j in range(Config.TIMESTAMP_COLUMN, Config.NUM_META_COLS+self.num_columns):
                    if (j == Config.SCHEMA_ENCODING_COLUMN) and (j == Config.TIMESTAMP_COLUMN) and (j == Config.BASE_RID_COLUMN):
                        continue # we don't read the SE, Timestamp and BASE RID columns for merge
                    page = self.bufferpool.find_page(self.name, index, 0, self.base_tracker[index], j)
                    page.pin_count += 1
                    bp.append(page)

                consolidated_bp = self.merge(bp, tp[index])

                # Create files and write to disk at the end of merge cycle
                self.base_tracker[index] += 1
                self.merge_tracker[index] += 1
                new_base_path = os.getcwd() + '/r_' + str(index) + '/0/s_' + str(self.base_tracker[index])
                if not os.path.exists(new_base_path):
                    os.makedirs(new_base_path)
                for i in range(0, Config.NUM_META_COLS+self.num_columns-1):
                    file = open(new_base_path+'/p_'+str(i+1)+'.txt', 'w')
                    file.write(str(consolidated_bp[i].lineage) + '\n')
                    data_str = ""
                    for j in range(consolidated_bp[i].num_records):
                        data_str += str(int.from_bytes(consolidated_bp[i].read(j), sys.byteorder)) + " "

                    file.write(data_str)
                    file.close()

                # remove pins on pages used during merge cycle
                for page in bp:
                    page.pin_count -= 1
                for page in tp[index]:
                    page.pin_count -= 1

                # Swap consolidated page into page directory
                self.pd_lock.acquire()
                
                for i in range(consolidated_bp[0].num_records):
                    rid = int.from_bytes(consolidated_bp[0].read(i), sys.byteorder)
                    self.page_directory[rid] = (index, 0, self.base_tracker[index], i)
                    self.key_directory[rid] = (index, self.base_tracker[index], i)
                self.pd_lock.release()
                

            time.sleep(self.interval)
 
