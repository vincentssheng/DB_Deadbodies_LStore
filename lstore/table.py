from lstore.index import Index
from lstore.page import *
from time import time
from collections import defaultdict
import os
import pickle

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Bufferpool:

    def __init__(self, table):
        self.table = table
        self.empty = [i for i in range(Config.POOL_MAX_LEN)]  
        self.used = []
        self.pool = [Page("", ()) for i in range(Config.POOL_MAX_LEN)] # Create empty shell pages
        self.directory = defaultdict(lambda: -1)

    def flush_pool(self):
        for page in self.pool:
            if page.dirty:
                file = open(page.path, "w")
                data_str = ""
                for i in range(page.num_records):
                    data_str += str(int.from_bytes(page.read(i), sys.byteorder)) + " "

                file.write(data_str)
                file.close()
    
    def retrieve(self, path, location):
       
        # if file not empty
        if os.stat(path).st_size > 0:
            file = open(path, "r")
            data_str = file.readlines()
            data_lst = data_str[0].split() # we always store a line
            file.close()
            data = [int(i) for i in data_lst]
            page = Page(path, location)
            for i in range(len(data)):
                page.write(i, data[i].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        else:
            page = Page(path, location)

        page.dirty = False
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.pool[empty_index] = page

        return empty_index

    def evict(self):
        evict_index = self.used.pop(0)
        self.empty.append(evict_index)

        if self.pool[evict_index].dirty:
            file = open(self.pool[evict_index].path, "w")
            data_str = ""
            for i in range(self.pool[evict_index].num_records):
                data_str += str(int.from_bytes(self.pool[evict_index].read(i), sys.byteorder)) + " "

            file.write(data_str)
            file.close()

        del self.directory[self.pool[evict_index].location]

    def find_index(self, table, range, bt, set, page):
        i = self.directory[(table, range, bt, set, page)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
            
            path = os.getcwd() + "/" + self.table.name + "/r_" + str(range) + "/" + str(bt) + "/s_" + str(set) + "/p_" + str(page) + ".txt"
            i = self.retrieve(path, (table, range, bt, set, page))
            self.directory.update({(table, range, bt, set, page): i})
        
        return i

class Table:

    # static variable
    base_current_rid = 0 
    tail_current_rid = Config.MAX_RID
    tail_tracker = [] # tracks the latest tail set ID for each range
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # dictionary that maps rid to (range #, page_set #, offset)
        self.key_directory = {} # dictionary that maps key to (range #, page_set #, offset)
        self.index = Index(self)
        self.latest_range_index = -1
        self.bufferpool = Bufferpool(self)
        if not os.path.exists(os.getcwd() + "/" + name):
            os.makedirs(name)

        pgdir_file = os.getcwd() + "/pgdir.txt"
        file = open(pgdir_file, "w+")
        file.close()
        if os.stat(pgdir_file).st_size > 0:
            with open(pgdir_file, "rb") as handle:
                self.page_directory = pickle.loads(handle.read())
            handle.close()
        
        keydir_file = os.getcwd() + "keydir.txt"
        file = open(keydir_file, "w+")
        file.close()
        if os.stat(keydir_file).st_size > 0:
            with open(keydir_file, "rb") as handle:
                self.key_directory = pickle.loads(handle.read())
            handle.close()

    def unload_dirs(self):
        pgdir_file = os.getcwd() + "/pgdir.txt"
        with open(pgdir_file, "wb") as handle:
            pickle.dump(self.page_directory, handle)
        handle.close()

        keydir_file = os.getcwd() + "/keydir.txt"
        with open(keydir_file, "wb") as handle:
            pickle.dump(self.key_directory, handle)
        handle.close()

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

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
