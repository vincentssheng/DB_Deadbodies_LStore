from lstore.index import Index
from lstore.config import *
from time import time
import os, json

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
    def __init__(self, name, num_columns, key, bufferpool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} # dictionary that maps rid to (range #, page_set #, offset)
        self.key_directory = {} # dictionary that maps key to (range #, page_set #, offset)
        self.index = Index(self)
        self.latest_range_index = -1
        self.bufferpool = bufferpool
        if not os.path.exists(os.getcwd() + "/" + name):
            os.makedirs(name)

        pgdir_file = os.getcwd() + "/pgdir.json"
        file = open(pgdir_file, "w+")
        file.close()
        if os.stat(pgdir_file).st_size > 0:
            with open(pgdir_file, "rb") as fp:
                self.page_directory = json.loads(fp.read())
            fp.close()
        
        keydir_file = os.getcwd() + "/keydir.json"
        file = open(keydir_file, "w+")
        file.close()
        if os.stat(keydir_file).st_size > 0:
            with open(keydir_file, "rb") as fp:
                self.key_directory = json.loads(fp.read())
            fp.close()

    def unload_dirs(self):
        with open(os.getcwd()+'/pgdir.json', "w") as fp:
            json.dump(self.page_directory, fp)
        fp.close()

        with open(os.getcwd()+'/keydir.json', "w") as fp:
            json.dump(self.key_directory, fp)
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

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
