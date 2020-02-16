from lstore.index import Index
from lstore.page import *
from time import time
from collections import defaultdict
import os

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Bufferpool:

    def __init__(self):
        self.empty = [i for i in range(Config.POOL_MAX_LEN)]  
        self.used = []
        self.pool = []
        self.directory = defaultdict(lambda: -1)
    
    def retrieve(self, path):
       
        file = open(path, "r")
        data_str = file.readlines().split()
        data = [int(i) for i in data_str]
        page = Page(path)
        for i in range(len(data)):
            page.write(i, data[i].to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        page.dirty = False
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.pool[empty_index] = page

        return empty_index

    def evict(self):
        evict_index = self.used.pop()
        self.empty.push(evict_index)

        if self.pool[evict_index].dirty:
            file = open(self.pool[evict_index].path)
            data_str = ""
            for i in range(self.pool[evict_index].num_records):
                data_str += str(int.from_bytes(self.pool[evict_index].read(i), sys.byteorder)) + " "

            file.write(data_str)

    def find_index(self, table, range, bt, set, page):
        i = self.directory[(range, bt, set, page)]

        if i == -1:
            if len(self.pool) == Config.POOL_MAX_LEN:
                self.evict()
            
            path = os.getcwd() + "/r_" + str(range) + "/" + str(bt) + "/s_" + str(set) + "/p_" + str(page) + ".txt"
            i = self.retrieve(path)
        
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

        # triple list that holds the pages
        # page_ranges[i][j][k] corresponds to
        # ith page range
        # kth set of pages in the ith page range
        # kth column of jth set of pages
        # the offset is the physical location of the record in this set of pages
        # start with one range and page
        os.mkdir(name)

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

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
