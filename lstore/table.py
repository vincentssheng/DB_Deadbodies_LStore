from lstore.index import Index
from lstore.page import *
from time import time

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
    ranges = []
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
        # jth set of pages in the ith page range
        # kth column of jth set of pages
        # the offset is the physical location of the record in this set of pages
        # start with one range and page
        self.ranges.append([])
        self.ranges[0].append([])
        self.ranges[0][0].append([Page() for i in range(self.num_columns+Config.NUM_META_COLS)])
        self.tail_tracker.append(-1)

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
 
