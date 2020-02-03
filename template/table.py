from template.index import Index
from template.page import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    # static variable
    base_current_rid = 0 
    tail_current_rid = 0 + Config.NUM_SETS_PER_RANGE * Config.NUM_RECORDS_PER_SET
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
        self.ranges[0].append([Page() for i in range(self.num_columns+Config.NUM_META_COLS)])

    # validate and assigns rid
    def assign_rid(self, method):
        if method == 'insert':
            if ((self.base_current_rid + 1) % Config.NUM_RECORDS_PER_RANGE) <= Config.NUM_BASE_PER_RANGE: # rid belongs to bp
                self.base_current_rid += 1
            else: # rid belongs to tp
                self.base_current_rid += Config.NUM_TAIL_PER_RANGE + 1
        else: # method == 'update'
            if ((self.base_current_rid + 1) % Config.NUM_RECORDS_PER_RANGE) <= Config.NUM_BASE_PER_RANGE: # rid belongs to bp
                self.tail_current_rid += Config.NUM_BASE_PER_RANGE + 1
            else: # rid belongs to tp
                self.base_current_rid += 1

    # calculate physical location based on RID
    def calculate_phys_location(self, rid):
            range_number = rid / Config.NUM_RECORDS_PER_RANGE
            set_number = ((rid - 1) % Config.NUM_RECORDS_PER_RANGE) / Config.NUM_RECORDS_PER_SET
            offset = (rid - 1) % Config.NUM_RECORDS_PER_SET
            return (int(range_number), int(set_number), int(offset))

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
