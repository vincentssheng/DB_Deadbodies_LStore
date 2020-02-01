from template.page import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    # static variable
    base_current_rid = 1 
    tail_current_rid = 1 + Config.NUM_SETS_PER_RANGE * Config.NUM_RECORDS_PER_SET
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

        # triple list that holds the pages
        # page_ranges[i][j][k] corresponds to
        # ith page range
        # jth set of pages in the ith page range
        # kth column of jth set of pages
        # the offset is the physical location of the record in this set of pages
        self.ranges = []
        # start with one range and page
        self.ranges.append([]) # add a new range
        self.add_page(self.ranges[0], 1) # add a new page


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

    # initialize a page range
    def init_range(self, current_rid):
        # new range
        if (current_rid-1) / Config.NUM_RECORDS_PER_RANGE >= len(self.ranges):
            self.ranges.append([]) # add a new range
            
            
    # create a new set of pages
    def add_page(self, range, current_rid):
        if current_rid % Config.NUM_RECORDS_PER_SET == 1:
            range.append([Page() for i in range(self.num_columns+Config.NUM_META_COLS)]) # initialize NUM_META_COLS + num_columns pages

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
