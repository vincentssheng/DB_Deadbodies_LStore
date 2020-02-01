from template.page import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    current_rid = 1 # static variable
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
        self.page_ranges = [[[] for j in range(Config.NUM_SETS_PER_RANGE)] for i in range(Config.NUM_RANGES)]
        
    def generate_tuple_indexes(self, RID):
        range_number = RID / Config.NUM_RECORDS_PER_RANGE
        set_number = (RID - (range_number * Config.NUM_RECORDS_PER_RANGE)) / Config.NUM_RECORDS_PER_SET
        offset = (RID - (range_number * Config.NUM_RECORDS_PER_RANGE)) % Config.NUM_RECORDS_PER_SET

        return (range_number, set_number, offset)

    # __ means its internal to the class, never going to be used outside
    def __merge(self):
        pass
 
