from template.page import *
from time import time

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Range:

    def __init__(first_rid):
        self.first_rid = first_rid
        self.base_pages = []
        self.tail_pages = []

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
        self.page_directory = {} # dictionary that maps rid to page + offset
        # Make 2^64 - 1 / PAGE_SIZE update ranges

    def __merge(self):
        pass
 
