from lstore.config import *
from sortedcontainers import sorteddict
import sys

"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.table = table
        # initialize sortedDict
        ##  self.sortedDict = sorteddict.SortedDict()
        # DEFAULT : primary_key (SID column)
        # put the line below in your tester since user creates index
        # self.create_index(0)
        self.indexes = [sorteddict.SortedDict() for i in range(Config.NUM_META_COLS + self.table.num_columns)]

    def update(self, column):
        self.drop_index(column)
        self.create_index(column)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # traversing sortedDict to find wanted value within specified column
        # self.update(column)
        
        if (not self.indexes[Config.NUM_META_COLS + column].__contains__(value)) :
            return None

        rids = self.indexes[Config.NUM_META_COLS + column].get(value)
        
        return rids

    """
    Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # traverse through sortedDict and find which leaves value would be between
        # self.update(column)
        cumul_rids = []

        for i in range(begin, end + 1) :
            if (not self.indexes[Config.NUM_META_COLS + column].__contains__(i)) :
                continue

            rid_list = self.indexes[Config.NUM_META_COLS + column].get(i)
            cumul_rids += rid_list
                
        return cumul_rids

    """
    # optional: Create index on specific column
    """

    def get_latest_val(self, page_range, set_num, offset, column_index):
        # checking if base page has been updated
        latest_rid_page = self.table.bufferpool.find_page(self.table.name, page_range, 0, set_num, Config.INDIRECTION_COLUMN)
        latest_rid_page.pin_count += 1
        latest_rid = int.from_bytes(latest_rid_page.read(offset), sys.byteorder)
        latest_rid_page.pin_count -= 1

        if latest_rid > latest_rid_page.lineage:
            latest_rid = 0 # read from base page if bp lineage is newer

        if latest_rid == 0:
            # read bp
            col_page = self.table.bufferpool.find_page(self.table.name, page_range, 0, set_num, column_index+Config.NUM_META_COLS)    
        else:
            # read the tail record
            # use page directory to get physical location of latest tp
            (range_index, _, set_index, offset) = self.table.page_directory[latest_rid]
            col_page = self.table.bufferpool.find_page(self.table.name, range_index, 1, set_index, column_index+Config.NUM_META_COLS)

        return int.from_bytes(col_page.read(offset), sys.byteorder)

    
    def create_index(self, column_number):
        # populate sortedDict here based on column - insertion
        # note: column_number = primary key column (like SID)

        # traverse through all the existing records 
        for i in range (self.table.base_current_rid) :
            rid = i + 1  # RID cannever be 0
            if rid not in self.table.page_directory.keys():
                # error, RID does NOT exist
                continue

            # find base record physical location
            (page_index, _, set_index, offset) = self.table.page_directory[rid]

            column_val = self.get_latest_val(page_index, set_index, offset, column_number)
            # insert value into sortedDict
            if (self.indexes[Config.NUM_META_COLS + column_number].__contains__(column_val)):
                rid_list = self.indexes[Config.NUM_META_COLS + column_number].get(column_val)
                rid_list.append(rid)
                self.indexes[Config.NUM_META_COLS + column_number].update({column_val: rid_list})
            else :
                self.indexes[Config.NUM_META_COLS + column_number].update({column_val: [rid]})


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if (self.indexes[Config.NUM_META_COLS + column_number]) : 
            self.indexes[Config.NUM_META_COLS + column_number].clear()

