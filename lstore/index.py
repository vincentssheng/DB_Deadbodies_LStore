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
        self.sortedDict = sorteddict.SortedDict()
        # DEFAULT : primary_key (SID column)
        # put the line below in your tester since user creates index
        # self.create_index(0)
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        # traversing sortedDict to find wanted value within specified column
        if (not self.sortedDict.__contains__(value)) :
            return None

        rids = self.sortedDict.get(value)

        record_locations = []

        for i in range (len(rids)) : 
            (page_index, _, set_index, offset) = self.table.page_directory[rids[i]]
            record_locations.append((page_index, set_index, offset))
        
        return record_locations
        pass

    """
    Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        # traverse through sortedDict and find which leaves value would be between
        
        pass

    """
    # optional: Create index on specific column
    """

    def get_latest_val(self, page_range, set_num, offset, column_index):
        # checking if base page has been updated
        latest_rid_index = self.table.bufferpool.find_index(self.table.name, page_range, 0, set_num, Config.INDIRECTION_COLUMN)
        self.table.bufferpool.pool[latest_rid_index].pin_count += 1
        latest_rid = int.from_bytes(self.table.bufferpool.pool[latest_rid_index].read(offset), sys.byteorder)
        self.table.bufferpool.pool[latest_rid_index].pin_count -= 1

        if latest_rid > self.table.bufferpool.pool[latest_rid_index].lineage:
            latest_rid = 0 # read from base page if bp lineage is newer

        if latest_rid == 0:
            # read bp
            col_index = self.table.bufferpool.find_index(self.table.name, page_range, 0, set_num, column_index+Config.NUM_META_COLS)    
        else:
            # read the tail record
            # use page directory to get physical location of latest tp
            (range_index, _, set_index, offset) = self.table.page_directory[latest_rid]
            col_index = self.table.bufferpool.find_index(self.table.name, range_index, 1, set_index, column_index+Config.NUM_META_COLS)

        return int.from_bytes(self.table.bufferpool.pool[col_index].read(offset), sys.byteorder)

    
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
            if (self.sortedDict.__contains__(column_val)):
                rid_list = self.sortedDict.get(column_val)
                rid_list.append(rid)
                self.sortedDict.update({column_val: rid_list})
            else :
                self.sortedDict.update({column_val: [rid]})

        # print(self.sortedDict)
        pass


    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
