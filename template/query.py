from template.table import Table, Record
from template.index import Index
from template.config import *
from template.page import *
import sys

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key): # invalidate RID of base record and all tail records
        (range_index, set_index, offset) = self.table.key_directory[key] #get location of base record
        indirect_rid = 1
        while indirect_rid != 0:
            self.table.ranges[range_index][set_index][Config.RID_COLUMN] = Config.INVALID_RID #invalidate RID
            indirect_rid = int.from_bytes(self.table.ranges[range_index][set_index][Config.INDIRECTION_COLUMN].read(offset), sys.byteorder) #get RID of next tail record
            (range_index, set_index, offset) = self.table.calculate_phys_location(indirect_rid) #get location of next tail record

    def write_to_page(self, i, j, offset, indirection, schema_encoding, record):
        self.table.ranges[i][j][Config.INDIRECTION_COLUMN].write(offset, indirection.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # indirection 0 for base records
        self.table.ranges[i][j][Config.RID_COLUMN].write(offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # rid column
        self.table.ranges[i][j][Config.TIMESTAMP_COLUMN].write(offset, Config.TODO_VALUE_TIMESTAMP.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # timestamp
        self.table.ranges[i][j][Config.SCHEMA_ENCODING_COLUMN].write(offset, bytearray(schema_encoding, "utf8")) # schema encoding
        for k in range(self.table.num_columns):
            self.table.ranges[i][j][k+Config.NUM_META_COLS].write(offset, record.columns[k].to_bytes(Config.ENTRY_SIZE, sys.byteorder))

    """
    # Insert a record with specified columns
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        self.table.assign_rid('insert') # get valid rid
        record = Record(self.table.base_current_rid, self.table.key, columns)
        (range_index, set_index, offset) = self.table.calculate_phys_location(record.rid)
        # store physical location in page directory
        self.table.page_directory.update({record.rid: (range_index, set_index, offset)}) 
        self.table.key_directory.update({record.columns[self.table.key]: (range_index, set_index, offset)})
        if (record.rid-1) / Config.NUM_RECORDS_PER_RANGE >= len(self.table.ranges):
            self.table.ranges.append([])
        if offset == 0:
            self.table.ranges[range_index].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
        self.write_to_page(range_index, set_index, offset, Config.INVALID_RID, schema_encoding, record) # writing to page

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """

    def select(self, key, query_columns):
        # need to make sure key is available
        if key not in self.table.key_directory.keys():
            # error, cannot find a key that does NOT exist
            pass

        # find base record physical location
        (range_index, set_index, offset) = self.table.key_directory[key]

        # get RID of latest tail record if available
        prev_indirection = int.from_bytes(self.table.ranges[range_index][set_index][Config.INDIRECTION_COLUMN].read(offset), sys.byteorder)
        record_info = []
        if prev_indirection == 0:
            # read bp
            for i in range(len(query_columns)):
                if(query_columns[i] == 0):
                    continue
                # read from the corresponding pages according to query_columns
                record_info.append(int.from_bytes(self.table.ranges[range_index][set_index][i + Config.NUM_META_COLS].read(offset),sys.byteorder))
        else:
            # read from latest tp
            # use page directory to get physical location of latest tp
            (range_index, set_index, offset) = self.table.page_directory[prev_indirection]

            for i in range(len(query_columns)):
                if(query_columns[i] == 0):
                    continue
                # read from the corresponding pages according to query_columns
                record_info.append(int.from_bytes(self.table.ranges[range_index][set_index][i + Config.NUM_META_COLS].read(offset),sys.byteorder))

        return record_info

    """
    # Update a record with specified key and columns
    columns = [1, 2, none, none, 4]
    """

    def update(self, key, *columns):
        self.table.assign_rid('update')
        record = Record(self.table.tail_current_rid, self.table.key, columns)
        (range_index, set_index, offset) = self.table.key_directory[key]

        # generate schema encoding
        schema_encoding = ""
        for i in range(columns):
            if(columns[i] == "none"):
                schema_encoding += '0'
            schema_encoding += '1'

        # calculate offset and allocate new page if necessary
        if self.table.ranges[range_index][set_index][0].has_capacity:
            tail_offset = self.table.ranges[range_index][set_index][0].num_records * Config.ENTRY_SIZE
        else:
            tail_offset = 0
            self.table.ranges[range_index].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])

        self.table.page_directory.update({record.rid: (range_index, set_index, tail_offset)})

        # read previous tail record rid
        prev_indirection = self.table.ranges[range_index][set_index][Config.INDIRECTION_COLUMN].read(offset).int_from_bytes(sys.byteorder)
        
        # write indirection to base page
        self.table.ranges[range_index][set_index][Config.INDIRECTION_COLUMN].write(offset, self.table.tail_current_rid)

        # write tail record
        self.write_to_page(range_index, set_index, offset, prev_indirection, schema_encoding, record)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass



