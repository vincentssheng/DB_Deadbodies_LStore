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

    def delete(self, key):
        pass

    def write_to_page(self, i, j, offset, indirection, schema_encoding, record):
        self.table.ranges[i][j][0].write(offset, indirection.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # indirection 0 for base records
        self.table.ranges[i][j][1].write(offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # rid column
        self.table.ranges[i][j][2].write(offset, Config.TODO_VALUE_TIMESTAMP.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # timestamp
        self.table.ranges[i][j][3].write(offset, bytearray(schema_encoding, "utf8")) # schema encoding
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
        if (record.rid-1) / Config.NUM_RECORDS_PER_RANGE >= len(self.table.ranges):
            self.table.ranges.append([])
        if offset == 0:
            self.table.ranges[range_index].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
        self.write_to_page(range_index, set_index, offset, 0, schema_encoding, record) # writing to page

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    columns = [1, 2, none, none, 4]
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass



