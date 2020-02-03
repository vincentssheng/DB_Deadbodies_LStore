from template.table import Table, Record
from template.index import Index
from template.config import *
from template.page import *
import sys
import struct

class Query:

    """
    # Creates a Query object that can perform different queries on the specified table 
    # @param: table - table to create a query on
    """
    def __init__(self, table):
        self.table = table
        pass
   
    """
    # Conversion of schema encoding to integer
    # @param: schema - schema encoding (string)
    # RETURN: schema encoding (int)
    """
    def schema_to_int(self, schema):
        int_value = 0
        for i in range(self.table.num_columns):
            int_value += int(schema[i]) * pow(2, self.table.num_columns-1-i)
        return int_value

    """
    # Conversion of integer to schema encoding
    # @param: schema - schema encoding (int)
    # RETURN: schema encoding (string)
    """
    def int_to_schema(self, value):
        lst = []
        for i in range(self.table.num_columns):
            lst.append(value%2)
            value /= 2
        reversed_list = [int(e) for e in reversed(lst)]
        schema = ''.join(str(e) for e in reversed_list)
        return schema

    """
    # Write columns to page
    # @param: i - index of range
    # @param: j - index of set
    # @param: offset - offset from start of page
    # @indirection: indirection column
    # @schema_encoding: schema_encoding
    # @record: record to be inserted
    """
    def write_to_page(self, i, j, offset, indirection, schema_encoding, record):
        self.table.ranges[i][j][Config.INDIRECTION_COLUMN].write(offset, indirection.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # indirection 0 for base records
        self.table.ranges[i][j][Config.RID_COLUMN].write(offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # rid column
        self.table.ranges[i][j][Config.TIMESTAMP_COLUMN].write(offset, Config.TODO_VALUE_TIMESTAMP.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # timestamp
        self.table.ranges[i][j][Config.SCHEMA_ENCODING_COLUMN].write(offset, schema_encoding.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) # schema encoding
        for k in range(self.table.num_columns):
            self.table.ranges[i][j][k+Config.NUM_META_COLS].write(offset, record.columns[k].to_bytes(Config.ENTRY_SIZE, sys.byteorder))

    """
    # Delete record with the specified key
    # @param: key - specified primary key
    """
    def delete(self, key): # invalidate RID of base record and all tail records
        (range_index, set_index, offset) = self.table.key_directory[key] #get location of base record
        indirect_rid = 1
        while indirect_rid != 0:
            self.table.ranges[range_index][set_index][Config.RID_COLUMN] = Config.INVALID_RID #invalidate RID
            indirect_rid = int.from_bytes(self.table.ranges[range_index][set_index][Config.INDIRECTION_COLUMN].read(offset), sys.byteorder) #get RID of next tail record
            (range_index, set_index, offset) = self.table.calculate_phys_location(indirect_rid) #get location of next tail record

    """
    # Insert into a database
    # @param: *columns - columns to be written
    """
    # Insert a record with specified columns
    def insert(self, *columns):

        # generate schema encoding
        schema_encoding = '0' * self.table.num_columns

        self.table.assign_rid('insert') # get valid rid
        record = Record(self.table.base_current_rid, self.table.key, columns)
        (range_index, set_index, offset) = self.table.calculate_phys_location(record.rid)

        # store physical location in page directory
        self.table.page_directory.update({record.rid: (range_index, set_index, offset)}) 
        self.table.key_directory.update({record.columns[self.table.key]: (range_index, set_index, offset)})

        # Create new range?
        if (record.rid-1) / Config.NUM_RECORDS_PER_RANGE >= len(self.table.ranges):
            self.table.tail_tracker.append(False)
            self.table.ranges.append([])

        # Create new page?
        if offset == 0:
            self.table.ranges[range_index].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
        self.write_to_page(range_index, set_index, offset, Config.INVALID_RID, self.schema_to_int(schema_encoding), record) # writing to page

    """
    # Select records from database
    # @param: key - specified key to select record
    # @param: query_columns - columns to return in result
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
    # @param: key - specified key that corresponds to a record which we want to update
    # @param: *columns - in the form of [1, 2, none, none, 4]
    """

    def update(self, key, *columns):
        # find RID for tail record
        self.table.assign_rid('update')
        record = Record(self.table.tail_current_rid, self.table.key, columns)
        (base_range, base_set, base_offset) = self.table.key_directory[key]

        # generate schema encoding
        new_schema = ""
        for i in range(self.table.num_columns):
            if(columns[i] == None):
                new_schema += '0'
            else:
                new_schema += '1'

        # calculate offset and allocate new page if necessary
        if self.table.tail_tracker[base_range] > 0: # if there is already a tail page
            if self.table.ranges[base_range][self.table.tail_tracker[base_range]][0].has_capacity: # if latest tail page has space
                tail_offset = self.table.ranges[base_range][self.table.tail_tracker[base_range]][0].num_records * Config.ENTRY_SIZE
            else:
                tail_offset = 0
                self.table.ranges[base_range].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
                self.table.tail_tracker[base_range] = len(self.table.ranges[base_range]) - 1
        else:
            tail_offset = 0
            self.table.ranges[base_range].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
            self.table.tail_tracker[base_range] = len(self.table.ranges[base_range]) - 1

        self.table.page_directory.update({record.rid: (base_range, self.table.tail_tracker[base_range], tail_offset)})

        # read previous tail record rid
        prev_indirection = int.from_bytes(self.table.ranges[base_range][base_set][Config.INDIRECTION_COLUMN].read(base_offset), sys.byteorder)
        non_updated_values = []
        if prev_indirection != 0: # if base record has been updated at least once
            (prev_range, prev_set, prev_offset) = self.table.page_directory[prev_indirection]  
        else: # if base record has not been updated
            prev_range = base_range
            prev_set = base_set
            prev_offset = base_offset
        for i in range(self.table.num_columns):
            if new_schema[i] == '0':
                value = int.from_bytes(self.table.ranges[prev_range][prev_set][Config.NUM_META_COLS+i].read(prev_offset), sys.byteorder)
                non_updated_values.append(value)
        
        # write indirection to base page and update base record schema encoding
        self.table.ranges[base_range][base_set][Config.INDIRECTION_COLUMN].write(base_offset, self.table.tail_current_rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        base_schema = self.int_to_schema(int.from_bytes(self.table.ranges[base_range][base_set][Config.SCHEMA_ENCODING_COLUMN].read(base_offset), sys.byteorder))
        result_schema = ""
        for i in range(self.table.num_columns):
            if base_schema[i] == '1' or new_schema[i] == '1':
                result_schema += '1'
            else:
                result_schema += '0'
        self.table.ranges[base_range][base_set][Config.SCHEMA_ENCODING_COLUMN].write(base_offset, self.schema_to_int(result_schema).to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        # write tail record
        count = 0
        new_columns = []
        for i in range(self.table.num_columns):
            if(columns[i] == None):
                new_columns.append(non_updated_values[count])
                count += 1
            else:
                new_columns.append(columns[i])
        record.columns = tuple(new_columns)
        self.write_to_page(base_range, self.table.tail_tracker[base_range], tail_offset, prev_indirection, self.schema_to_int(new_schema), record)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass



