from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.page import *
import sys, struct, time, os
from datetime import datetime

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
        for i in range(self.table.num_cols):
            int_value += int(schema[i]) * pow(2, self.table.num_cols-1-i)
        return int_value

    """
    # Conversion of integer to schema encoding
    # @param: schema - schema encoding (int)
    # RETURN: schema encoding (string)
    """
    def int_to_schema(self, value):
        lst = []
        for i in range(self.table.num_cols):
            lst.append(value%2)
            value /= 2
        reversed_list = [int(e) for e in reversed(lst)]
        schema = ''.join(str(e) for e in reversed_list)
        return schema

    """
    # Write columns to page
    # @param: i - index of range
    # @param: j - tail or base
    # @param: k - set index
    # @param: offset - offset from start of page
    # @indirection: indirection column
    # @schema_encoding: schema_encoding
    # @record: record to be inserted
    """
    def write_to_page(self, range, bt, set, offset, indirection, schema_encoding, record):

        i = self.table.bufferpool.find_index(self.table.name, range, bt, set, Config.INDIRECTION_COLUMN)
        self.table.bufferpool.pool[i].write(offset, indirection.to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        
        i = self.table.bufferpool.find_index(self.table.name, range, bt, set, Config.RID_COLUMN)
        self.table.bufferpool.pool[i].write(offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        i = self.table.bufferpool.find_index(self.table.name, range, bt, set, Config.TIMESTAMP_COLUMN)
        self.table.bufferpool.pool[i].write(offset, int(time.mktime(datetime.now().timetuple())).to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        i = self.table.bufferpool.find_index(self.table.name, range, bt, set, Config.SCHEMA_ENCODING_COLUMN)
        self.table.bufferpool.pool[i].write(offset, schema_encoding.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        j = 0
        while j < self.table.num_cols:
            index = self.table.bufferpool.find_index(self.table.name, range, bt, set, j+Config.NUM_META_COLS)
            self.table.bufferpool.pool[index].write(offset, record.columns[j].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
            #print(self.table.bufferpool.pool[index].path)
            j += 1
            
    """
    # Delete record with the specified key
    # @param: key - specified primary key
    
    def delete(self, key): # invalidate RID of base record and all tail records

        # Get location in read info from base record
        (range_index, set_index, offset) = self.table.key_directory[key]
        indirection = int.from_bytes(self.table.ranges[range_index][0][set_index][Config.INDIRECTION_COLUMN].read(offset), sys.byteorder)
        base_rid = int.from_bytes(self.table.ranges[range_index][0][set_index][Config.RID_COLUMN].read(offset), sys.byteorder)

        # remove key and rid from dictionaries
        del self.table.key_directory[key]
        del self.table.page_directory[base_rid]

        # delete base record
        self.table.ranges[range_index][0][set_index][Config.RID_COLUMN].write(offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        # Track down tail records associated to the base record that is deleted
        while indirection > 0:
            # Find next indirection
            (next_range, tail, next_set, next_offset) = self.table.page_directory[indirection]

            # delete from page directory
            del self.table.page_directory[indirection]
            indirection = int.from_bytes(self.table.ranges[next_range][tail][next_set][Config.INDIRECTION_COLUMN].read(next_offset), sys.byteorder)

            # invalidate record
            self.table.ranges[range_index][tail][set_index][Config.RID_COLUMN].write(offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) 

    
    # Insert into a database
    # @param: *columns - columns to be written
    """
    # Insert a record with specified columns
    def insert(self, *columns):

        # generate schema encoding
        schema_encoding = '0' * self.table.num_cols

        self.table.assign_rid('insert') # get valid rid
        record = Record(self.table.base_current_rid, self.table.key, columns)
        (range_index, base, set_index, offset) = self.table.calculate_base_location(record.rid)

        # store physical location in page directory
        self.table.page_directory.update({record.rid: (range_index, 0, set_index, offset)}) 
        self.table.key_directory.update({record.columns[self.table.key]: (range_index, set_index, offset)})

        # Create new range?
        if range_index > self.table.latest_range_index:
            self.table.tail_tracker.append(-1)
            self.table.latest_range_index += 1
            path = os.getcwd() + "/" + self.table.name + "/r_" + str(range_index) + "/0"
            if not os.path.exists(path):
                os.makedirs(path)

        # Create new page?
        if offset == 0:
            path = os.getcwd() + "/" + self.table.name + "/r_" + str(range_index) + "/0/s_" + str(set_index)
            if not os.path.exists(path):
                os.makedirs(path)
            for i in range(self.table.num_cols+Config.NUM_META_COLS):
                file = open(path + "/p_" + str(i) + ".txt", "w+")
                file.close()

            pages = [Page(path+"/p_"+str(i)+".txt") for i in range(self.table.num_cols+Config.NUM_META_COLS)]

            for i in range(len(pages)):
                print(pages[i].path)
                index = self.table.bufferpool.find_index(self.table.name, range_index, 0, set_index, i)
                self.table.bufferpool.pool[index] = pages[i]

        self.write_to_page(range_index, base, set_index, offset, Config.INVALID_RID, self.schema_to_int(schema_encoding), record) # writing to page

    """
    # Select records from database
    # @param: key - specified key to select record
    # @param: query_columns - columns to return in result
    

    def get_latest_val(self, page_range, set_num, offset, column_index):
        # checking if base page has been updated
        prev_indirection = int.from_bytes(self.table.ranges[page_range][0][set_num][Config.INDIRECTION_COLUMN].read(offset), sys.byteorder)
        # CHECK IF RECORD EXISTS (MILESTONE 2)

        if prev_indirection == 0:
            # read bp
            return int.from_bytes(self.table.ranges[page_range][0][set_num][column_index + Config.NUM_META_COLS].read(offset), sys.byteorder)
        else:
            # read the tail record
            # use page directory to get physical location of latest tp
            (range_index, tail, set_index, offset) = self.table.page_directory[prev_indirection]
            return int.from_bytes(self.table.ranges[range_index][tail][set_index][column_index + Config.NUM_META_COLS].read(offset), sys.byteorder)


    def select(self, key, query_columns):
        # need to make sure key is available
        if key not in self.table.key_directory.keys():
            # error, cannot find a key that does NOT exist
            return None

        # find base record physical location
        (range_index, set_index, offset) = self.table.key_directory[key]

        record_info = []

        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                record_info.append(self.get_latest_val(range_index, set_index, offset, i))
            else:
                record_info.append('None')
        
        rid = int.from_bytes(self.table.ranges[range_index][0][set_index][Config.RID_COLUMN].read(offset), sys.byteorder)
        #print("rid: " + str(rid))
        #print("info: " + str(tuple(record_info)))
        return [Record(rid, key, tuple(record_info))]

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
        if self.table.tail_tracker[base_range] > -1: # if there is already a tail page
            if self.table.ranges[base_range][1][self.table.tail_tracker[base_range]][0].has_capacity(): # if latest tail page has space
                tail_offset = self.table.ranges[base_range][1][self.table.tail_tracker[base_range]][0].num_records
            else:
                tail_offset = 0
                self.table.ranges[base_range][1].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
                self.table.tail_tracker[base_range] += 1
        else:
            self.table.ranges[base_range].append([])
            tail_offset = 0
            self.table.ranges[base_range][1].append([Page() for i in range(self.table.num_columns+Config.NUM_META_COLS)])
            self.table.tail_tracker[base_range] += 1

        self.table.page_directory.update({record.rid: (base_range, 1, self.table.tail_tracker[base_range], tail_offset)})

        # read previous tail record rid
        prev_indirection = int.from_bytes(self.table.ranges[base_range][0][base_set][Config.INDIRECTION_COLUMN].read(base_offset), sys.byteorder)
        non_updated_values = []
        if prev_indirection != 0: # if base record has been updated at least once
            (prev_range, prev_tail, prev_set, prev_offset) = self.table.page_directory[prev_indirection]  
        else: # if base record has not been updated
            prev_range = base_range
            prev_tail = 0
            prev_set = base_set
            prev_offset = base_offset
        for i in range(self.table.num_columns):
            if new_schema[i] == '0':
                value = int.from_bytes(self.table.ranges[prev_range][prev_tail][prev_set][Config.NUM_META_COLS+i].read(prev_offset), sys.byteorder)
                non_updated_values.append(value)
        
        # write indirection to base page and update base record schema encoding
        self.table.ranges[base_range][0][base_set][Config.INDIRECTION_COLUMN].write(base_offset, self.table.tail_current_rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        base_schema = self.int_to_schema(int.from_bytes(self.table.ranges[base_range][0][base_set][Config.SCHEMA_ENCODING_COLUMN].read(base_offset), sys.byteorder))
        result_schema = ""
        for i in range(self.table.num_columns):
            if base_schema[i] == '1' or new_schema[i] == '1':
                result_schema += '1'
            else:
                result_schema += '0'
        self.table.ranges[base_range][0][base_set][Config.SCHEMA_ENCODING_COLUMN].write(base_offset, self.schema_to_int(result_schema).to_bytes(Config.ENTRY_SIZE, sys.byteorder))

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
        self.write_to_page(base_range, 1, self.table.tail_tracker[base_range], tail_offset, prev_indirection, self.schema_to_int(new_schema), record)

    
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    
    def sum(self, start_range, end_range, aggregate_column_index):
        # need to make sure key is available
        if (start_range not in self.table.key_directory.keys() or end_range not in self.table.key_directory.keys()):
            # error, cannot find a key that does NOT exist
            return 0

        (range_index, set_index, offset) = self.table.key_directory[start_range]
        sum = self.get_latest_val(range_index, set_index, offset, aggregate_column_index)

        while (start_range != end_range):
            start_range += 1

            # check if new key exists in dictionary
            if (start_range not in self.table.key_directory.keys()):
                continue
            
            # get physical location
            (range_index, set_index, offset) = self.table.key_directory[start_range]

            sum += self.get_latest_val(range_index, set_index, offset, aggregate_column_index)

        return sum
        """