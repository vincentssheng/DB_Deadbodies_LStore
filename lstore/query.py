from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import *
from lstore.page import *
import sys, struct, time, os, threading
from datetime import datetime

class Query:

    """
    # Creates a Query object that can perform different queries on the specified table 
    # @param: table - table to create a query on
    """
    def __init__(self, table):
        self.table = table
   
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
    # @param: j - tail or base
    # @param: k - set index
    # @param: offset - offset from start of page
    # @indirection: indirection column
    # @schema_encoding: schema_encoding
    # @record: record to be inserted
    """
    def write_to_page(self, range, bt, set, offset, indirection, schema_encoding, base_rid, record):

        page = self.table.bufferpool.find_page(self.table.name, range, bt, set, Config.INDIRECTION_COLUMN)
        page.write(offset, indirection.to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        
        page = self.table.bufferpool.find_page(self.table.name, range, bt, set, Config.RID_COLUMN)
        page.write(offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        page = self.table.bufferpool.find_page(self.table.name, range, bt, set, Config.TIMESTAMP_COLUMN)
        page.write(offset, int(time.mktime(datetime.now().timetuple())).to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        page = self.table.bufferpool.find_page(self.table.name, range, bt, set, Config.SCHEMA_ENCODING_COLUMN)
        page.write(offset, schema_encoding.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        page = self.table.bufferpool.find_page(self.table.name, range, bt, set, Config.BASE_RID_COLUMN)
        page.write(offset, base_rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        j = 0
        while j < self.table.num_columns:
            page = self.table.bufferpool.find_page(self.table.name, range, bt, set, j+Config.NUM_META_COLS)
            page.write(offset, record.columns[j].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
            j += 1
            
    
    # Delete record with the specified key
    # @param: key - specified primary key
    
    def delete(self, key, commit=False, abort=False, t=None, *args): # invalidate RID of base record and all tail records

        # committing to database
        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return (Config.INVALID_RID, 0, True)

        # undoing previous writes
        if abort:
            rid = args[0]
            key = args[1]
            (range_index, base, set_index, offset) = self.table.calculate_base_location(rid)

            rid_page = self.table.bufferpool.find_page(self.table.name, range_index, base, set_index, Config.RID_COLUMN)
            rid_page.write(offset, rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

            self.table.pd_lock.acquire()
            self.table.page_directory[rid] = (range_index, base, set_index, offset)
            self.table.index.indexes[Config.NUM_META_COLS+self.table.key][key] = [rid]
            self.table.pd_lock.release()

            return (Config.INVALID_RID, 0, True)

        # Get location in read info from base record
        self.table.pd_lock.acquire()
        rids = self.table.index.locate(0, key)
        (range_index, _, set_index, offset) = self.table.page_directory[rids[0]]
        self.table.pd_lock.release()

        if t:
            self.table.lm_lock.acquire()
            if not self.table.lock.acquire(rids[0], t, 'X'):
                self.table.lm_lock.release()
                return (Config.INVALID_RID, 0, False)

            self.table.lm_lock.release()

        #ind_page = self.table.bufferpool.find_page(self.table.name, range_index, 0, set_index, Config.INDIRECTION_COLUMN) 
        #indirection = int.from_bytes(ind_page.read(offset), sys.byteorder)
        rid_page = self.table.bufferpool.find_page(self.table.name, range_index, 0, set_index, Config.RID_COLUMN) 
        base_rid = int.from_bytes(rid_page.read(offset), sys.byteorder)

        # remove key and rid from dictionaries
        self.table.pd_lock.acquire()
        del self.table.page_directory[base_rid]
        del self.table.index.indexes[Config.NUM_META_COLS + self.table.key][key]
        del self.table.key_directory[key]
        self.table.pd_lock.release()

        # delete base record
        rid_page.write(offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

        """
        # Track down tail records associated to the base record that is deleted
        while indirection > 0:
            # Find next indirection
            (next_range, _, next_set, next_offset) = self.table.page_directory[indirection]

            # delete from page directory
            del self.table.page_directory[indirection]
            ind_page = self.table.bufferpool.find_page(self.table.name, next_range, 1, next_set, Config.INDIRECTION_COLUMN) 
            indirection = int.from_bytes(ind_page.read(next_offset), sys.byteorder)

            # invalidate record
            rid_page = self.table.bufferpool.find_page(self.table.name, next_range, 1, next_set, Config.RID_COLUMN) 
            rid = rid_page.read(next_offset)
            rid_page.write(next_offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder)) 
        """
        return (base_rid, key, True)     

    # Insert into a database
    # @param: *columns - columns to be written
    
    # Insert a record with specified columns
    def insert(self, *columns, commit=False, abort=False, t=None):
        # committing to database
        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return (Config.INVALID_RID, 0, True)

        # undoing writes
        if abort:
            rid = columns[0]
            key = columns[1]
            (range_index, base, set_index, offset) = self.table.calculate_base_location(rid)
            rid_page = self.table.bufferpool.find_page(self.table.name, range_index, base, set_index, Config.RID_COLUMN)
            rid_page.write(offset, rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

            self.table.pd_lock.acquire()
            del self.table.page_directory[rid]
            del self.table.index.indexes[Config.NUM_META_COLS + self.table.key][key]
            del self.table.key_directory[key]
            self.table.pd_lock.release()

            return (Config.INVALID_RID, 0, True)

        # generate schema encoding
        schema_encoding = '0' * self.table.num_columns

        self.table.base_rid_lock.acquire()
        self.table.tail_rid_lock.acquire()
        self.table.assign_rid('insert') # get valid rid
        record = Record(self.table.base_current_rid, self.table.key, columns)
        self.table.tail_rid_lock.release()
        self.table.base_rid_lock.release()
        (range_index, base, set_index, offset) = self.table.calculate_base_location(record.rid)

        if t:
            self.table.lm_lock.acquire()
            if not self.table.lock.acquire(record.rid, t, 'X'):
                self.table.lm_lock.release()
                return (Config.INVALID_RID, 0, False)
            self.table.lm_lock.release()

        # store physical location in page directory
        self.table.pd_lock.acquire()
        self.table.page_directory.update({record.rid: (range_index, 0, set_index, offset)}) 
        self.table.index.indexes[Config.NUM_META_COLS + self.table.key].update({record.columns[self.table.key]: [record.rid]})
        self.table.key_directory.update({record.columns[self.table.key]: (range_index, set_index, offset)}) 
        self.table.pd_lock.release()

        # Create new range?
        if range_index > self.table.latest_range_index:
            self.table.tt_lock.acquire()
            self.table.tail_tracker.append(-1)
            self.table.tt_lock.release()
            self.table.merge_tracker.append(0)
            self.table.base_tracker.append(0)
            self.table.latest_range_index += 1
            path = os.getcwd() + "/r_" + str(range_index) + "/0"
            if not os.path.exists(path):
                os.makedirs(path)

        # Create new page?
        if offset == 0:
            path = os.getcwd() + "/r_" + str(range_index) + "/0/s_" + str(set_index)
            if not os.path.exists(path):
                os.makedirs(path)
            ind_path = os.getcwd() + "/r_" + str(range_index) + "/0/indirection.txt"
            file = open(ind_path, 'w+')
            file.close()
            for i in range(1, self.table.num_columns+Config.NUM_META_COLS):
                file = open(path + "/p_" + str(i) + ".txt", "w+")
                file.close()

        self.write_to_page(range_index, base, set_index, offset, Config.INVALID_RID, self.schema_to_int(schema_encoding), record.rid, record) # writing to page
        return (record.rid, record.columns[self.table.key], True)

    # Select records from database
    # @param: key - specified key to select record
    # @param: query_columns - columns to return in result
    

    def get_latest_val(self, page_range, set_num, offset, column_index):
        # checking if base page has been updated
        #set_num = 0
        latest_rid_page = self.table.bufferpool.find_page(self.table.name, page_range, 0, set_num, Config.INDIRECTION_COLUMN)
        latest_rid_page.pin_count += 1
        latest_rid = int.from_bytes(latest_rid_page.read(offset), sys.byteorder)
        latest_rid_page.pin_count -= 1
        if latest_rid > latest_rid_page.lineage:
            latest_rid = 0 # read from base page if bp lineage is newer
        if latest_rid == 0:
            # read bp
            page = self.table.bufferpool.find_page(self.table.name, page_range, 0, set_num, column_index+Config.NUM_META_COLS)    
        else:
            # read the tail record
            # use page directory to get physical location of latest tp
            self.table.pd_lock.acquire()
            (range_index, _, set_index, offset) = self.table.page_directory[latest_rid]
            self.table.pd_lock.release()
            page = self.table.bufferpool.find_page(self.table.name, range_index, 1, set_index, column_index+Config.NUM_META_COLS)

        return int.from_bytes(page.read(offset), sys.byteorder)


    def select(self, key, column, query_columns, t=None, commit=False, abort=False, *args):
        # need to make sure key is available
        # Milestone 2 index
        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return ([], True)

        if abort:
            return([], True)

        record_list = []
        # find base record physical location
        rids = self.table.index.locate(column, key)
        
        if (rids == None):
            return None  # or None?

        if t:
            self.table.lm_lock.acquire()
            for rid in rids:
                if not self.table.lock.acquire(rid, t, 'S'):
                    self.table.lm_lock.release()
                    return ([], False)
            self.table.lm_lock.release()

        for rid in rids:
            record_info = []
            self.table.pd_lock.acquire()
            (range_index, _, set_index, offset) = self.table.page_directory[rid]
            self.table.pd_lock.release()

            for j in range(len(query_columns)):
                if query_columns[j] == 1:
                    record_info.append(self.get_latest_val(range_index, set_index, offset, j))
                else:
                    record_info.append('None')
            
            # this line may not be correct as locate_range returns a list of rids
            # and the first rid in this list may not be the one we are looking for
            # also inefficient as have to generate another sortedDict-->traverse everything
            # rid = self.table.index.locate_range(key, key, column)[0]

            rid_page = self.table.bufferpool.find_page(self.table.name, range_index, 0, set_index, Config.RID_COLUMN)
            rid_page.pin_count += 1
            rid = int.from_bytes(rid_page.read(offset), sys.byteorder)
            rid_page.pin_count -= 1

            record_list.append(Record(rid, key, record_info))
        return (record_list, True)

        
    # Update a record with specified key and columns
    # @param: key - specified key that corresponds to a record which we want to update
    # @param: *columns - in the form of [1, 2, none, none, 4]
    
    def update(self, key, *columns, commit=False, abort=False, t=None):

        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return (Config.INVALID_RID, True)

        if abort:
            base_rid = columns[0][0]
            (range_index, base, set_index, offset) = self.table.calculate_base_location(base_rid)
            base_ind_page = self.table.bufferpool.find_page(self.table.name, range_index, base, set_index, Config.INDIRECTION_COLUMN)
            base_ind = int.from_bytes(base_ind_page.read(offset), sys.byteorder)

            self.table.pd_lock.acquire()
            (tail_range, tail, tail_set, tail_offset) = self.table.page_directory[base_ind]
            self.table.pd_lock.release()
            tail_rid_page = self.table.bufferpool.find_page(self.table.name, tail_range, tail, tail_set, Config.RID_COLUMN)
            tail_rid_page.write(tail_offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

            tail_ind_page = self.table.bufferpool.find_page(self.table.name, tail_range, tail, tail_set, Config.INDIRECTION_COLUMN)
            base_ind_page.write(offset, tail_ind_page.read(tail_offset))

            self.table.pd_lock.acquire()
            del self.table.page_directory[base_ind]
            self.table.pd_lock.release()

            return (Config.INVALID_RID, True)

        # find RID for tail record
        self.table.base_rid_lock.acquire()
        self.table.tail_rid_lock.acquire()
        self.table.assign_rid('update')
        record = Record(self.table.tail_current_rid, self.table.key, columns)
        self.table.tail_rid_lock.release()
        self.table.base_rid_lock.release()
        rids = self.table.index.locate(0, key)

        if t:
            self.table.lm_lock.acquire()
            if not self.table.lock.acquire(rids[0], t, 'X'):
                self.table.lm_lock.release()
                return (Config.INVALID_RID, False)
            self.table.lm_lock.release()
        
        self.table.pd_lock.acquire()
        (base_range, _, base_set, base_offset) = self.table.page_directory[rids[0]]
        self.table.pd_lock.release()
        # generate schema encoding
        new_schema = ""
        for i in range(self.table.num_columns):
            if(columns[i] == None):
                new_schema += '0'
            else:
                new_schema += '1'

        # Base RID (1)
        base_rid_page = self.table.bufferpool.find_page(self.table.name, base_range, 0, base_set, Config.RID_COLUMN)
        base_rid_page.pin_count += 1
        base_rid = int.from_bytes(base_rid_page.read(base_offset), sys.byteorder)
        base_rid_page.pin_count -= 1
        
        # Base Indirection (0)
        base_ind_page = self.table.bufferpool.find_page(self.table.name, base_range, 0, base_set, Config.INDIRECTION_COLUMN)
        base_ind_page.pin_count += 1
        base_ind = int.from_bytes(base_ind_page.read(base_offset), sys.byteorder)
        base_ind_page.write(base_offset, record.rid.to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        base_ind_page.pin_count -= 1
       
        # Base SE (3)
        base_SE_page = self.table.bufferpool.find_page(self.table.name, base_range, 0, base_set, Config.SCHEMA_ENCODING_COLUMN)
        base_SE_page.pin_count += 1
        base_SE = int.from_bytes(base_SE_page.read(base_offset), sys.byteorder)
        # write indirection to base page and update base record schema encoding
        base_schema = self.int_to_schema(base_SE)
        result_schema = ""
        for i in range(self.table.num_columns):
            if base_schema[i] == '1' or new_schema[i] == '1':
                result_schema += '1'
            else:
                result_schema += '0'
        base_SE_page.write(base_offset, self.schema_to_int(result_schema).to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        base_SE_page.pin_count -= 1
        
        # Get information from latest updated record
        non_updated_values = []
        if base_ind != 0: # if base record has been updated at least once
            self.table.pd_lock.acquire()
            (prev_range, prev_bt, prev_set, prev_offset) = self.table.page_directory[base_ind]  
            self.table.pd_lock.release()
        else: # if base record has not been updated
            prev_range = base_range
            prev_bt = 0
            prev_set = base_set
            prev_offset = base_offset
        
        for i in range(self.table.num_columns):
            if new_schema[i] == '0':
                page = self.table.bufferpool.find_page(self.table.name, prev_range, prev_bt, prev_set, i+Config.NUM_META_COLS)
                page.pin_count += 1
                value = int.from_bytes(page.read(prev_offset), sys.byteorder)
                page.pin_count -= 1
                non_updated_values.append(value)
        count = 0
        new_columns = []
        for i in range(self.table.num_columns):
            if(columns[i] == None):
                new_columns.append(non_updated_values[count])
                count += 1
            else:
                new_columns.append(columns[i])
        record.columns = tuple(new_columns)
        
        # write tail record to memory
        self.table.tt_lock.acquire()
        tail_index = self.table.tail_tracker[base_range]
        path = os.getcwd()+"/r_"+str(base_range)+"/1"
        if tail_index == -1 and not os.path.exists(path): # if no updates to record yet
            os.makedirs(path)

        if tail_index == -1: # no tail page created yet
            path = os.getcwd() + "/r_" + str(base_range) + "/1" + "/s_0"
            if not os.path.exists(path):
                os.makedirs(path)
            self.table.tail_tracker[base_range] = 0
            tail_offset = 0
            for i in range(self.table.num_columns+Config.NUM_META_COLS):
                file = open(path + "/p_" + str(i) + ".txt", "w+")
                file.close()

        else: # if tail page has been created
            rid_page = self.table.bufferpool.find_page(self.table.name, base_range, 1, self.table.tail_tracker[base_range], 0)
            rid_page.pin_count += 1
            if rid_page.has_capacity():
                tail_offset = rid_page.num_records
            else:
                self.table.tail_tracker[base_range] += 1
                tail_offset = 0
                path = os.getcwd() + '/r_' + str(base_range) + '/1/s_' + str(self.table.tail_tracker[base_range])
                if not os.path.exists(path):
                    os.makedirs(path)
                for i in range(self.table.num_columns+Config.NUM_META_COLS):
                    file = open(path+"/p_"+str(i)+".txt", 'w+')
                    file.close()

            rid_page.pin_count -= 1

        self.table.pd_lock.acquire()
        self.table.page_directory.update({record.rid: (base_range, 1, self.table.tail_tracker[base_range], tail_offset)})
        self.table.pd_lock.release()
        self.write_to_page(base_range, 1, self.table.tail_tracker[base_range], tail_offset, base_ind, self.schema_to_int(new_schema), base_rid, record)
        self.table.tt_lock.release()
        return (base_rid, True)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    
    def sum(self, start_range, end_range, aggregate_column_index, *args, t=None, commit=False, abort=False):
        # need to make sure key is availabl

        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return (-1, True)

        if abort:
            return (-1, True)

        while start_range not in self.table.key_directory.keys():
            start_range += 1

        while end_range not in self.table.key_directory.keys():
            end_range -= 1

        self.table.pd_lock.acquire()
        (range_index, set_index, offset) = self.table.key_directory[start_range]
        self.table.pd_lock.release()
        rid = self.table.bufferpool.find_page(self.table.name, range_index, 0, set_index, Config.RID_COLUMN)

        if t:
            self.table.lm_lock.acquire()
            if not self.table.lock.acquire(rid, t, 'S'):
                self.table.lm_lock.release()
                return False
            self.table.lm_lock.release()

        total = self.get_latest_val(range_index, set_index, offset, aggregate_column_index)  

        while start_range != end_range:
            start_range += 1
            if start_range not in self.table.key_directory.keys():
                continue

            self.table.pd_lock.acquire()
            (range_index, set_index, offset) = self.table.key_directory[start_range]
            self.table.pd_lock.release()
            rid = self.table.bufferpool.find_page(self.table.name, range_index, 0, set_index, Config.RID_COLUMN)

            if t:
                self.table.lm_lock.acquire()
                if not self.table.lock.acquire(rid, t, 'S'):
                    self.table.lm_lock.release()
                    return False
                self.table.lm_lock.acquire()

            total += self.get_latest_val(range_index, set_index, offset, aggregate_column_index)  
            
        return total

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, *args, commit=False, abort=False, t=None):

        if commit:
            self.table.lm_lock.acquire()
            self.table.lock.release(t)
            self.table.lm_lock.release()
            return (Config.INVALID_RID, True)

        if abort:
            base_rid = args[0][0]
            (range_index, base, set_index, offset) = self.table.calculate_base_location(base_rid)
            base_ind_page = self.table.bufferpool.find_page(self.table.name, range_index, base, set_index, Config.INDIRECTION_COLUMN)
            base_ind = int.from_bytes(base_ind_page.read(offset), sys.byteorder)

            self.table.pd_lock.acquire()
            (tail_range, tail, tail_set, tail_offset) = self.table.page_directory[base_ind]
            self.table.pd_lock.release()
            tail_rid_page = self.table.bufferpool.find_page(self.table.name, tail_range, tail, tail_set, Config.RID_COLUMN)
            tail_rid_page.write(tail_offset, Config.INVALID_RID.to_bytes(Config.ENTRY_SIZE, sys.byteorder))

            tail_ind_page = self.table.bufferpool.find_page(self.table.name, tail_range, tail, tail_set, Config.INDIRECTION_COLUMN)
            base_ind_page.write(offset, tail_ind_page.read(tail_offset))

            self.table.pd_lock.acquire()
            del self.table.page_directory[base_ind]
            self.table.pd_lock.release()

            return (Config.INVALID_RID, True)

        (r, b) = self.select(key, self.table.key, [1] * self.table.num_columns, t=threading.current_thread().ident)
        if b is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[0].columns[column] + 1
            (base_rid, ub) = self.update(key, *updated_columns, t=threading.current_thread().ident)
            return (base_rid, ub)

        return (None, False)