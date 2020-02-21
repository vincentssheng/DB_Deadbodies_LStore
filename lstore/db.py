from lstore.table import Table
import os, sys
from collections import defaultdict
from lstore.page import *

class Bufferpool:

    def __init__(self, db):
        self.db = db
        self.empty = [i for i in range(Config.POOL_MAX_LEN)]  
        self.used = []
        self.pool = [Page("", ()) for i in range(Config.POOL_MAX_LEN)] # Create empty shell pages
        self.directory = defaultdict(lambda: -1)

    def flush_pool(self):
        for page in self.pool:
            if page.dirty:
                file = open(page.path, "w")
                data_str = ""
                for i in range(page.num_records):
                    data_str += str(int.from_bytes(page.read(i), sys.byteorder)) + " "

                file.write(data_str)
                file.close()
    
    def retrieve(self, path, location):
       
        # if file not empty
        if os.stat(path).st_size > 0:
            file = open(path, "r")
            data_str = file.readlines()
            data_lst = data_str[0].split() # we always store a line
            file.close()
            data = [int(i) for i in data_lst]
            page = Page(path, location)
            for i in range(len(data)):
                page.write(i, data[i].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        else:
            page = Page(path, location)

        page.dirty = False
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.pool[empty_index] = page

        return empty_index

    def evict(self):
        evict_index = self.used.pop(0)
        self.empty.append(evict_index)

        if self.pool[evict_index].dirty:
            file = open(self.pool[evict_index].path, "w")
            data_str = ""
            for i in range(self.pool[evict_index].num_records):
                data_str += str(int.from_bytes(self.pool[evict_index].read(i), sys.byteorder)) + " "

            file.write(data_str)
            file.close()

        del self.directory[self.pool[evict_index].location]

    def find_index(self, table, range, bt, set, page):
        i = self.directory[(table, range, bt, set, page)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
            
            path = os.getcwd() + "/" + table + "/r_" + str(range) + "/" + str(bt) + "/s_" + str(set) + "/p_" + str(page) + ".txt"
            i = self.retrieve(path, (table, range, bt, set, page))
            self.directory.update({(table, range, bt, set, page): i})
        
        return i


class Database():

    def __init__(self):
        # key: name
        # value: table
        self.tables = {}
        self.bufferpool = Bufferpool(self)
        pass

    def open(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        os.chdir(path)


    def close(self):
        for _, table in self.tables.items():
            table.bufferpool.flush_pool()
            table.unload_dirs()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key, self.bufferpool)
        self.tables.update({name: table}) # insert table with name
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] #remove table with name
        
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]


