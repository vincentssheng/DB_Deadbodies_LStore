from lstore.table import Table
import os, sys, json, threading
from collections import defaultdict, OrderedDict
from lstore.page import *


class Bufferpool:

    def __init__(self, db):
        self.db = db
        self.empty = [i for i in range(Config.POOL_MAX_LEN)]  
        self.used = []
        #self.pool = [Page("", ()) for i in range(Config.POOL_MAX_LEN)] # Create empty shell pages
        self.directory = defaultdict(lambda: -1)
        self.queue_lock = threading.Lock()
        self.pool = OrderedDict(lambda:-1)

    """
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
            data_lst = data_str[1].split() # we always store a line
            file.close()
            data = [int(i) for i in data_lst]
            page = Page(path, location)
            page.lineage = int(data_str[0])
            for i in range(len(data)):
                page.write(i, data[i].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        else:
            page = Page(path, location)

        page.dirty = False
        self.queue_lock.acquire()
        empty_index = self.empty.pop()
        self.used.append(empty_index)
        self.queue_lock.release()
        self.pool[empty_index] = page

        return empty_index

    def evict(self):
        self.queue_lock.acquire()
        evict_index = self.used.pop(0)
        self.empty.append(evict_index)
        self.queue_lock.release()

        if self.pool[evict_index].dirty:
            file = open(self.pool[evict_index].path, "w")
            file.write(str(self.pool[evict_index].lineage)+'\n')
            data_str = ""
            for i in range(self.pool[evict_index].num_records):
                data_str += str(int.from_bytes(self.pool[evict_index].read(i), sys.byteorder)) + " "

            file.write(data_str)
            file.close()

        self.queue_lock.acquire()
        del self.directory[self.pool[evict_index].location]
        self.queue_lock.release()

    def find_index(self, table, range, bt, set, page):
        i = self.directory[(table, range, bt, set, page)]
        if i == -1:
            if len(self.used) == len(self.pool):
                self.evict()
            
            path = os.getcwd() + '/r_' + str(range) + '/' + str(bt) 
            if bt == 0 and page == 0:
                path += '/indirection.txt'
            else:
                path += '/s_' + str(set) + '/p_' + str(page) + '.txt'
            i = self.retrieve(path, (table, range, bt, set, page))
            self.directory.update({(table, range, bt, set, page): i})
        
        return i
    """

    def flush_pool(self):
        for key in self.pool:
            if self.pool[key].dirty:
                file = open(self.pool[key].path, "w")
                data_str = ""
                for i in range(self.pool[key].num_records):
                    data_str += str(int.from_bytes(self.pool[key].read(i), sys.byteorder)) + " "

                file.write(str(self.pool[key].lineage)+'\n')
                file.write(data_str)
                file.close()

    def retrieve(self, path, location):
       
        # if file not empty
        if os.stat(path).st_size > 0:
            file = open(path, "r")
            data_str = file.readlines()
            data_lst = data_str[1].split() # we always store a line
            file.close()
            data = [int(i) for i in data_lst]
            page = Page(path, location)
            page.lineage = int(data_str[0])
            for i in range(len(data)):
                page.write(i, data[i].to_bytes(Config.ENTRY_SIZE, sys.byteorder))
        else:
            page = Page(path, location)

        page.dirty = False
        self.queue_lock.acquire()
        self.pool[location] = page
        self.queue_lock.release()

        return page

    def evict(self):

        self.queue_lock.acquire()
        evict_page = self.pool.pop()
        self.queue_lock.release()
        if evict_page.dirty:
            file = open(evict_page.path, "w")
            file.write(str(evict_page.lineage)+'\n')
            data_str = ""
            for i in range(evict_page.num_records):
                data_str += str(int.from_bytes(evict_page.read(i), sys.byteorder)) + " "

            file.write(data_str)
            file.close()  

    def find_page(self, table, r, bt, s, page):
        page = self.pool[(table, r, bt, s, page)]
        if page == -1:
            if len(self.pool) == Config.POOL_MAX_LEN:
                self.evict()
            path = os.getcwd() + '/r_' + str(range) + '/' + str(bt) 
            if bt == 0 and page == 0:
                path += '/indirection.txt'
            else:
                path += '/s_' + str(set) + '/p_' + str(page) + '.txt'
            page = self.retrieve(path, (table, range, bt, set, page))

        return page
            



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
            table.unload_meta()


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, key, num_columns, self.bufferpool, 
                        latest_range_index=-1, base_current_rid=0, 
                        tail_current_rid=Config.MAX_RID, 
                        tail_tracker=[],
                        merge_tracker=[], base_tracker=[])
        self.tables.update({name: table}) # insert table with name
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] #remove table with name
        # delete folder associated with table
        
    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        with open(os.getcwd()+'/'+name+'/metadata.json', 'r') as fp:
            meta_dict = json.loads(fp.read())
        fp.close()

        table = Table(name, meta_dict['key'], meta_dict['num_columns'], 
                        self.bufferpool, meta_dict['latest_range'], 
                        meta_dict['base_rid'], meta_dict['tail_rid'], 
                        meta_dict['tail_tracker'],
                        meta_dict['merge_tracker'], meta_dict['base_tracker'])
        self.tables.update({name: table})
        return table


