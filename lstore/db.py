from lstore.table import Table
import os, sys, json, threading
from collections import defaultdict, OrderedDict
from lstore.page import *

# The bufferpool holds a subset of our persistent (disk) data in RAM
class Bufferpool:

    def __init__(self, db):
        self.db = db
        self.pool = OrderedDict() # Pages stored in an ordered dictionary to mimic an LRU cache

    """
    Called upon db.close()
    Flushes all the dirty pages in the pool into persistent storage
    """
    def flush_pool(self):
        for _, page in self.pool.copy().items():
            if page.dirty:
                file = open(page.path, "w")
                data_str = ""
                for i in range(page.num_records):
                    data_str += str(int.from_bytes(page.read(i), sys.byteorder)) + " "

                file.write(str(page.lineage)+'\n')
                file.write(data_str)
                file.close()

    """
    Retrieves a page (not currently in pool) and place it in pool
    @param: path - path of the file corresponding to the page
    @param: location - a tuple containing (table, range, base/tail, set, page)
    @return: the page retrieved
    """
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
        self.pool[location] = page

        return page

    """
    Evicts the least recently used page and writes the data if the page is dirty
    """
    def evict(self):

        (_, evict_page) = self.pool.popitem()
        if evict_page.dirty:
            file = open(evict_page.path, "w")
            file.write(str(evict_page.lineage)+'\n')
            data_str = ""
            for i in range(evict_page.num_records):
                data_str += str(int.from_bytes(evict_page.read(i), sys.byteorder)) + " "

            file.write(data_str)
            file.close()  

    """
    Finds the page and retrieves it to the pool
    Evicts pages if bufferpool is full
    @param: table - name of the table
    @param: r - range index
    @param: bt - base(0)/tail(1) indicator
    @param: s - set index
    @param: pg - page number (column)
    @return: page retrieved into pool
    """
    def find_page(self, table, r, bt, s, pg):
        if not self.pool.__contains__((table, r, bt, s, pg)):
            if len(self.pool) == Config.POOL_MAX_LEN:
                self.evict()
            path = os.getcwd() + '/r_' + str(r) + '/' + str(bt) 
            if bt == 0 and pg == 0:
                path += '/indirection.txt'
            else:
                path += '/s_' + str(s) + '/p_' + str(pg) + '.txt'
            page = self.retrieve(path, (table, r, bt, s, pg))
            return page

        else: # bring page to the most recent slot
            page = self.pool[(table, r, bt, s, pg)]
            self.pool.pop((table, r, bt, s, pg))
            self.pool[(table, r, bt, s, pg)] = page
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
                        meta_dict['merge_tracker'], meta_dict['base_tracker'],
                        method='get')
        self.tables.update({name: table})
        return table


