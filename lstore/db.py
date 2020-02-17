from lstore.table import Table
import os

class Database():

    def __init__(self):
        # key: name
        # value: table
        self.tables = {}
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
        table = Table(name, num_columns, key)
        self.tables.update({name: table}) # insert table with name
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        del self.tables[name] #remove table with name
        


