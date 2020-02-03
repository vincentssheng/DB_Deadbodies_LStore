from lstore.table import Table

class Database():

    def __init__(self):
        # key: name
        # value: table
        self.tables = {}
        pass

    def open(self, path):
        pass

    def close(self):
        pass

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
        

