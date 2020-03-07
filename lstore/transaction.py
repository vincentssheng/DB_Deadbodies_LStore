from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.results = []

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            ret_val = query(t=self, *args)
            self.results.append(ret_val)
            # If the query has failed the transaction should abort
            if ret_val[-1] == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        for (i, ret_val) in enumerate(self.results):
            if not ret_val[-1]:
                return False

            query = self.queries[i]
            if query.__name__ == 'insert':
                query(ret_val, abort=True)
            elif query.__name__ == 'update':
                query(0, ret_val, abort=True)
            elif query.__name__ == 'sum':
                query(0, 0, 0, abort=True)
            elif query.__name__ == 'select':
                query(0, 0, 0, abort=True)
            elif query.__name__ == 'delete':
                query(0, ret_val, abort=True)
            
        return False

    def commit(self):
        # TODO: commit to database
        query = self.queries[0]
        if query.__name__ == 'insert':
            query((self), commit=True)
        elif query.__name__ == 'update':
            query(0, (self), commit=True)
        elif query.__name__ == 'sum':
            query(0, 0, 0, (self), commit=True)
        elif query.__name__ == 'select':
            query(0, 0, 0, (self), commit=True)
        elif query.__name__ == 'delete':
            query(0, (self), commit=True)

        return True