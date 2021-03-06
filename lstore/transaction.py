from lstore.table import Table, Record
from lstore.index import Index
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.results = []
        self.ret_vals = []

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
            ret_val = query(t=threading.current_thread().ident, *args)
            self.ret_vals.append(ret_val[-1])
            self.results.append(ret_val)
            # If the query has failed the transaction should abort
            if type(ret_val) is bool: 
                if ret_val is False:
                    return self.abort()
            elif type(ret_val) is int:
                continue
            elif ret_val[-1] == False:
                #print("we have to abort %s, " %(str(threading.current_thread().ident)), self.results)
                return self.abort()
        self.commit()
        return self.ret_vals

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        #print([self.queries[i][0].__name__ for i in range(len(self.results))])
        for (i, ret_val) in enumerate(self.results[:-1]):
            #print("Start undoing " + self.queries[i][0].__name__)
            query = self.queries[i][0]
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
            elif query.__name__ == 'increment':
                query(0, 0, ret_val, abort=True)
            #print("Undone " + self.queries[i][0].__name__)    
        self.commit() # remove all locks
        return False

    def commit(self):
        # TODO: commit to database
        query = self.queries[0][0]
        if query.__name__ == 'insert':
            query(t=threading.current_thread().ident, commit=True)
        elif query.__name__ == 'update':
            query(0, t=threading.current_thread().ident, commit=True)
        elif query.__name__ == 'sum':
            query(0, 0, 0, t=threading.current_thread().ident, commit=True)
        elif query.__name__ == 'select':
            query(0, 0, 0, t=threading.current_thread().ident, commit=True)
        elif query.__name__ == 'delete':
            query(0, t=threading.current_thread().ident, commit=True)
        elif query.__name__ == 'increment':
            query(0, 0, commit=True, t=threading.current_thread().ident)

        return True