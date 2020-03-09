from lstore.table import Table, Record
from lstore.index import Index
from lstore.transaction import Transaction

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    def run(self):
        for i in range(len(self.transactions)):
            transaction = Transaction()
            transaction.queries = [query for query in self.transactions[i].queries]
            
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        print(len(self.stats))
        self.result = len(list(filter(lambda x: x, self.stats)))