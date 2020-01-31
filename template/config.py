# Global Setting for the Database
# PageSize, StartRID, etc..

PAGE_SIZE = 4096 # size of one page 2^12 KB
ENTRY_SIZE = 8 # size of entry in KB corresponding to one column of a record 2^3 KB
START_RID = 1 # initial RID given to first record created, grows monotonically
MAX_NUM_RECORDS = pow(2,33) - 1 # maximum number of records stored
NUM_SETS = 2 # number of sets of base pages / page range

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3