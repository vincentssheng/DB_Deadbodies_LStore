# Global Setting for the Database
# PageSize, StartRID, etc..

PAGE_SIZE = 4096 # size of one page
ENTRY_SIZE = 64 # size of entry corresponding to one column of a record
START_RID = 1 # initial RID given to first record created, grows monotonically

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3