# Global Setting for the Database
# PageSize, StartRID, etc..

class Config:

    PAGE_SIZE = 4096 # size of one page 2^12 KB
    ENTRY_SIZE = 8 # size of entry in KB corresponding to one column of a record 2^3 KB
    START_RID = 1 # initial RID given to first record created, grows monotonically
    MAX_NUM_RECORDS = pow(2,33) # maximum number of records stored
    NUM_SETS = 2 # number of sets of base pages / page range
    NUM_RECORDS_PER_PAGE_SET = PAGE_SIZE / ENTRY_SIZE
    TAIL_TO_BASE_RATIO = 1
    RANGE_SIZE = NUM_SETS * NUM_RECORDS_PER_PAGE_SET * (TAIL_TO_BASE_RATIO + 1)
    NUM_RANGES = MAX_NUM_RECORDS / RANGE_SIZE
    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3