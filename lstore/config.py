# Global Setting for the Database
# PageSize, StartRID, etc..

class Config:

    # Constants in terms of memory
    PAGE_SIZE = 1024 # size of one page 2^12 KB
    ENTRY_SIZE = 8 # size of entry in KB corresponding to one column of a record 2^3 KB
    POOL_MAX_LEN = 10 #

    # Constants in terms of number of records
    MAX_RID = pow(2,64) - 1 # maximum number of records stored
    NUM_BASE_SETS_PER_RANGE = 2 # number of sets of base pages / page range
    NUM_RECORDS_PER_SET = int(PAGE_SIZE / ENTRY_SIZE) # number of records in each page range
    NUM_BASE_RECORDS_PER_RANGE =  NUM_BASE_SETS_PER_RANGE * NUM_RECORDS_PER_SET # number of records in each page range
    NUM_RANGES = int(MAX_RID / NUM_BASE_RECORDS_PER_RANGE) # number of page ranges
    
    # Indices of columns containing metadata
    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3
    BASE_RID_COLUMN = 4
    NUM_META_COLS = 5
    INVALID_RID = 0
    BASE_INDEX = 0
    TAIL_INDEX = 1
    TODO_VALUE_TIMESTAMP = 420