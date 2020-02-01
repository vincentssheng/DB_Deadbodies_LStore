# Global Setting for the Database
# PageSize, StartRID, etc..

class Config:

    # Constants in terms of memory
    PAGE_SIZE = 4096 # size of one page 2^12 KB
    ENTRY_SIZE = 8 # size of entry in KB corresponding to one column of a record 2^3 KB

    # Constants in terms of number of records
    MAX_RID = pow(2,64) - 2 # maximum number of records stored
    TAIL_FRACTION= 0.5 # fraction of tail pages
    NUM_SETS_PER_RANGE = 2 # number of sets of base pages / page range
    NUM_RECORDS_PER_SET = PAGE_SIZE / ENTRY_SIZE # number of records in each page range
    NUM_RECORDS_PER_RANGE = NUM_SETS_PER_RANGE * NUM_RECORDS_PER_SET * (1/TAIL_FRACTION) # number of records in each page range
    NUM_TAIL_PER_RANGE = NUM_RECORDS_PER_RANGE * TAIL_FRACTION
    NUM_BASE_PER_RANGE = NUM_RECORDS_PER_RANGE - NUM_TAIL_PER_RANGE
    NUM_RANGES = MAX_RID / NUM_RECORDS_PER_RANGE # number of page ranges
    
    # Indices of columns containing metadata
    INDIRECTION_COLUMN = 0
    RID_COLUMN = 1
    TIMESTAMP_COLUMN = 2
    SCHEMA_ENCODING_COLUMN = 3
    NUM_META_COLS = 4
    TODO_VALUE_TIMESTAMP = 420