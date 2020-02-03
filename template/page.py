from lstore.config import *
import sys

class Page:

    num_records = 0
    def __init__(self):
        self.data = bytearray(Config.PAGE_SIZE)

    def has_capacity(self):
        print(self.num_records)
        if self.num_records == Config.NUM_RECORDS_PER_SET:
            return False
        return True

    def write(self, offset, value):
        self.num_records += 1
        self.data[offset*Config.ENTRY_SIZE:((offset+1)*Config.ENTRY_SIZE)] = value

    def read(self, offset):
        return self.data[offset*Config.ENTRY_SIZE:((offset+1)*Config.ENTRY_SIZE)]

