from template.config import *
import sys

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(Config.PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < Config.NUM_RECORDS_PER_SET:
            return True
        return False

    def write(self, offset, value):
        self.num_records += 1
        byte_value = value.to_bytes(Config.ENTRY_SIZE, sys.byteorder)
        self.data[offset*Config.ENTRY_SIZE : ((offset+1)*Config.ENTRY_SIZE-1)] = byte_value

    def read(self, offset):
        pass

