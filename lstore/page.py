from lstore.config import *
import sys

class Page:

    def __init__(self, path, location):
        self.data = bytearray(Config.PAGE_SIZE)
        self.path = path
        self.location = location
        self.num_records = 0
        self.pin_count = 0
        self.dirty = False
        self.lineage = Config.MAX_RID

    def has_capacity(self):
        if self.num_records < Config.NUM_RECORDS_PER_SET:
            return True
        return False

    def write(self, offset, value):
        self.dirty = True
        self.num_records += 1
        self.data[offset*Config.ENTRY_SIZE:((offset+1)*Config.ENTRY_SIZE)] = value

    def read(self, offset):
        return self.data[offset*Config.ENTRY_SIZE:((offset+1)*Config.ENTRY_SIZE)]

