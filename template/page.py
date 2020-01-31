from template.config import *

class Page:

    NUM_RECORDS_PER_PAGE_SET = PAGE_SIZE / ENTRY_SIZE

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < self.NUM_RECORDS_PER_PAGE_SET:
            return True
        return False

    def write(self, value):
        self.num_records += 1
        pass

    def read(offset):
        pass

