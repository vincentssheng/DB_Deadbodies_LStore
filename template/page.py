from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(Config.PAGE_SIZE)

    def has_capacity(self):
        if self.num_records < Config.NUM_RECORDS_PER_SET:
            return True
        return False

    def write(self, value):
        self.num_records += 1
        # write value to page?
        pass

    def read(offset):
        pass

