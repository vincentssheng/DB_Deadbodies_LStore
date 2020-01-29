from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return PAGE_SIZE - self.num_records*ENTRY_SIZE

    def write(self, value):
        self.num_records += 1
        pass

