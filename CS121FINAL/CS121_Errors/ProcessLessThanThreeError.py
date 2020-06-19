"""
Custom Error for indexer. If the number of specified processes is less than three, raise this exception.
"""


class ProcessLessThanThreeError(Exception):
    pass
