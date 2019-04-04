
import sqlite3
from sqlite3 import Binary

##from rtree.index import Index
##
##from tempfile import TemporaryFile
##
##def rtree_idx_to_blob(idx):
##    # index to pickle buffer
##    temppath = idx._temppath
##    buf = Binary(pkl)
##    return buf
##
##def from_blob(buf):
##    # pickle buffer to index
##    idx = Index().loads(bytes(buf))
##    return idx
##
##sqlite3.register_adapter(Index, idx_to_blob)
##sqlite3.register_converter('rtree', from_blob)
