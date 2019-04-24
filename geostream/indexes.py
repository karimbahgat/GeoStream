
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

class Spatial_Index_Manager:
    pass

class RTreeIndex:
    pass

class PyramidIndex:
    pass

# Backends

class _RTreeBackend:
    pass

class _PyRTreeBackend:
    pass

class QuadTreeIndex:
    pass

class _PyqtreeBackend:
    pass

class _PyramidTableBackend:
    # see https://www.researchgate.net/publication/311423420_An_Efficient_Tile-Pyramids_Building_Method_for_Fast_Visualization_of_Massive_Geospatial_Raster_Datasets
    pass





