
import sqlite3
from sqlite3 import Binary

from shapely.wkb import loads as wkb_loads
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry


def shapely_to_wkb(shp):
    # shapely to wkb buffer
    wkb = shp.wkb
    buf = Binary(wkb)
    return buf

def geoj_to_wkb(geoj):
    # geojson to wkb buffer
    wkb = shape(geoj).wkb
    buf = Binary(wkb)
    return buf

def from_wkb(wkb_buf):
    # wkb buffer to shapely
    shp = wkb_loads(bytes(wkb_buf))
    return shp


sqlite3.register_adapter(BaseGeometry, shapely_to_wkb)
sqlite3.register_adapter(dict, geoj_to_wkb)
sqlite3.register_converter('geom', from_wkb)
