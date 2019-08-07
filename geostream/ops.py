
import sqlite3
import math

from shapely.ops import unary_union
from shapely.geometry import Point

from .vector.serialize import from_wkb, shapely_to_wkb

# REMEMBER: All functions take the raw sqlite type, ie blob, so must convert, and then convert back to raw blob again before returning



def register_funcs(db):
    # funcs
    db.create_function("st_point", 2, makepoint)
    
    db.create_function("st_intersects", 2, intersects)
    db.create_function("st_disjoint", 2, disjoint)
    
    db.create_function("st_intersection", 2, intersection)
    db.create_function("st_union", 2, union)
    db.create_function("st_difference", 2, difference)

    db.create_function("st_area", 1, area)

    # aggregates
    db.create_aggregate("st_union", 1, UnionAgg)


###

def makepoint(x, y):
    p = Point(x, y)
    wkb = shapely_to_wkb(p)
    return wkb


def intersects(obj, other):
    # assert is shapely...
    if obj is None or other is None:
        return None
    obj = from_wkb(obj)
    other = from_wkb(other)
    return obj.intersects(other)

def disjoint(obj, other):
    # assert is shapely...
    if obj is None or other is None:
        return None
    obj = from_wkb(obj)
    other = from_wkb(other)
    return obj.disjoint(other)

def intersection(obj, other):
    # assert is shapely...
    if obj is None or other is None:
        return None
    obj = from_wkb(obj)
    other = from_wkb(other)
    res = obj.intersection(other)
    wkb = shapely_to_wkb(res)
    return wkb

def union(obj, other):
    # assert is shapely...
    if obj is None or other is None:
        return None
    obj = from_wkb(obj)
    other = from_wkb(other)
    res = obj.union(other)
    wkb = shapely_to_wkb(res)
    return wkb

def difference(obj, other):
    # assert is shapely...
    if obj is None or other is None:
        return None
    obj = from_wkb(obj)
    other = from_wkb(other)
    res = obj.difference(other)
    wkb = shapely_to_wkb(res)
    return wkb



def area(obj):
    # assert is shapely or geography...
    if obj is None:
        return None
    return obj.area
    

###

class UnionAgg:
    def __init__(self):
        self.geoms = []

    def step(self, wkb):
        # TODO: do cascaded union for every X items, to avoid memory overload
        try:
            if wkb is None:
                return None

            g = from_wkb(wkb)
            self.geoms.append(g)
            
        except Exception as EXStep:
            pass
            return None

    def finalize(self):
        try:
            union = unary_union(self.geoms)
            #print str(union)[:100]
            wkb = shapely_to_wkb(union)
            #print str(wkb)[:100]
            return wkb
        except Exception as EXFinal:
            pass
            return None



