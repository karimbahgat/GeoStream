
# explore also shapely.strtree.STRtree

from geosqlite import Writer, Reader

from shapely.wkb import loads as wkb_loads
from shapely.wkt import loads as wkt_loads
from shapely.geometry import shape, asShape, mapping

import pythongis as pg

from time import time

def timings():
    print 'loading'
    data = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_urban_extent_polygons_v1.01.shp", encoding='latin')
    #data = list(pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", encoding='latin')) * 3
    print len(data)

    print 'making shapely (no copy)'
    t = time()
    shapelys = [asShape(f.geometry) for f in data]
    print time()-t

    print 'making shapely (copy)'
    t = time()
    shapelys = [shape(f.geometry) for f in data]
    print time()-t

    print 'dump geoj (interface)'
    t = time()
    geojs = [s.__geo_interface__ for s in shapelys]
    print time()-t

    ##print 'dump geoj (mapping)'
    ##t = time()
    ##geojs = [mapping(s) for s in shapelys]
    ##print time()-t

    print 'load geoj asShape (no copy)'
    t = time()
    shapelys = [asShape(geoj) for geoj in geojs]
    print time()-t

    print 'load geoj shape (copy)'
    t = time()
    shapelys = [shape(geoj) for geoj in geojs]
    print time()-t

    print 'dump wkt'
    t = time()
    wkts = [s.wkt for s in shapelys]
    print time()-t

    print 'load wkt'
    t = time()
    shapelys = [wkt_loads(wkt) for wkt in wkts]
    print time()-t

    print 'dump wkb'
    t = time()
    wkbs = [s.wkb for s in shapelys]
    print time()-t

    print 'load wkb'
    t = time()
    shapelys = [wkb_loads(wkb) for wkb in wkbs]
    print time()-t

def sqlite_geoms():
    print 'load shapefile'
    t = time()
    #data = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_urban_extent_polygons_v1.01.shp", encoding='latin')
    #data = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\atlas_urban.geojson", encoding='latin')
    data = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
    #data = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", encoding='latin')
    print time()-t

    print 'making shapely'
    t = time()
    shapelys = [shape(f.geometry) for f in data] # CRUCIAL SPEEDUP, SHAPELY SHOULD BE FROM SHAPE, NOT ASSHAPE WHICH IS INDIRECT REFERENCING
    print time()-t

    print 'dump wkb'
    t = time()
    wkbs = [s.wkb for s in shapelys]
    print time()-t

    print 'convert to binary'
    from sqlite3 import Binary
    t = time()
    blobs = [Binary(wkb) for wkb in wkbs]
    print time()-t

    print 'insert wkb into db'
    fields = ['ID', 'geom']
    typs = ['int', 'BLOB']
    w = Writer('testgeodb::data', fields=zip(fields,typs), replace=True)
    t = time()
    for i,blb in enumerate(blobs):
        w.add([i, blb])
    print time()-t

    print 'load wkb from db'
    t = time()
    shapelys = [wkb_loads(bytes(blb)) for ID,blb in w.select('*')]
    print time()-t

##    print 'insert wkt into db'
##    fields = ['ID', 'geom']
##    typs = ['int', 'text']
##    w.db.close()
##    w = Writer('testgeodb::data', fields=zip(fields,typs), replace=True)
##    t = time()
##    for i,s in enumerate(shapelys):
##        w.add([i, s.wkt])
##    print time()-t
##
##    print 'load wkt from db'
##    t = time()
##    shapelys = [wkt_loads(wkt) for ID,wkt in w.select('*')]
##    print time()-t


########

#timings()
sqlite_geoms()

    




    

