
import geostream as gs

from time import time

from geographiclib.geodesic import Geodesic
geod = Geodesic.WGS84

from cPickle import dumps, loads


def create_geom(geoj):
    return gs.vector.serialize.shape(geoj)

def create_geog(geoj):
    assert geoj['type'] == 'Polygon'
    coords = geoj['coordinates'][0]
    geopol = geod.Polygon()
    for x,y in coords:
        geopol.AddPoint(y, x)
    return geopol

def geom_pickle(geom):
    return bytes(gs.vector.serialize.shapely_to_wkb(geom))

def geog_pickle(geog):
    return dumps(geog)

def geom_unpickle(geom_raw):
    return gs.vector.serialize.wkb_loads(geom_raw)

def geog_unpickle(geog_raw):
    return loads(geog_raw)

def geom_area(geom):
    return geom.area

def geog_area(geog):
    num, perim, area = geog.Compute(reverse=True)
    return area

def timed(descr, func, obj):
    t=time()
    for _ in range(100):
        func(obj)
    print descr, time()-t

tab = gs.Table(r'C:\Users\kimok\Desktop\gazetteer data\urban.db', 'natearth')
for geom in tab.select('geom'):
    if geom.geom_type == 'Polygon':
        break
geoj = geom.__geo_interface__
geom = create_geom(geoj)
geog = create_geog(geoj)
geom_raw = geom_pickle(geom)
geog_raw = geog_pickle(geog)
#geom = geom_unpickle(geom_raw)
#geog = geog_unpickle(geog_raw)

timed('create geom', create_geom, geoj)
timed('dump geom', geom_pickle, geom)
timed('load geom', geom_unpickle, geom_raw)
timed('area geom', geom_area, geom)

timed('create geog', create_geog, geoj)
timed('dump geog', geog_pickle, geog)
timed('load geog', geog_unpickle, geog_raw)
timed('area geog', geog_area, geog)



