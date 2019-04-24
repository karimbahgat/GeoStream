
import pythongis as pg
import geostream as gs

def view(results, text=False):
    import pythongis as pg
    # setup map
    m = pg.renderer.Map()
    m.add_layer(r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", fillcolor='gray')
    # options
    kwargs = {}
    if text:
        kwargs.update(text=lambda f: f[text][:20], textoptions=dict(textsize=3))
    # add
    d = pg.VectorData(fields=[])
    for row in results:
        d.add_feature([], row[-1].__geo_interface__)
    m.add_layer(d, fillcolor='blue', **kwargs)
    # view
    m.view()



TESTFILE = 'casetest.db'
SETUP = False

workspace = gs.Workspace(TESTFILE, 'w')

if SETUP:
    # start by clearing the database
    ##try: workspace.clear()
    ##except Exception as err: print err
    ##workspace.clear(True)

    # import datasets
    print 'importing datasets'

    ##if 0:
    ##    # timing test
    ##    print 'timing tests'
    ##    from time import time
    ##    t=time()
    ##    pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
    ##    print 'pythongis', time()-t
    ##    t=time()
    ##    workspace.import_table('cities', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", replace=True, encoding='latin')
    ##    print 'stream import', time()-t
    ##    t=time()
    ##    for row in workspace.table('cities'):
    ##        pass
    ##    print 'streaming from db', time()-t
    ##    workspace.clear(True)

    # Test imports
    workspace.import_table('countries', r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", replace=True)
    workspace.import_table('ne_cities', r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_populated_places.shp", replace=True)
    workspace.import_table('roads', r"C:\Users\kimok\Desktop\misctests\MajorRoads.shp", replace=True)

    #workspace.import_table('un_messy', r"C:\Users\kimok\Desktop\gazetteer data\raw\WUP2018-F12-Cities_Over_300K.xls", replace=True, skip=16)
    #workspace.import_table('un', r"C:\Users\kimok\Desktop\gazetteer data\extracted\un.csv", replace=True)
    #workspace.import_table('ciesin_cities', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", replace=True, encoding='latin')
    #workspace.import_table('urban', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_urban_extent_polygons_v1.01.shp", replace=True, encoding='latin')
    #workspace.import_table('gns_raw', r"C:\Users\kimok\Desktop\gazetteer data\raw\Countries_populatedplaces_p.txt", replace=True)

    ##workspace.import_table('mammals', r"C:\Users\kimok\Desktop\misctests\TERRESTRIAL_MAMMALS.shp", replace=True, encoding='latin')

    ##reader = gs.raster.load.file_reader(r"P:\Freelance\Projects\Henry City Data\Work Files\Sources\GlobCover\GLOBCOVER_L4_200901_200912_V2.3.tif")
    ##print reader
    ##fdsfd

    # Large file test
    ##print 'large pure approach'
    ##import csv
    ##import sys
    ##from time import time
    ##csv.field_size_limit(sys.maxint)
    ##with open(r"C:\Users\kimok\Desktop\gazetteer data\raw\planet-latest_geonames.tsv", 'rb') as robj:
    ##    robj.seek(0, 2)
    ##    end = robj.tell()
    ##    robj.seek(0)
    ##    reader = csv.reader(robj, delimiter='\t')
    ##    for attr in 'delimiter doublequote escapechar lineterminator quotechar quoting skipinitialspace strict'.split():
    ##        print attr, repr(getattr(reader.dialect, attr))
    ##    fieldnames = next(reader)
    ##    print fieldnames
    ##    nxt=incr=10000
    ##    for i,row in enumerate(reader):
    ##        if len(row) != 24:
    ##            print len(row)
    ##        if i >= nxt:
    ##            print robj.tell()/float(end)
    ##            nxt+=incr
    ##print time()-t, 'seconds'
    ##
    ##print 'large stream approach'
    ##workspace.import_table('osm_raw', r"C:\Users\kimok\Desktop\gazetteer data\raw\planet-latest_geonames.tsv", replace=True,
    ##                       doublequote=1, quotechar='"') # autodetect gets it wrong, override
    ##
    ##fdsfsd

    # create spindex
    print 'create spindexes'
    for tab in workspace.tables():
        if 'geom' in tab.fieldnames:
            tab.create_spatial_index('geom')
    workspace.describe()

# inspect our workspace
print 'inspect initial workspace'
workspace.describe()
for tab in workspace.tables():
    tab.describe()
    for field in tab.fieldnames[:3]:
        tab.describe(field)

# calc some stats
print 'calc some stats'
countries = workspace.table('countries')
cities = workspace.table('ne_cities')
roads = workspace.table('roads')
print '# values()'
for reg in countries.values('subregion', order='subregion'):
    print reg
print '# groupby()'
for k,gr in countries.groupby(by='subregion', keep_fields='iso_a2', order='subregion'):
    print k,len(list(gr))
print '# aggregate()'
for row in countries.aggregate('count(iso_a2)', by='subregion'):
    print row
for row in countries.aggregate('avg(pop_est)', by='subregion'):
    print row
print '# join() streaming'
for row in countries.join(cities, 'countries.iso_a3 = ne_cities.ADM0_A3',
                          keep_fields=['countries.NAME', 'ne_cities.NAME']):
    pass 
##print '# join() with output'
##joined_table = countries.join(cities, 'countries.iso_a3 = ne_cities.ADM0_A3',
##                              keep_fields=['countries.NAME', 'ne_cities.NAME'], #, 'ne_cities.geom'],
##                              output='joined', replace=True)
##print joined_table

# now do some geo things
# ...

# maybe getting features in a country
print countries # figure out what fields
print list(countries.values('NAME')) # get possible country names
#view(cities.intersection('geom', countries.get(where="name='Angola'")[-1].bounds))
view(roads.intersection('geom', countries.get(where="name='Angola'")[-1].bounds))

# and more...
# ...

# finally, fork the workspace and then delete it
print 'fork and delete the workspace'
forked = workspace.fork('casetest_fork.db')
workspace.describe()
forked.describe()
forked.delete(True)

    







