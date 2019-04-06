
import pythongis as pg
import geostream as gs

TESTFILE = 'casetest.db'

workspace = gs.Workspace(TESTFILE, 'w')

# start by clearing the database
try: workspace.clear()
except Exception as err: print err
workspace.clear(True)

# import datasets
print 'importing datasets'

if 0:
    # timing test
    print 'timing tests'
    from time import time
    t=time()
    pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
    print 'pythongis', time()-t
    t=time()
    workspace.import_table('cities', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", replace=True, encoding='latin')
    print 'stream import', time()-t
    t=time()
    for row in workspace.table('cities'):
        pass
    print 'streaming from db', time()-t
    workspace.clear(True)

workspace.import_table('un_messy', r"C:\Users\kimok\Desktop\gazetteer data\raw\WUP2018-F12-Cities_Over_300K.xls", replace=True, skip=16)
workspace.import_table('un', r"C:\Users\kimok\Desktop\gazetteer data\extracted\un.csv", replace=True)
workspace.import_table('countries', r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", replace=True)
#workspace.import_table('cities', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", replace=True, encoding='latin')
#workspace.import_table('urban', r"C:\Users\kimok\Desktop\gazetteer data\raw\global_urban_extent_polygons_v1.01.shp", replace=True, encoding='latin')

# inspect our stream
print 'inspect initial workspace'
workspace.describe()
for tab in workspace.tables():
    tab.describe()

# calc some stats
print 'calc some stats'
countries = workspace.table('countries')
print '# values()'
for reg in countries.values('subregion', order='subregion'):
    print reg
print '# groupby()'
for k,gr in countries.groupby('iso_a2', by='subregion', order='subregion'):
    print k,len(list(gr))
print '# aggregate()'
for row in countries.aggregate('count(iso_a2)', by='subregion'):
    print row
for row in countries.aggregate('avg(pop_est)', by='subregion'):
    print row

# now do some geo things
# create spindex
print 'create spindexes'
for tab in workspace.tables():
    tab.create_spatial_index('geom')
workspace.describe()
# and more...
# ...

# finally, fork the workspace and then delete it
print 'fork and delete the workspace'
forked = workspace.fork('casetest_fork.db')
workspace.describe()
forked.describe()
forked.delete(True)

    







