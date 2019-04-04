
import pythongis as pg
import geostream as gs

TESTFILE = 'casetest.db'

stream = gs.stream.Stream(TESTFILE, 'w')

# start by clearing the database
try: stream.clear()
except Exception as err: print err
stream.clear(True)

# import datasets
print 'loading source'
source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", encoding='latin')
print 'importing source'
stream.import_table('countries', ((f.row,f.geometry) for f in source), fieldnames=source.fields, replace=True)

##print 'loading source'
##source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
##print 'importing source'
##stream.import_table('cities', ((f.row,f.geometry) for f in source), fieldnames=source.fields, replace=True)
##
##print 'loading source'
##source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
##print 'importing source'
##stream.import_table('urban', ((f.row,f.geometry) for f in source), fieldnames=source.fields, replace=True)

# inspect our stream
print 'inspect initial workspace'
stream.describe()
for tab in stream.tables():
    tab.describe()

# create spindex
print 'create spindexes'
for tab in stream.tables():
    tab.create_spatial_index('geom')
stream.describe()

# now do some things
# ...

# finally, fork the workspace and then delete it
print 'fork and delete the workspace'
forked = stream.fork('casetest_fork.db')
stream.describe()
forked.describe()
forked.delete(True)

    







