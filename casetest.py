
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

##print 'loading source'
##source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
##print 'importing source'
##stream.import_table('urban', ((f.row,f.geometry) for f in source), fieldnames=source.fields, replace=True)

# inspect our stream
print 'inspect initial workspace'
stream.describe()
for tab in stream.tables():
    tab.describe()

# calc some stats
print 'calc some stats'
countries = stream.table('countries')
print '# values()'
for reg in countries.values('subregion'):
    print reg
print '# groupby()'
for k,gr in countries.groupby('iso_a2', by='subregion'):
    print k,len(list(gr))
print '# aggregate()'
for row in countries.aggregate(['subregion','count(iso_a2)'], by='subregion'):
    print row
for row in countries.aggregate(['subregion','avg(pop_est)'], by='subregion'):
    print row
fsdfsd

# now do some geo things
# create spindex
print 'create spindexes'
for tab in stream.tables():
    tab.create_spatial_index('geom')
stream.describe()

# finally, fork the workspace and then delete it
print 'fork and delete the workspace'
forked = stream.fork('casetest_fork.db')
stream.describe()
forked.describe()
forked.delete(True)

    







