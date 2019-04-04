
import pythongis as pg
import geostream as gs

TESTFILE = 'casetest.db'

with gs.stream.Stream(TESTFILE, 'w') as stream:

    # import datasets
    print 'loading source'
    source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\ne_10m_admin_0_countries.shp", encoding='latin')
    print 'importing source'
    stream.import_table('countries', (f.row for f in source), fieldnames=source.fields, replace=True)
    
##    print 'loading source'
##    source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
##    print 'importing source'
##    stream.import_table('cities', (f.row for f in source), fieldnames=source.fields, replace=True)
##
##    print 'loading source'
##    source = pg.VectorData(r"C:\Users\kimok\Desktop\gazetteer data\raw\global_settlement_points_v1.01.shp", encoding='latin')
##    print 'importing source'
##    stream.import_table('urban', (f.row for f in source), fieldnames=source.fields, replace=True)

    # inspect our stream
    stream.describe()
    for tab in stream.tables():
        tab.describe()

    # now do some things
    # ...
    







