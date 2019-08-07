
import geostream as gs

TESTFILE = 'test.db'

with gs.Workspace(TESTFILE, 'w') as wsp:

    wsp.clear(True)

    # test create multiple tables
    fields = ['num int'.split(), 'txt text'.split()]

    data1 = wsp.new_table('data1', fields, True)
    print data1.fields

    data2 = wsp.new_table('data2', fields, True)
    print data2.fields

    print wsp.tables

    # test adding rows
    for i,char in enumerate('hello'):
        data1.add_row(i, char)
    for i,char in enumerate('world'):
        data1.add_row(num=i, txt=char)

    # test filtering rows
    for row in data1:
        print row

    # test adding geometry
    data1.add_field('point', 'geom')
    print data1.fields

    source = gs.table.Table(r"C:\Users\kimok\Desktop\gazetteer data/prepped/natearth.db", 'data')

    print 'inserting from geojson'
    data1.begin()
    for row in source:
        x,y = row['lon'],row['lat']
        geoj = {'type':'Point', 'coordinates':(x,y)}
        data1.add_row('hello','world',geoj)#point=geoj)

    print 'inserting from shapely'
    from shapely.geometry import Point
    for row in source:
        x,y = row['lon'],row['lat']
        geom = Point(x,y)
        data1.add_row(point=geoj)

    print len(data1)

    print 'reading'
    print data1.fields
    for row in data1:
        #print row
        pass

    print 'test spatial ops'
    data1.compute('intersects', 'st_intersects(point, point)', dtype='bool')
    data1.compute('disjoint', 'st_disjoint(point, point)', dtype='bool')
    data1.compute('intersection', 'st_intersection(point, point)', dtype='geom')
    print [str(r) for r in data1._cursor().execute('select st_intersection(point, point) from data1').fetchmany(40)]



    # TODO: STRICTER API FOR AGGREGATE
    # MUST TAKE COLNAME + STAT, AS LIST OF PAIRS, OR AS DICT OF STAT + LIST OF COLNAMES
    # eg: data1.aggregate([('realcol','sum'),('point','union')])
    # or: data1.aggregate({'sum':'realcol', 'union':'point'})
    # or: data1.aggregate(realcol='sum', point='union') ???????????
    # or custom parsing??? 
    # or: data1.aggregate(['sum(realcol)','union(point)'])
    # or: data1.aggregate('sum(realcol), union(point)')
    print 'test spatial agg'
    #data1.aggregate('cast(st_union(point) as geom)').inspect()
    res = data1._cursor().execute('select st_union(point) as "union [geom]" from data1')
    print res.description
    for r in res: print r

    data1._cursor().execute('create table tempunion (hmm geom)')
    print wsp.table('tempunion').fields
    data1._cursor().execute('insert into tempunion select st_union(point) from data1')
    res = data1._cursor().execute('select * from tempunion')
    for r in res: print r

    fdsfd

    data1._cursor().execute('create table tempunion as select st_union(point) from data1')
    for col in data1._cursor().execute('pragma tableinfo(tempunion)'):
        print 'col',col
    print wsp.table('tempunion').fields
    res = data1._cursor().execute('select * from tempunion')
    for r in res: print r

    union = data1.aggregate('st_union(point) as "union [geom]"')
    print union.fields
    union.inspect()

    print data1

    

    print 'reading'
    print data1.fields
    data1.inspect(40)

    print 'creating spindex'
    data1.create_spatial_index('point')

    print 'query spindex'
    x,y = 11,59
    buf = 1
    for row in data1.intersection('point', [x-buf,y-buf,x+buf,y+buf]):
        print row

    print 'store spindex'
    data1.store_spatial_index('point')

    print 'query spindex again'
    x,y = 11,59
    buf = 1
    for row in data1.intersection('point', [x-buf,y-buf,x+buf,y+buf]):
        print row

    #data1.store_spatial_index('point')
    #for r in wsp.table('spatial_indexes'):
    #    print r


with gs.Workspace(TESTFILE, 'r') as wsp:
    print 'query spindex again (after reloading)'
    for r in wsp.table('spatial_indexes'):
        print r

    data1 = wsp.table('data1')
    for row in data1.intersection('point', [x-buf,y-buf,x+buf,y+buf]):
        print row




