
import geostream as gs


with gs.stream.Stream('test.db', 'w') as stream:

    # test create multiple tables
    fields = ['num int'.split(), 'txt text'.split()]

    data1 = stream.new_table('data1', fields, True)
    print data1.fields

    data2 = stream.new_table('data2', fields, True)
    print data2.fields

    print stream.tables

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
    for row in source:
        x,y = row[2:4]
        geoj = {'type':'Point', 'coordinates':(x,y)}
        data1.add_row(point=geoj)

    print 'inserting from shapely'
    from shapely.geometry import Point
    for row in source:
        x,y = row[2:4]
        geom = Point(x,y)
        data1.add_row(point=geoj)

    print len(data1)

    print 'reading'
    for row in data1:
        pass

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
    #for r in stream.table('spatial_indexes'):
    #    print r


with gs.stream.Stream('test.db', 'r') as stream:
    print 'query spindex again (after reloading)'
    for r in stream.table('spatial_indexes'):
        print r

    data1 = stream.table('data1')
    for row in data1.intersection('point', [x-buf,y-buf,x+buf,y+buf]):
        print row




