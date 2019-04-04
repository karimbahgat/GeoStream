
import geostream as gs

stream = gs.stream.Stream('test.db')

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

source = gs.table.Table(r"C:\Users\kimok\Desktop\gazetteer data/prepped/ciesin.db", 'data')

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

##    print test.fields
##    print test.column_info()
##
##    def boxfunc(row):
##        lon,lat = row['ciesin_lon'],row['ciesin_lat']
##        return [lon,lat,lon,lat]
##
##    test.create_spatial_index(boxfunc)
##    for r in test.intersection([20,20,30,30]):
##        print r
