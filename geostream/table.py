


class Table(object):
    def __init__(self, stream, name):
        self.name = name

        from .stream import Stream
        if not isinstance(stream, Stream):
            if isinstance(stream, str):
                stream = Stream(stream)
            else:
                raise Exception('Table "stream" arg must be a Stream instance or a filepath to a db file.')
        self.stream = stream

        if not name in self.stream.tables:
            raise Exception('Could not find a table by the name "{}"'.format(name))

    def __len__(self):
        return self.stream.c.execute('SELECT COUNT(oid) FROM {}'.format(self.name)).fetchone()[0]

    def __iter__(self):
        return self.filter()

    def _column_info(self):
        # cid,name,typ,notnull,default,pk
        return list(self.stream.c.execute('PRAGMA table_info({})'.format(self.name)))

    def add_field(self, field, typ):
        self.stream.c.execute('ALTER TABLE {name} ADD {field} {typ}'.format(name=self.name, field=field, typ=typ))

    @property
    def fields(self):
        info = self._column_info()
        return [(name,typ) for _,name,typ,_,_,_ in info]

    @property
    def fieldnames(self):
        info = self.column_info()
        return [name for _,name,_,_,_,_ in info]

    ####

    def filter(self, fields=None, where=None):
        if fields:
            fieldstring = ','.join(fields)
        else:
            fieldstring = '*'
        query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
        if where: query += ' WHERE {}'.format(where)
        return self.stream.c.execute(query)

    def add_row(self, *row, **kw):
        if row:
            questionmarks = ','.join(('?' for _ in row))
            self.stream.c.execute('INSERT INTO {} VALUES ({})'.format(self.name, questionmarks), row)
        elif kw:
            cols,vals = list(zip(*kw.items()))
            colstring = ','.join((col for col in cols))
            questionmarks = ','.join(('?' for _ in cols))
            self.stream.c.execute('INSERT INTO {} ({}) VALUES ({})'.format(self.name, colstring, questionmarks), vals)

    ####
            
##    def create_spatial_index(self, boxfunc):
##        import rtree
##        import pyqtree
##        fieldnames = self.fieldnames
##        if hasattr(self, 'spindex'):
##            self.spindex.close()
##        if os.path.exists(self.path+'.idx'):
##            os.remove(self.path+'.idx')
##        if os.path.exists(self.path+'.dat'):
##            os.remove(self.path+'.dat')
##        self.spindex = rtree.index.Index(self.path)
##        #self.spindex = pyqtree.Index([-180,-90,180,90])
##        nxt=incr=10000
##        for i,row in enumerate(self.select(['oid','*'])):
##            oid = row[0]
##            if i>=nxt:
##                print i
##                nxt+=incr
##            rowdict = dict(zip(fieldnames, row[1:]))
##            bbox = boxfunc(rowdict)
##            #lon,lat = row[1:]
##            #bbox = [lon,lat,lon,lat]
##            if bbox:
##                self.spindex.insert(oid, bbox)
##
##    def intersection(self, bbox):
##        if not hasattr(self, "spindex"):
##            raise Exception("You need to create the spatial index before you can use this method")
##        # ensure min,min,max,max pattern
##        xs = bbox[0],bbox[2]
##        ys = bbox[1],bbox[3]
##        bbox = [min(xs),min(ys),max(xs),max(ys)]
##        # return generator over results
##        ids = self.spindex.intersection(bbox)
##        #ids = self.spindex.intersect(bbox)
##        idstring = ','.join(map(str,ids))
##        return self.c.execute('SELECT * FROM {} WHERE oid IN ({})'.format(self.table, idstring))




