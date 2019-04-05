
from tempfile import TemporaryFile

from sqlite3 import Binary

class Table(object):
    def __init__(self, stream, name):
        self.name = name

        from .stream import Stream
        if not isinstance(stream, Stream):
            if isinstance(stream, basestring):
                stream = Stream(stream)
            else:
                raise Exception('Table "stream" arg must be a Stream instance or a filepath to a db file.')
        self.stream = stream

        if not name in self.stream.tablenames + self.stream.metatablenames:
            raise Exception('Could not find a table by the name "{}"'.format(name))

    def __str__(self):
        ident = '  '
        numgeoms = self.get('COUNT(oid)', where='geom IS NOT NULL')
        numspindex = self.stream.table('spatial_indexes').get('COUNT(oid)', where="tbl = '{}'".format(self.name))
        lines = ['Streaming Table:',
                 ident+'Name: "{}"'.format(self.name),
                 ident+'Rows ({})'.format(len(self)),
                 ident+'Geometries ({})'.format(numgeoms),
                 ident+'Spatial Indexes ({})'.format(numspindex),
                 ident+'Fields ({})'.format(len(self.fieldnames))]
        fieldlist = ['{} ({})'.format(name,typ) for name,typ in self.fields]
        lines += [ident*2 + '\t\t\t'.join(fieldlist[i:i+4])
                  for i in range(0, len(fieldlist), 4)]
        descr = '\n'.join(lines)
        return descr

    def __len__(self):
        return self.get('COUNT(oid)')

    def __iter__(self):
        return self.filter()

    #### Hidden

    def _column_info(self):
        # cid,name,typ,notnull,default,pk
        return list(self.stream.c.execute('PRAGMA table_info({})'.format(self.name)))

    #### Fields

    def add_field(self, field, typ):
        self.stream.c.execute('ALTER TABLE {name} ADD {field} {typ}'.format(name=self.name, field=field, typ=typ))

    @property
    def fields(self):
        info = self._column_info()
        return [(name,typ) for _,name,typ,_,_,_ in info]

    @property
    def fieldnames(self):
        info = self._column_info()
        return [name for _,name,_,_,_,_ in info]

    #### Metadata

    def describe(self):
        print(self.__str__())

    #### Basic Functions

    def set(self, values=None, where=None):
        cols,vals = zip(*values.items())
        valuestring = ', '.join(('{} = ?'.format(col) for col in cols))
        query = 'UPDATE {} SET {}'.format(self.name, valuestring)
        if where: query += ' WHERE {}'.format(where)
        #print query
        self.stream.c.execute(query, vals)

    def get(self, fields=None, where=None):
        return next(self.filter(fields, where, limit=1))

    def filter(self, fields=None, where=None, limit=None):
        if fields:
            if isinstance(fields, basestring):
                fields = [fields]
            fieldstring = ','.join(fields)
        else:
            fieldstring = '*'
        query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
        if where: query += ' WHERE {}'.format(where)
        if limit: query += ' LIMIT {}'.format(limit)
        #print query
        result = self.stream.c.execute(query)
        if len(fields) == 1:
            return (row[0] for row in result)
        else:
            return result

    def add_row(self, *row, **kw):
        if row:
            questionmarks = ','.join(('?' for _ in row))
            self.stream.c.execute('INSERT INTO {} VALUES ({})'.format(self.name, questionmarks), row)
        elif kw:
            cols,vals = list(zip(*kw.items()))
            colstring = ','.join((col for col in cols))
            questionmarks = ','.join(('?' for _ in cols))
            self.stream.c.execute('INSERT INTO {} ({}) VALUES ({})'.format(self.name, colstring, questionmarks), vals)

    #### Manipulations

    def join(self):
        pass

    #### Stats

    def values(self, fields, where=None, order=None, limit=None):
        if isinstance(fields, basestring):
            fields = [fields]
        fieldstring = ', '.join(fields)
        query = 'SELECT DISTINCT {} FROM {}'.format(fieldstring, self.name)
        if where: query += ' WHERE {}'.format(where)
        if limit: query += ' LIMIT {}'.format(limit)
        if order:
            ordstring = ', '.join(order)
            query += ' ORDER BY {}'.format(ordstring)
        # return
        result = self.stream.c.execute(query)
        if len(fields) == 1:
            return (row[0] for row in result)
        else:
            return result

    def groupby(self, fields, by, where=None, order=None):
        uniq = self.values(by, where=where, order=order)
        uniq = list(uniq)
        if isinstance(by, basestring):
            by = [by]
            uniq = ([v] for v in uniq)
        for u in uniq:
            u = ["'{}'".format(v) if isinstance(v, basestring) else "{}".format(v)
                 for v in u]
            byvals = zip(by, u)
            wherestring = ' AND '.join(['{} = {}'.format(b, v) for b,v in byvals])
            query = self.filter(fields, where=wherestring)
            if len(by) == 1:
                u = u[0]
            yield u,query

    def aggregate(self, stats, by=None, where=None, order=None, limit=None):
        statstring = ', '.join(stats)
        query = 'SELECT {} FROM {}'.format(statstring, self.name)
        if where: query += ' WHERE {}'.format(where)
        if by:
            if isinstance(by, basestring):
                by = [by]
            bystring = ', '.join(by)
            query += ' GROUP BY {}'.format(bystring)
        if order:
            ordstring = ', '.join(order)
            query += ' ORDER BY {}'.format(ordstring)
        result = self.stream.c.execute(query)
        if len(stats) == 1:
            return (row[0] for row in result)
        else:
            return result

    #### Spatial indexing

    # TODO: Move all spatial indexing to separate class
    # incl moving the spatial_indexes memory dict to the new class
    # ...
            
    def create_spatial_index(self, geofield):
        # create the spatial index
        import rtree
        temppath = TemporaryFile().name
        spindex = rtree.index.Index(temppath)
        spindex._temppath = temppath
        self.stream.spatial_indexes[(self.name, geofield)] = spindex

        # NOTE: Maybe consider using generator as input to Index()
        # but this seems to only be in memory then, and not on file
        
        nxt=incr=10000
        for i,(oid,geom) in enumerate(self.filter(['oid',geofield])):
            if i>=nxt:
                print i
                nxt+=incr
            if geom:
                bbox = geom.bounds
                spindex.insert(oid, bbox)
                
        # add new entry to the spatial index table
        idxtable = self.stream.table('spatial_indexes')
        idxtable.add_row(tbl=self.name, col=geofield)

    def load_spatial_index(self, geofield):
        if (self.name,geofield) in self.stream.spatial_indexes:
            # already loaded in spatial_indexes dict
            spindex = self.stream.spatial_indexes[(self.name,geofield)]
        else:
            # load idx and dat blob data
            idxtable = self.stream.table('spatial_indexes')
            (idx,dat) = idxtable.get(['rtree_idx','rtree_dat'], where="tbl = '{}' AND col = '{}' ".format(self.name, geofield))
            # write to temp file
            temppath = TemporaryFile().name
            with open(temppath+'.idx', 'wb') as fobj:
                fobj.write(idx)
            with open(temppath+'.dat', 'wb') as fobj:
                fobj.write(dat)
            import rtree
            # load index from temp file
            spindex = rtree.index.Index(temppath)
            spindex._temppath = temppath
            # update spatial_indexes dict
            self.stream.spatial_indexes[(self.name,geofield)] = spindex
        return spindex

    def store_spatial_index(self, geofield):
        spindex = self.stream.spatial_indexes.pop((self.name,geofield))
        spindex.close()
        temppath = spindex._temppath
        # get the byte data from the files
        with open(temppath+'.idx', 'rb') as fobj:
            idx = Binary(fobj.read())
        with open(temppath+'.dat', 'rb') as fobj:
            dat = Binary(fobj.read())
        # update the idx and dat columns in the spatial_indexes table
        idxtable = self.stream.table('spatial_indexes')
        idxtable.set(dict(rtree_idx=idx, rtree_dat=dat),
                     where="tbl = '{}' AND col = '{}' ".format(self.name, geofield))

    def intersection(self, geofield, bbox):
        #if not hasattr(self, "spindex"):
        #    raise Exception("You need to create the spatial index before you can use this method")
        # ensure min,min,max,max pattern
        xs = bbox[0],bbox[2]
        ys = bbox[1],bbox[3]
        bbox = [min(xs),min(ys),max(xs),max(ys)]
        # load the spindex
        spindex = self.load_spatial_index(geofield)
        # return generator over results
        ids = spindex.intersection(bbox)
        #ids = self.spindex.intersect(bbox)
        idstring = ','.join(map(str,ids))
        return self.filter(where='oid IN ({})'.format(idstring))




