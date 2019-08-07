
import os
import warnings
from tempfile import TemporaryFile, mktemp

import sqlite3
from sqlite3 import Binary

from .verbose import track_progress
from . import vector
from . import raster

class Row(sqlite3.Row):
    def __str__(self):
        return 'Row: {}'.format(tuple(self).__str__())

class Table(object):
    def __init__(self, workspace, name, mode='r'):
        self.name = name

        from . import Workspace
        if not isinstance(workspace, Workspace):
            if isinstance(workspace, basestring):
                workspace = Workspace(workspace, mode=mode)
            else:
                raise Exception('Table "workspace" arg must be a Workspace instance or a filepath to a db file.')
        self.workspace = workspace

        if not name.lower().startswith(('temp','temporary')) and name not in self.workspace.tablenames + self.workspace.metatablenames:
            raise Exception('Could not find a table by the name "{}"'.format(name))

    def __str__(self):
        # TODO: Consider building custom sql query to retrieve all info at once
        ident = '  '
        numgeoms = self.get('COUNT(oid)', where='geom IS NOT NULL') if 'geom' in self.fieldnames else 0
        numspindex = self.workspace.table('spatial_indexes').get('COUNT(oid)', where="tbl = '{}'".format(self.name)) if 'spatial_indexes' in self.workspace.tablenames else 0
        lines = ['Streaming Table:',
                 ident+'Name: "{}"'.format(self.name),
                 ident+'Rows ({})'.format(len(self)),
                 ident+'Geometries ({})'.format(bool(numgeoms)),
                 ident+'Spatial Indexes ({})'.format(numspindex),
                 ident+'Fields ({})'.format(len(self.fieldnames))]
        fields = list(self.fields)
        maxlen = 25
        fields = [(name[:maxlen-len(str(typ))-3-3]+u'...',typ) if (len(name)+len(str(typ))+3) > maxlen else (name,typ)
                       for name,typ in fields] # limit display length of individual fieldnames
        fieldlist = ['{} ({})'.format(name,typ) for name,typ in fields]
        fieldlist = [v.ljust(maxlen) for v in fieldlist]
        lines += [ident*2 + '\t\t'.join(fieldlist[i:i+4])
                  for i in range(0, len(fieldlist), 4)]
        descr = '\n'.join(lines)
        return descr

    def __len__(self):
        # this is sometimes insanely slow for very large tables
        # alternatively iterate through oid with enumerator
        #for i,_ in enumerate(self.select('oid')):
        #    pass
        #return i
        return self.get('COUNT(oid)')

    def __iter__(self):
        query = 'SELECT * FROM {}'.format(self.name)
        cur = self._cursor()
        result = cur.execute(query)
        if len(self.fields) == 1:
            return (row[0] for row in result)
        else:
            return result

    def __getitem__(self, i):
        limit = 1
        offset = i
        query = 'SELECT * FROM {} LIMIT {} OFFSET {}'.format(self.name, limit, offset)
        return self._fetchall(query)[0]


    #### Hidden

    def _fetchall(self, query, vals=None):
        return self.workspace._fetchall(query, vals)

    def _cursor(self):
        return self.workspace._cursor()

    def _column_info(self):
        # cid,name,typ,notnull,default,pk
        if '.' in self.name:
            schema,name = self.name.split('.')
            query = 'PRAGMA {}.table_info({})'.format(schema, name)
        else:
            query = 'PRAGMA table_info({})'.format(self.name)
        return list(self._fetchall(query))

    def _query_to_table(self, query, vals=None, output=False, replace=False):
        if output:
            if replace:
                self._fetchall('DROP TABLE IF EXISTS {}'.format(output))
            query = 'CREATE TABLE {} AS {}'.format(output, query)
        else:
            tempname = os.path.split(mktemp())[1]
            output = 'temp.{}'.format(tempname)
            query = 'CREATE TEMP TABLE {} AS {}'.format(tempname, query)
        self._fetchall(query, vals)
        return output

    #### Optimizing

    def begin(self):
        self.workspace.begin()

    def commit(self):
        self.workspace.commit()

    #### Fields

    def add_field(self, field, dtype):
        typ = dtype
        cur = self._cursor()
        cur.execute('ALTER TABLE {name} ADD {field} {typ}'.format(name=self.name, field=field, typ=typ))

##        # enforce that failed type conversions become NULL
##        trigname = '{}_enforce_failed_type_insert'.format(self.name.replace('.','_'))
##        cur.execute('DROP TRIGGER IF EXISTS {}'.format(trigname))
##        fieldstring = ', '.join(("{field} = (CASE WHEN TYPEOF({field}) LIKE '{typ}%' THEN {field} ELSE NULL END)".format(field=fn,typ=typ) for fn,typ in self.fields))
##        query = ''' CREATE TRIGGER {trigname} AFTER INSERT ON {table}
##                    BEGIN
##                        UPDATE {tableonly}
##                        SET {fieldstring}
##                        WHERE oid = NEW.oid;
##                    END;'''.format(trigname=trigname, table=self.name, tableonly=self.name.split('.')[-1], fieldstring=fieldstring)
##        cur.execute(query)
##
##        # and same for updates
##        trigname = '{}_enforce_failed_type_update'.format(self.name.replace('.','_'))
##        cur.execute('DROP TRIGGER IF EXISTS {}'.format(trigname))
##        fieldstring = ', '.join(("{field} = (CASE WHEN TYPEOF({field}) LIKE '{typ}%' THEN {field} ELSE NULL END)".format(field=fn,typ=typ) for fn,typ in self.fields))
##        query = ''' CREATE TRIGGER {trigname} AFTER UPDATE ON {table}
##                    BEGIN
##                        UPDATE {tableonly}
##                        SET {fieldstring}
##                        WHERE oid = NEW.oid;
##                    END;'''.format(trigname=trigname, table=self.name, tableonly=self.name.split('.')[-1], fieldstring=fieldstring)
##        cur.execute(query)

        cur.close() 

    @property
    def fields(self):
        info = self._column_info()
        return [(name,typ) for _,name,typ,_,_,_ in info]

    @property
    def fieldnames(self):
        info = self._column_info()
        return [name for _,name,_,_,_,_ in info]

    #### Metadata

    def describe(self, field=None, **kwargs):
        # TODO: Consider building custom sql query to retrieve all info at once
        if field:
            ident = '  '
            typ = next((t for f,t in self.fields if f == field)) #self.fieldtypes[self.fieldnames.index(field)]
            typ = typ.lower()
            lines = ['Column Field:',
                     ident+u'Name: "{}"'.format(field),
                     ident+u'Data Type: {}'.format(typ)]
            
            if typ == 'geom':
                spindex = self.workspace.table('spatial_indexes').get('COUNT(oid)', where="tbl = '{}' and fld = '{}'".format(self.name, field))
                lines += [ident+'Spatial Index: {}'.format(bool(spindex))]
                # some more too
                # ...
            
            elif typ == 'text':
                # TODO: Maybe order by most common counts first, instead of alphabetical
                valuecounts = list(self.aggregate('count(oid)', by=field, order=field, **kwargs))
                lines += [ident+'Unique Values ({}):'.format(len(valuecounts))]
                maxlen = 25
                valuecounts = [(val[:maxlen-len(str(cnt))-3-3]+u'...',cnt) if (len(val)+len(str(cnt))+3) > maxlen else (val.ljust(maxlen-len(str(cnt))-3),cnt)
                               for val,cnt in valuecounts] # limit length of individual values
                valuelist = [u'{} ({})'.format(val,cnt) for val,cnt in valuecounts[:400]] 
                lines += [ident*2 + '\t\t'.join(valuelist[i:i+4])
                          for i in range(0, len(valuelist), 4)]
                if len(valuecounts) > 400:
                    # Limit in case way too many values
                    lines += [ident*2 + '...and {} more results not displayed here'.format(len(valuecounts)-400)] 
                
            elif typ in ('int','real'):
                lines += [ident+'Stats: ']
                stats = 'count min avg max sum'.split()
                lines += [ident*2 + '\t\t'.join(stats)]
                statsdef = ['{}({})'.format(stat, field) for stat in stats]
                statsvals = list(self.aggregate(statsdef, **kwargs))
                statstrings = ['{:.3f}'.format(val) for val in statsvals]
                lines += [ident*2 + '\t\t'.join(statstrings) ]
                
            descr = '\n'.join(lines)
            print(descr)
            
        else:
            print(self.__str__())

    def inspect(self, limit=10, offset=0, where=None):
        print(self.fieldnames)
        query = 'SELECT * FROM {}'.format(self.name)
        if where: query += ' WHERE {}'.format(where)
        query += ' LIMIT {} OFFSET {}'.format(limit, offset)
        for r in self._fetchall(query):
            print(r)

    #### Reading

    def get(self, fields=None, where=None):
        if fields:
            if isinstance(fields, basestring):
                fields = [fields]
            fieldstring = ','.join(fields)
        else:
            fields = ['*']
            fieldstring = '*'
            
        query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
        if where: query += u' WHERE {}'.format(where)
        query += ' LIMIT 2'
        
        result = self._fetchall(query)
        if len(result) > 1:
            warnings.warn('Get results is not unique, returning only the first result')

        if result:
            row = result[0]
            if len(row) == 1:
                return row[0]
            else:
                return row

    def values(self, fields, where=None, order=None, limit=None):
        '''Iterate all of the unique values.
        RENAME unique() ?
        '''
        # wrap single args in lists
        if isinstance(fields, basestring):
            fields = [fields]
        if order and isinstance(order, basestring):
            order = [order]

        # fields query
        fieldstring = ', '.join(fields)
        query = 'SELECT DISTINCT {} FROM {}'.format(fieldstring, self.name)
        
        # where query
        if where: query += u' WHERE {}'.format(where)

        # limit query
        if limit: query += ' LIMIT {}'.format(limit)

        # order query
        if order:
            ordstring = ', '.join(order)
            query += ' ORDER BY {}'.format(ordstring)

        # execute and return results
        cur = self._cursor()
        result = cur.execute(query)
        if len(fields) == 1:
            return (row[0] for row in result)
        else:
            return result

    def groupby(self, by, keep_fields=None, where=None, order=None):
        '''Iterate the groups and each of their members.
        RENAME split() ?
        '''
        # wrap single args in lists
        if isinstance(keep_fields, basestring):
            keep_fields = [keep_fields]
        if isinstance(by, basestring):
            by = [by]
        if order and isinstance(order, basestring):
            order = [order]

        # get unique values
        uniq = list(self.values(by, where=where, order=order))
        if len(by) == 1:
            uniq = ([v] for v in uniq)

        # loop unique values
        if not keep_fields: keep_fields = ['*']
        for u in uniq:
            fieldstring = ', '.join(keep_fields)
            query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
            # insert by-fields and unique values into a where query
            whereby = ' AND '.join(['{} = ?'.format(b) for b in by])
            query += ' WHERE {}'.format(whereby)
            if where: query += ' AND {}'.format(where)
            # execute and return unique value with group result
            output = self._query_to_table(query, u, output=False, replace=False)
            if len(by) == 1:
                u = u[0]
            group = self.workspace.table(output)
            yield u,group

    #### Writing

    def add_row(self, *row, **kw):
        if row:
            questionmarks = ','.join(('?' for _ in row))
            self.workspace.c.execute('INSERT INTO {} VALUES ({})'.format(self.name, questionmarks), row)
        elif kw:
            cols,vals = list(zip(*kw.items()))
            colstring = ','.join((col for col in cols))
            questionmarks = ','.join(('?' for _ in cols))
            self.workspace.c.execute('INSERT INTO {} ({}) VALUES ({})'.format(self.name, colstring, questionmarks), vals)

    def recode(self, field, *args, **kwargs):
        # Setting to multiple constant values depending on conditions
        if args:
            conds,vals = zip((arg.split('=') for arg in args))
            conds = [c.strip() for c in conds]
            vals = [v.strip() for v in vals]
        elif kwargs:
            conds,vals = zip(*conditions.items())
        whenstring = 'CASE'
        for cond in conds:
            whenstring += ' WHEN {} THEN ?'.format(cond)
        whenstring += ' END'
        
        query = 'UPDATE {}'.format(self.name)
        query += ' SET {} = ({})'.format(field, whenstring)
        
        self._fetchall(query, vals)

    def compute(self, field, value, where=None, dtype=None, verbose=False):
        '''Computing values for new or existing field
        '''
        cursor = self._cursor()
        if value is None: value = 'NULL'
        valstring = '{} = {}'.format(field,value)

        # create new field if type is specified
        if dtype:
            self.add_field(field, dtype)
        
        if verbose:
            # prepare loop incl progress tracking
            loop = self.values('oid', where=where)
            if where:
                total = self.get('COUNT(oid)', where=where)
            else:
                total = len(self)
            loop = track_progress(loop, 'Computing values', total=total)

            # loop and perform calculations
            cursor.execute('BEGIN')
            for oid in loop:
                query = 'UPDATE {} SET {} WHERE OID = {}'.format(self.name, valstring, oid)
                cursor.execute(query)
            cursor.execute('COMMIT')
            cursor.close()

        else:
            # calculate all in one go
            query = 'UPDATE {} SET {}'.format(self.name, valstring)
            if where: query += ' WHERE {}'.format(where)
            cursor.execute(query)
            cursor.close()

    #### Manipulations

    def select(self, fields=None, where=None, limit=None, output=False, replace=False):
        # parse which fields to keep
        if fields:
            if isinstance(fields, basestring):
                fields = [fields]
            fieldstring = ','.join(fields)
        else:
            fields = ['*']
            fieldstring = '*'

        # construct query
        query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
        if where: query += u' WHERE {}'.format(where)
        if limit: query += ' LIMIT {}'.format(limit)

        # execute and store in normal or temporary table
        output = self._query_to_table(query, output=output, replace=replace)

        # return the new table to user
        return Table(self.workspace, output, 'w')

    def join(self, other, conditions, keep_fields=None, keep_all=True, output=False, replace=False):
        # wrap single args in lists
        if isinstance(conditions, basestring):
            conditions = [conditions]
        if isinstance(keep_fields, basestring):
            keep_fields = [keep_fields]

        # by default keep all fields
        if not keep_fields:
            keep_fieldstring = '*'
        else:
            keep_fieldstring = ', '.join(keep_fields)

        # determine join type
        if keep_all:
            jointype = 'LEFT JOIN'
        else:
            jointype = 'INNER JOIN'

        # construct query
        conditionstring = ' AND '.join(conditions)
        query = 'SELECT {fields} FROM {left} AS left {jointype} {right} AS right ON {conditions}'.format(fields=keep_fieldstring,
                                                                                                           left=self.name,
                                                                                                           jointype=jointype,
                                                                                                           right=other.name,
                                                                                                           conditions=conditionstring)

        # maybe rename fieldnames somehow
        # ...
        
        # execute and store in normal or temporary table
        output = self._query_to_table(query, output=output, replace=replace)

        # return the new table to user
        return Table(self.workspace, output, 'w')

    def reshape(self, columns, fields):
        # https://stackoverflow.com/questions/2444708/sqlite-long-to-wide-formats
        # ...
        pass
        
    #### Stats

    def aggregate(self, stats, by=None, where=None, order=None, output=False, replace=False):
        '''Create a table of aggregates statistics.
        '''
        # wrap single args in lists
        if isinstance(stats, basestring):
            stats = [stats]
        if by and isinstance(by, basestring):
            by = [by]
        if order and isinstance(order, basestring):
            order = [order]
            
        # auto add groupby to stats
        if by:
            stats = by + stats
            
        # stats query
        statstring = ', '.join(stats)
        query = 'SELECT {} FROM {}'.format(statstring, self.name)
        
        # where query
        if where:
            query += u' WHERE {}'.format(where)
            
        # by query
        if by:
            bystring = ', '.join(by)
            query += ' GROUP BY {}'.format(bystring)
            
        # order query
        if order:
            ordstring = ', '.join(order)
            query += ' ORDER BY {}'.format(ordstring)

        # execute and store in normal or temporary table
        output = self._query_to_table(query, output=output, replace=replace)

        # return the new table to user
        return Table(self.workspace, output, 'w')

    #### Indexing

    # TODO: Maybe have indexes be objects...
    # ...

    @property
    def indexes(self):
        res = self._fetchall("SELECT * FROM SQLite_master WHERE type = 'index' AND tbl_name = '{}'".format(self.name))
        return list(res)

    @property
    def indexnames(self):
        return [r[1] for r in self.indexes] # return names of indexes

    def create_index(self, fields, name=None, replace=False, lower=False): #nocase=False):
        # wrap single args in lists
        if isinstance(fields, basestring):
            fields = [fields]
        # auto create index name if not specified
        if not name:
            fieldstring = '_'.join(fields)
            name = '{}_{}'.format(self.name, fieldstring)
        # drop if exists
        if replace and name in self.indexnames:
            self.drop_index(fields, name)
        # construct query and execute
        if lower: fields = ['lower({})'.format(f) for f in fields]
        fieldstring = ', '.join(fields)
        #if nocase: fieldstring += ' COLLATE NOCASE'
        query = 'CREATE INDEX {} ON {} ({})'.format(name, self.name, fieldstring)
        self._fetchall(query)

    def drop_index(self, fields, name=None):
        # wrap single args in lists
        if isinstance(fields, basestring):
            fields = [fields]
        # auto create index name if not specified
        if not name:
            fieldstring = '_'.join(fields)
            name = '{}_{}'.format(self.name, fieldstring)
        # construct query and execute
        query = 'DROP INDEX {}'.format(name)
        self._fetchall(query)
            
    #### Spatial indexing

    # TODO: Move all spatial indexing to separate class
    # incl moving the spatial_indexes memory dict to the new class
    # ...

    # TODO: Maybe custom implement rtrees as tables
    # using pure python pyrtree to easily populate
    # the node, parent, and rowid tables
    # see: https://stackoverflow.com/questions/25241406/best-option-for-supplying-quadtree-gps-data-to-an-app/25242162#25242162
    # https://github.com/pvanek/sqliteman/tree/master/Sqliteman/sqliteman/extensions/rtree
    # https://github.com/endlesssoftware/sqlite3/blob/master/rtree.c

    # TODO: Consider implementing quadtree tables as well
    # perhaps based on pyqtree and update to be more efficient
    # http://lspiroengine.com/?p=530
            
    def create_spatial_index(self, geofield, verbose=True):
        # create the spatial index
        import rtree
        temppath = TemporaryFile().name
        spindex = rtree.index.Index(temppath)
        spindex._temppath = temppath
        self.workspace.spatial_indexes[(self.name, geofield)] = spindex

        # NOTE: Maybe consider using generator as input to Index()
        # but this seems to only be in memory then, and not on file
        geoms = self.select(['oid',geofield])
##        i = 0
##        for g in geoms:
##            print g
##            i+=1
##        print i
##        fdsf

        if verbose:
            geoms = track_progress(geoms, 'Creating Spatial Index for Field "{}" on Table "{}"'.format(geofield, self.name), total=len(self))
            
        for oid,geom in geoms:
            if geom:
                bbox = geom.bounds if hasattr(geom, 'bounds') else geom.bbox
                # ensure min,min,max,max pattern
                xs = bbox[0],bbox[2]
                ys = bbox[1],bbox[3]
                bbox = [min(xs),min(ys),max(xs),max(ys)]
                # insert
                spindex.insert(oid, bbox)
                
        # add new entry to the spatial index table
        idxtable = self.workspace.table('spatial_indexes')
        idxtable.add_row(tbl=self.name, col=geofield)

    def load_spatial_index(self, geofield):
        if (self.name,geofield) in self.workspace.spatial_indexes:
            # already loaded in spatial_indexes dict
            spindex = self.workspace.spatial_indexes[(self.name,geofield)]
        else:
            # load idx and dat blob data
            idxtable = self.workspace.table('spatial_indexes')
            print 'retrieving idx and dat binary strings from db'
            cur = idxtable._cursor()
            (idx,dat) = cur.execute('''select rtree_idx,rtree_dat FROM {rtree} WHERE tbl = '{table}' AND col = '{geofield}' '''.format(rtree=idxtable.name, table=self.name, geofield=geofield)).fetchone()
            cur.close()
            # write to temp file
            temppath = TemporaryFile().name
            with open(temppath+'.idx', 'wb') as fobj:
                print 'placing .idx on disk'
                fobj.write(idx)
            with open(temppath+'.dat', 'wb') as fobj:
                print 'placing .dat on disk'
                fobj.write(dat)
            import rtree
            # load index from temp file
            print 'creating rtree from files'
            spindex = rtree.index.Index(temppath)
            print 'created'
            spindex._temppath = temppath
            # update spatial_indexes dict
            self.workspace.spatial_indexes[(self.name,geofield)] = spindex
        return spindex

    def store_spatial_index(self, geofield):
        spindex = self.workspace.spatial_indexes.pop((self.name,geofield))
        spindex.close()
        temppath = spindex._temppath
        # get the byte data from the files
        with open(temppath+'.idx', 'rb') as fobj:
            idx = Binary(fobj.read())
        with open(temppath+'.dat', 'rb') as fobj:
            dat = Binary(fobj.read())
        # delete the temporary files from disk
        os.remove(temppath+'.idx')
        os.remove(temppath+'.dat')
        # update the idx and dat columns in the spatial_indexes table
        idxtable = self.workspace.table('spatial_indexes')
        cur = idxtable._cursor()
        cur.execute('''UPDATE {rtree} SET rtree_idx=:idx, rtree_dat=:dat,
                     WHERE tbl = '{table}' AND col = '{geofield}' '''.format(rtree=idxtable.name, table=self.name, geofield=geofield),
                    dict(idx=idx, dat=dat))
        cur.close()

    def intersection(self, geofield, bbox, fields=None):
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
        fieldstring = ','.join(fields) if fields else '*'
        cur = self._cursor()
        return cur.execute('''SELECT {fields} FROM {table} WHERE oid IN ({oids})'''.format(fields=fieldstring,
                                                                                           table=self.name,
                                                                                           oids=idstring))

    #### Exporting
    
    def dump(self, filepath, **kwargs):
        # TODO: Need option to choose which geom or raster field (for now just assume pure table)
        fields = [(name,typ) for name,typ in self.fields if typ not in ('geom','rast')]
        data = self.select(fields=[name for name,typ in fields])
        vector.dump.to_file(filepath, fields, data, **kwargs)


