
import os
from tempfile import TemporaryFile

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

        if not name in self.workspace.tablenames + self.workspace.metatablenames:
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
        return self.select()

    #### Hidden

    def _fetchall(self, query, vals=None):
        return self.workspace._fetchall(query, vals)

    def _cursor(self):
        return self.workspace._cursor()

    def _column_info(self):
        # cid,name,typ,notnull,default,pk
        return list(self._fetchall('PRAGMA table_info({})'.format(self.name)))

    #### Fields

    def add_field(self, field, typ):
        self._execute('ALTER TABLE {name} ADD {field} {typ}'.format(name=self.name, field=field, typ=typ))

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
            print descr
            
        else:
            print(self.__str__())

    #### Reading

    def get(self, fields=None, where=None):
        return next(self.select(fields, where, limit=1))

    def select(self, fields=None, where=None, limit=None, output=False, replace=False):
        if fields:
            if isinstance(fields, basestring):
                fields = [fields]
            fieldstring = ','.join(fields)
        else:
            fields = ['*']
            fieldstring = '*'
        query = 'SELECT {} FROM {}'.format(fieldstring, self.name)
        if where: query += u' WHERE {}'.format(where)
        if limit: query += ' LIMIT {}'.format(limit)
        #print query
        if output:
            # create new table
            if fields[0] == '*':
                fields = list(self.fields)
            else:
                fieldnames,fieldtypes = zip(*self.fields)
                fields = [(fn, fieldtypes[fieldnames.index(fn)]) for fn in fields]
                
            table = self.workspace.new_table(output, fields, replace=replace)
            
            cur = self._cursor()
            result = cur.execute(query)
            for row in result:
                table.add_row(*row)
            cur.close()
            return table

        else:
            cur = self._cursor()
            result = cur.execute(query)
            if len(fields) == 1 and fields[0] != '*':
                return (row[0] for row in result)
            else:
                return result

    #### Writing

    def add_row(self, *row, **kw):
        if row:
            questionmarks = ','.join(('?' for _ in row))
            self._fetchall('INSERT INTO {} VALUES ({})'.format(self.name, questionmarks), row)
        elif kw:
            cols,vals = list(zip(*kw.items()))
            colstring = ','.join((col for col in cols))
            questionmarks = ','.join(('?' for _ in cols))
            self._fetchall('INSERT INTO {} ({}) VALUES ({})'.format(self.name, colstring, questionmarks), vals)

    def set(self, **values):
        # Setting to constant values
        where = values.pop('where', None)
        
        cols,vals = zip(*values.items())
        valuestring = ', '.join(('{} = ?'.format(col) for col in cols))
        query = 'UPDATE {} SET {}'.format(self.name, valuestring)
        
        if where: query += u' WHERE {}'.format(where)
        
        self._fetchall(query, vals)

    def recode(self, field, **conditions):
        # Setting to multiple constant values depending on conditions
        conds,vals = zip(*conditions.items())
        whenstring = 'CASE'
        for cond in conds:
            whenstring += ' WHEN {} THEN ?'.format(cond)
        whenstring += ' END'
        
        query = 'UPDATE {}'.format(self.name)
        query += ' SET {} = ({})'.format(field, whenstring)
        
        self._fetchall(query, vals)

    def compute(self, **expressions):
        # Setting to expressions
        where = expressions.pop('where', None)
        verbose = expressions.pop('verbose', True)

        cursor = self._cursor()
        exprstring = ', '.join(('{} = {}'.format(col,expr) for col,expr in expressions.items()))

        # prepare loop incl progress tracking
        loop = self.select('oid', where=where)
        if verbose:
            if where:
                total = self.get('COUNT(oid)', where=where)
            else:
                total = len(self)
            loop = track_progress(loop, 'Computing values', total=total)

        # loop and perform calculations
        for oid in loop:
            query = 'UPDATE {} SET {} WHERE OID = {}'.format(self.name, exprstring, oid)
            cursor.execute(query)
        cursor.close()
        
        #valuestring = ', '.join(('{col} = (SELECT OID,{expr} FROM {table} AS calculated WHERE {table}.OID = calculated.OID)'.format(col=col, expr=expr, table=self.name) for col,expr in expressions.items()))
        #query = 'UPDATE {} SET {}'.format(self.name, valuestring)
        #query += ' WHERE OID IN (SELECT OID FROM {})'.format(self.name)
        # 'update natearth set calcarea = (SELECT ST_GEO_AREA(geom) FROM natearth AS calculated WHERE natearth.OID = calculated.OID)'        
        #if where: query += ' AND {}'.format(where)
        #print query
        #self.workspace.c.execute(query)

    #### Manipulations

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

        # return results
        if output:
            # determine fields
            leftfields = [('{}_{}'.format(self.name,f), typ) for f,typ in self.fields]
            rightfields = [('{}_{}'.format(other.name,f), typ) for f,typ in other.fields]
            fields = leftfields + rightfields
            if keep_fields:
                keep_fields = [f.replace('.', '_') for f in keep_fields]
                fields = [(f,typ) for f,typ in fields if f in keep_fields]

            # create new table
            table = self.workspace.new_table(output, fields, replace=replace)

            # populate with results
            cur = self._cursor()
            result = cur.execute(query)
            for row in result:
                table.add_row(*row)
            cur.close()
            return table
        else:
            # iterate through result
            cur = self._cursor()
            result = cur.execute(query)
            if keep_fields:
                result = (row[0] for row in result)
            return result

    def reshape(self, columns, fields):
        # https://stackoverflow.com/questions/2444708/sqlite-long-to-wide-formats
        # ...
        pass
        
    #### Stats

    def values(self, fields, where=None, order=None, limit=None):
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
        for u in uniq:
            # wrap unique text values with quotes
            # TODO: Change this so uses ? and inserts values the safe way
            # ...
            u = ["'{}'".format(v) if isinstance(v, basestring) else "{}".format(v)
                 for v in u]
            # insert by-fields and unique values into a where query
            byvals = zip(by, u)
            wherestring = ' AND '.join(['{} = {}'.format(b, v) for b,v in byvals])
            # execute and return unique value with group result
            group = self.select(keep_fields, where=wherestring)
            if len(by) == 1:
                u = u[0]
            yield u,group

    def aggregate(self, stats, by=None, where=None, order=None):
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

        # execute and return results
        cur = self._cursor()
        result = cur.execute(query)
        if len(stats) == 1:
            result = (row[0] for row in result)
        if not by:
            result = list(result)[0]
        return result

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
            (idx,dat) = idxtable.get(['rtree_idx','rtree_dat'], where="tbl = '{}' AND col = '{}' ".format(self.name, geofield))
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
        idxtable.set(rtree_idx=idx, rtree_dat=dat,
                     where="tbl = '{}' AND col = '{}' ".format(self.name, geofield))

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
        return self.select(fields=fields, where='oid IN ({})'.format(idstring))

    #### Exporting
    
    def dump(self, filepath, **kwargs):
        # TODO: Need option to choose which geom or raster field (for now just assume pure table)
        fields = [(name,typ) for name,typ in self.fields if typ not in ('geom','rast')]
        data = self.select(fields=[name for name,typ in fields])
        vector.dump.to_file(filepath, fields, data, **kwargs)


