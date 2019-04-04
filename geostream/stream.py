
import sqlite3
import os

from .table import Table

class Stream(object):
    def __init__(self, path, mode='r'):
        self.path = path
        self.mode = mode
        self.db = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.db.cursor()

        self.spatial_indexes = dict() # where they are stored when loaded in memory

        if mode == 'w':
            # create spatial index tables
            fields = ['tbl', 'col', 'rtree_idx', 'rtree_dat']
            typs = ['text', 'text', 'blob', 'blob']
            self.new_table('spatial_indexes', list(zip(fields, typs)), replace=True)

            # create crs/srs tables
            # ...

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        # store back any unsaved spatial indexes
        for (name,field),idx in self.spatial_indexes.items():
            tbl = self.table(name)
            tbl.store_spatial_index(field)
        # close up the db
        self.db.commit()
        self.c.close()

    def  describe(self):
        ident = '  '
        lines = ['Streaming Environment: ',
                 ident+'Spatial Indexes ({})'.format(len(self.table('spatial_indexes'))),
                 ident+'Datasets ({})'.format(len(self.tablenames)),]
        lines += [ident*2+'"{}" ({} fields, {} rows)'.format(tab.name,len(tab.fieldnames),len(tab))
                  for tab in self.tables()]
        print('\n'.join(lines))

    @property
    def tablenames(self):
        names = [row[0] for row in self.c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return tuple(names)

    def table(self, name):
        return Table(self, name)

    def tables(self):
        for name in self.tablenames:
            yield self.table(name)

    def new_table(self, name, fields, replace=False):
        if replace:
            self.c.execute('DROP TABLE IF EXISTS {}'.format(name))
        
        fieldstring = ', '.join(['{} {}'.format(fn,typ) for fn,typ in fields])
        self.c.execute('''CREATE TABLE {name} ({fieldstring})'''.format(name=name, fieldstring=fieldstring))
        self.db.commit()
        return Table(self, name)

    def import_table(self, name, source, fieldnames=None, fieldtypes=None, sniffsize=10000, replace=False):

        if isinstance(source, str):
            # load using format loaders
            # this should give use
            # - source iterator
            # - fieldnames and/or types
            # ...
            raise NotImplementedError('Loading from external files not yet supported, but planned')

        # determine fields
        if fieldtypes:
            # create the table
            table = self.new_table(name, fields, replace=replace)
        else:
            # make sure we have fieldnames at least
            if not fieldnames:
                raise Exception('Fieldnames must be given or detectable from a source file')
            
            # sniff the first fields to detect field types
            type_tests = [('int', lambda v: float(v).is_integer() ),
                          ('real', lambda v: float(v) ),
                          ('bool', lambda v: v in (True,False) ),
                          ('text', lambda v: v ),
                          ]
            sniffsample = []
            for i,row in enumerate(source):
                sniffsample.append(row)
                if i >= sniffsize:
                    break
            fieldtypes = []
            for colname,column in zip(fieldnames, zip(*sniffsample)):
                valid = (v for v in column if v is not None)
                typegen = iter(type_tests)
                typ,typtest = next(typegen)
                v = next(valid, None)
                while v:
                    try:
                        # check that value can be converted to datatype
                        if typtest(v):
                            v = next(valid, None)
                        else:
                            # did not pass test
                            typ,typtest = next(typegen)                            
                    except:
                        # if fails, must downgrade to next more flexible datatype
                        # and check again
                        typ,typtest = next(typegen)
                # sniffsample for that column complete
                #print colname,typ,v
                fieldtypes.append(typ)

            # now we can define the fields
            fields = zip(fieldnames, fieldtypes)

            # create the table
            table = self.new_table(name, fields, replace=replace)

            # and finally add the data from the sniffsample
            for row in sniffsample:
                table.add_row(*row)
        
        # iterate and add what remains of the source
        for row in source:
            table.add_row(*row)

        return table



    

