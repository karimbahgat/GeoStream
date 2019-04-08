
import sqlite3
import os
import shutil
import warnings
from itertools import izip, izip_longest

from . import vector
from . import raster

from .table import Table
from .verbose import track_progress

class Workspace(object):
    def __init__(self, path, mode='r'):
        self.path = path
        self.mode = mode
        self._connect()
        if self.mode == 'w':
            self._setup()

    def _connect(self):
        # connect to db
        self.db = sqlite3.connect(self.path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.db.cursor()

        # connect to spatial indexes, if any
        self.spatial_indexes = dict() # where they are stored when loaded in memory

    def _setup(self):
        # TODO: make metatables start with _ underscore
        # and don't show when listing tables
        metatables = self.metatablenames
        if not 'spatial_indexes' in metatables:
            # create spatial index tables
            fields = ['tbl', 'col', 'rtree_idx', 'rtree_dat']
            typs = ['text', 'text', 'blob', 'blob']
            self.new_table('spatial_indexes', list(zip(fields, typs)))

        # create crs/srs tables
        # ...

    # Builtins

    def __str__(self):
        ident = '  '
        lines = ['Streaming Workspace: ',
                 ident+'Location: "{}"'.format(self.path),
                 ident+'Spatial Indexes ({})'.format(len(self.table('spatial_indexes'))),
                 ident+'Datasets ({})'.format(len(self.tablenames)),]
        lines += [ident*2+'"{}" ({} fields, {} rows)'.format(tab.name,len(tab.fieldnames),len(tab))
                  for tab in self.tables()]
        descr = '\n'.join(lines)
        return descr

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    # Handling

    def close(self):
        # store back any unsaved spatial indexes
        for (name,field),idx in self.spatial_indexes.items():
            tbl = self.table(name)
            tbl.store_spatial_index(field)
        # close up the db
        self.db.commit()
        self.c.close()
        self.db.close()

    def clear(self, confirm=False):
        if confirm and self.mode == 'w':
            self.close()
            os.remove(self.path)
            self._connect()
            self._setup()
        else:
            raise Exception('To clear this database ({}) you must set confirm = True'.format(self.path))

    def delete(self, confirm=False):
        if confirm and self.mode == 'w':
            self.close()
            os.remove(self.path)
        else:
            raise Exception('To delete this database from disk ({}) you must set confirm = True'.format(self.path))

    def fork(self, path):
        self.db.commit()
        shutil.copyfile(self.path, path)
        return Workspace(path, 'w')

    # Metadata

    def describe(self, table=None):
        if table:
            self.table(table).describe()
        else:
            print(self.__str__())

    # Tables

    @property
    def tablenames(self):
        names = [row[0] for row in self.c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        metanames = self.metatablenames
        names = [n for n in names if n not in metanames]
        return tuple(names)

    @property
    def metatablenames(self):
        names = [row[0] for row in self.c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        metanames = ('spatial_indexes',)
        names = [n for n in names if n in metanames]
        return tuple(names)

    def table(self, name):
        return Table(self, name)

    def tables(self):
        for name in self.tablenames:
            yield self.table(name)

    # Data creation

    def new_table(self, name, fields, replace=False):
        # drop existing table if exists
        if replace:
            self.c.execute('DROP TABLE IF EXISTS {}'.format(name))

        # to save heartache later, auto replace problematic fieldname characters like underscore, period, etc.
        def clean(name):
            # ensure is text
            name = name if isinstance(name, basestring) else str(name)
            # replace problematic characters with underscore
            name = name.replace(' ', '_').replace('.', '_')
            # insert ensure name starts with a character
            if not name[0].isalpha():
                name = '_' + name
            # check for reserved names
            if name in ('Index',):
                name = '_' + name
            return name
        newfields = []
        for fn,typ in fields:
            cleaned = clean(fn)
            if cleaned != fn:
                warnings.warn('Field name "{}" was cleaned of problematic characters and its new name is now "{}". '.format(fn, cleaned))
            newfields.append((cleaned, typ))
        fields = newfields
        
        # create table
        fieldstring = ', '.join(['{} {}'.format(fn,typ) for fn,typ in fields])
        self.c.execute('''CREATE TABLE {name} ({fieldstring})'''.format(name=name, fieldstring=fieldstring))
        self.db.commit()
        return Table(self, name)

    def import_table(self, name, source, fieldnames=None, fieldtypes=None, select=None, keepfields=None, dropfields=None, sniffsize=10000, replace=False, verbose=True, **kwargs):
        # NOTE: use argname sniffsize for data detection, and sniffdialectsize for TextDelimited for detecting file structure
        # NOTE: kselect, eepfields and dropfields not yet implemented
        
        if isinstance(source, basestring):
            # load using format loaders
            reader = vector.load.from_file(source, **kwargs)
            source = reader
            if not fieldnames:
                fieldnames = reader.fieldnames
            if not fieldtypes:
                fieldtypes = reader.fieldtypes
            meta = reader.meta

        # wrap in a progress tracker
        if verbose:
            # by byte position in file
##            reader.fileobj.seek(0, 2)
##            end = reader.fileobj.tell()
##            def filecallback(a,b,c,d):
##                print reader.fileobj.tell()/float(end)
##            source = track_progress(source, 'Importing table "{}"'.format(name), callback=filecallback)
            # by row
            source = track_progress(source, 'Importing table "{}"'.format(name))

        # fields are known
        if fieldnames and fieldtypes:
            fields = list(zip(fieldnames, fieldtypes))
            
            # add geom field
            fields.append(('geom','geom'))
                
            # create the table
            table = self.new_table(name, fields, replace=replace)

            # add geom to each row
            source = (list(row)+[geo]
                      for row,geo in source)
            
            # add the source rows
            fails = 0
            for row in source:
                try: table.add_row(*row)
                except Exception as err:
                    warnings.warn('One or more rows could not be added due to a problem: {}'.format(err))
                    fails += 1

        # need to determine fields
        else:
            # make sure we have fieldnames at least
            if not fieldnames:
                raise Exception('Fieldnames must be given or detectable from a source file')

            # add geom to each row
            source = (list(row)+[geo]
                      for row,geo in source)
            
            # detect field types from the first few rows
            type_tests = [('int', lambda v: float(v).is_integer() ),
                          ('real', lambda v: float(v) ),
                          ('bool', lambda v: v in (True,False) ),
                          ('text', lambda v: v ),
                          ]
            
            # collect the sniff sample
            sniffsample = []
            for i,row in enumerate(source):
                sniffsample.append(row)
                if i >= sniffsize:
                    break
                
            # begin sniffing
            fieldtypes = []
            for colname,column in izip(fieldnames, izip_longest(*sniffsample)):
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
                fieldtypes.append(typ)

            # now we can define the fields
            fields = list(zip(fieldnames, fieldtypes))

            # add geom field
            fields.append(('geom','geom'))

            # create the table
            table = self.new_table(name, fields, replace=replace)

            # add the data from the sniffsample
            fails = 0
            for row in sniffsample:
                try: table.add_row(*row)
                except Exception as err:
                    warnings.warn('One or more rows could not be added due to a problem: {}'.format(err))
                    fails += 1
        
            # iterate and add what remains of the source
            for row in source:
                try: table.add_row(*row)
                except Exception as err:
                    warnings.warn('One or more rows could not be added due to a problem: {}'.format(err))
                    fails += 1

        if fails > 0:
            warnings.warn('A total of {} of rows could not be imported due to unknown problems'.format(fails))

        return table

    def import_raster(self, name, source,
                      tilesize=None, tiles=None,
                      replace=False, verbose=True, **kwargs):
        
        if isinstance(source, basestring):
            # load using format loaders
            rast = raster.data.Raster(source, **kwargs)

##            if nodataval is not None:
##                # override nodatavals (same for all bands)
##                for band in rast:
##                    band.nodataval = nodataval

            source = rast.tiled(tilesize, tiles)

        if verbose:
            # by byte position in file
##            reader.fileobj.seek(0, 2)
##            end = reader.fileobj.tell()
##            def filecallback(a,b,c,d):
##                print reader.fileobj.tell()/float(end)
##            source = track_progress(source, 'Importing table "{}"'.format(name), callback=filecallback)
            # by row
            source = track_progress(source, 'Importing raster "{}"'.format(name))

        # determine fields
        fields = []
        fields += [('rast', 'rast')]

        # create the table
        table = self.new_table(name, fields, replace=replace)
    
        # iterate and add what remains of the source
        fails = 0
        for tile in source:
            if 1:
                #print tile
                if isinstance(tile, raster.data.Raster):
                    # means raster was already loaded from file
                    rast = tile
                else:
                    # means user provided iterator of array lists (one list per raster, with one or more band arrays)
                    # all rasters will be given the same affine transform
                    width, height = tile[0].shape
                    rast = raster.data.Raster(None, width, height, affine)
                    for bandarr in tile:
                        dtype = bandarr.dtype
                        rast.add_band(bandarr, dtype, width, height, nodataval)
                        
                table.add_row(rast)
                
            if 0: #except Exception as err:
                warnings.warn('One or more tiles could not be added due to a problem: {}'.format(err))
                fails += 1

        if fails > 0:
            warnings.warn('A total of {} of tiles could not be imported due to unknown problems'.format(fails))

        return table
                
