
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

    @property
    def tables(self):
        names = [row[0] for row in self.c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        return tuple(names)

    def table(self, name):
        return Table(self, name)

    def new_table(self, name, fields, replace=False):
        if replace:
            self.c.execute('DROP TABLE IF EXISTS {}'.format(name))
        
        fieldstring = ', '.join(['{} {}'.format(fn,typ) for fn,typ in fields])
        self.c.execute('''CREATE TABLE {name} ({fieldstring})'''.format(name=name, fieldstring=fieldstring))
        self.db.commit()
        return Table(self, name)





    

