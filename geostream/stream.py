
import sqlite3
import os

from .table import Table

class Stream(object):
    def __init__(self, path):
        self.path = path
        self.db = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.c = self.db.cursor()

    def __del__(self):
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
        self.c.execute('''CREATE TABLE {name}
                        ({fieldstring})'''.format(name=name, fieldstring=fieldstring))
        self.db.commit()
        return Table(self, name)





    

