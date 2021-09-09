#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2014-11-26

from sqlalchemy import create_engine


def dump_db_schema(metadata):
    def dump(sql, *multiparams, **params):
        print sql.compile(dialect = mock_engine.dialect)
    
    mock_engine = create_engine('mysql://', strategy = 'mock', executor = dump)
    metadata.create_all(mock_engine, checkfirst = False)


def create_database(metadata, engine):
    """
    for exmaple:
    engine = create_engine('mysql://root:password@localhost/dbname',
                           connect_args = {'charset' : 'utf8'}))

    engine = create_engine('mysql://root:password@localhost/dbname?charset=utf8')
    """
    metadata.create_all(bind=engine)
    
# Test Codes
if __name__ == "__main__":
    print 'Done'
