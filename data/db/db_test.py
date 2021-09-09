#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-01-12

from gcommon.data import db

db_conn_param = {
    'user' : 'admin',
    'pass' : 'password',

    # 'server' : 'localhost',
    # 'server' : '192.168.118.128',
    'server' : '192.168.1.11',

    'port' : '3306',
    'db' : 'slim',
}


def init_db_engine(params):
    from sqlalchemy import create_engine
    from sqlalchemy import MetaData
    from sqlalchemy.orm import sessionmaker
    
    db_conn_template = 'mysql://%(user)s:%(pass)s@%(server)s/%(db)s?charset=utf8'
    
    db_conn_str = db_conn_template % params
    
    db.db_engine = create_engine(db_conn_str)
    db.db_metadata = MetaData(db.db_engine)
    db.db_session = sessionmaker(bind = db.db_engine)

# Test Codes
if __name__ == "__main__":
    init_db_engine(db_conn_param)

    sess = db.create_session()

    # models.GroupMessageEvent.__table__.create(bind = db.db_engine)
    # models.PersonalMessageEvent.__table__.create(bind = db.db_engine)

    print 'Done'
