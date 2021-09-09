#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import thread

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker


is_debug = False
logger = logging.getLogger('db')


def create_local_session_maker(db_conn_str):
    if is_debug:
        from sqlalchemy.pool import AssertionPool
        db_engine = create_engine(db_conn_str, echo=True)
    else:
        db_engine = create_engine(db_conn_str)

    _db_metadata = MetaData(db_engine)
    db_session = sessionmaker(bind=db_engine)
    return db_session


@contextmanager
def create_local_session(db_session_maker):
    sess = db_session_maker()
    logger.debug('[%x] - db session create - %s', thread.get_ident(), sess)
    try:
        yield sess
    except Exception, e:
        logger.error('[%x] - db session or app error - %s - exception: %s', thread.get_ident(), sess, e)
        sess.rollback()
        raise
    else:
        try:
            sess.commit()
            logger.debug('[%x] - db session commit - %s', thread.get_ident(), sess)
        except Exception, e:
            logger.error('[%x] - db session or app error - %s - exception: %s', thread.get_ident(), sess, e)
            sess.rollback()
            raise
    finally:
        logger.debug('[%x] - db session close - %s', thread.get_ident(), sess)
        sess.close()


def format_sqlite_connection_str(filename):
    return 'sqlite:///' + filename


def dump_sqlite_database(filename):
    import sqlite3

    db_conn = sqlite3.connect(filename)
    lines = []
    for line in db_conn.iterdump():
        lines.append(line)

    lines = [line.strip() for line in lines]
    statement = ''.join(lines)
    lines = statement.split(';')

    insert_statements = []
    for line in lines:
        if line.startswith("INSERT INTO"):
            insert_statements.append(line + ";")

    return insert_statements
