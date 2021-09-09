#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2014-12-04

import logging
import thread

from contextlib import contextmanager

logger = logging.getLogger('db')

is_debug = False

db_engine = None
db_metadata = None
db_session = None


@contextmanager
def create_session():
    sess = db_session()
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


def session_wrapper(func, *args, **kwargs):
    with create_session() as sess:
        return func(sess, *args, **kwargs)


def init(db_conn_str, pool_size=15):
    from sqlalchemy import create_engine
    from sqlalchemy import MetaData
    from sqlalchemy.orm import sessionmaker
    
    global db_engine
    global db_metadata
    global db_session

    if is_debug:
        from sqlalchemy.pool import AssertionPool
        db_engine = create_engine(db_conn_str, poolclass=AssertionPool, pool_recycle=3600, echo=True)
    else:
        db_engine = create_engine(db_conn_str, pool_size=pool_size, pool_recycle=3600)

    db_metadata = MetaData(db_engine)
    db_session = sessionmaker(bind=db_engine)


# Test Codes
if __name__ == "__main__":
    print 'Done'
