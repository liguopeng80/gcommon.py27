#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

logger = logging.getLogger('rediset')


class Set(object):
    # Presence info are saved in Key-Value pairs

    def __init__(self, redis_conn, encoder = None, decoder = None):
        self.conn = redis_conn

        self.encoder = encoder
        self.decoder = decoder

    def add(self, key, *items):
        if not items:
            logger.debug('[] - Set add, key: %s, item is empty', key)
            return

        values = []
        for item in items:
            if self.encoder:
                values.append(self.encoder(item))
            else:
                values.append(item)

        logger.debug('- Set add, key: %s, item: %s', key, values)
        self.conn.sadd(key, *values)

    def ismember(self, key, item):
        if self.encoder:
            value = self.encoder(item)
        else:
            value = item

        returnValue = self.conn.sismember(key, value)
        logger.debug('- Set ismember, key: %s, item: %s',  key, returnValue)
        return returnValue

    def remove(self, key, *items):
        values = []
        for item in items:
            if self.encoder:
                values.append(self.encoder(item))
            else:
                values.append(item)

        logger.debug('- Set remove, key: %s, item: %s', key, values)
        return self.conn.srem(key, *values)

    def get_all(self, key):
        items = self.conn.smembers(key)
        if items:
            if self.decoder:
                result = list(self.decoder(x) for x in items)
            else:
                result = list(items)
        else:
            result = list(items)
        logger.debug('- Set get_all, key: %s, item: %s', key, result)
        return result

    def count(self, *keys):
        pipeline = self.conn.pipeline()
        for key in keys:
            pipeline.scard(key)

        counts = pipeline.execute()
        logger.debug('- Set count, keys: %s, counts: %s', keys, counts)
        return counts

    def remove_keys(self, *keys):
        result = None
        if keys:
            result = self.conn.delete(*keys)

        logger.debug('- Set remove_keys, keys: %s, result: %s', keys, result)
        return result


def test():
    import redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    m = Set(r, hash)
    return m

if __name__ == '__main__':
    pass
