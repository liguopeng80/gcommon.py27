#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-02-10s

import logging

logger = logging.getLogger('rediskv')


class KeyValue(object):
    # Messages and Team Profiles are saved in Key-Value pairs

    def __init__(self, redis_conn, encoder = None, decoder = None):
        self.conn = redis_conn

        self.encoder = encoder
        self.decoder = decoder

    def set(self, key, item, expire = None):
        #logger.debug('- KeyValue set, key:%s, item:%s', key, item)
        if self.encoder:
            value = self.encoder(item)
        else:
            value = item

        self.conn.set(key, value, ex=expire)

    def get(self, key):
        item = self.conn.get(key)
        if item and self.decoder:
            item = self.decoder(item)

        #logger.debug('- KeyValue get, key:%s, item:%s', key, item)
        return item

    def mset(self, keys, items):
        #logger.debug('- KeyValue mset, key:%s, item:%s', keys, items)

        assert None not in items and len(keys) == len(items)
        if items and self.encoder:
            items = [self.encoder(item) for item in items]

        mapping = dict(zip(keys, items))
        self.conn.mset(mapping)

        return mapping

    def mget(self, *keys):
        items = self.conn.mget(*keys)

        if items and self.decoder:
            items = [item and self.decoder(item) for item in items]

        #logger.debug('- KeyValue mget, key:%s, item:%s', keys, items)
        return items

    def keys(self, pattern):
        cached_keys = self.conn.keys(pattern)
        return cached_keys

    def pttl(self, key):
        return self.conn.pttl(key)

    def pexpire(self, key, ttl):
        return self.conn.pexpire(key, ttl)

    def remove_keys(self, *keys):
        result = None
        if keys:
            result = self.conn.delete(*keys)

        logger.debug('- KeyValue remove_keys, keys: %s, result: %s', keys, result)
        return result


def test():
    import redis
    r = redis.Redis(host='192.168.1.11', port=6379, db=0)
    m = KeyValue(r)
    m.set('haha', 'hehe', 12345)
    import time
    time.sleep(1)
    print m.ttl('haha')
    time.sleep(2)
    print m.ttl('haha')
    time.sleep(3)
    print m.ttl('haha')

    # from slim.common.utils.jsonobj import JsonObject

    # def encoder(profile):
    #     return profile.dumps()

    # def decoder(profile_string):
    #     return JsonObject.loads(profile_string)

    # m = KeyValue(r, encoder, decoder)
    # profiles = []
    # keys = []
    # for i in xrange(3):
    #     profile = JsonObject()
    #     profile.group_id = i
    #     profile.name = "test_%d" % i
    #     profiles.append(profile)

    #     key = 'profile.test.%s' % profile.group_id
    #     keys.append(key)

    # m.mset(keys=keys, items=profiles)

    # w = KeyValue(r)
    # p = []
    # k = []

    # for i in xrange(2):
    #     k.append(i)

    # p.append('abc + %d' % i)
    # p.append(None)
    # w.mset(keys=k, items=p)
    # return w

if __name__ == '__main__':
    test()
    pass
