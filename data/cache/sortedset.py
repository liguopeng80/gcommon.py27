#!/usr/bin/python
# -*- coding: utf-8 -*-


import logging

logger = logging.getLogger('redisrts')


class SortedSet(object):
    """Item list is saved in a sorted sets."""
    MAX_SCORE = 0xffffffff    
    MAX_ITEMS_WITHIN_ONE_FETCH = 50

    def __init__(self, redis_conn, score_func=None, encoder=None, decoder=None,
                 max_score=0, max_items_within_one_fetch=0):
        self.conn = redis_conn

        self.score_func = score_func
        self.encoder = encoder
        self.decoder = decoder

        self.max_score = max_score or self.MAX_SCORE
        self.max_items_within_one_fetch = max_items_within_one_fetch or self.MAX_ITEMS_WITHIN_ONE_FETCH
                
    def append(self, key, item):
        score = self.score_func(item)

        if self.encoder:
            value = self.encoder(item)
        else:
            value = item

        self.conn.zadd(key, value, score)

        # return encoded string for convenience of redis publish
        logger.debug('[%06x] - SortedSet append, key: %s, item: %s, score: %s', 0, key, value, score)
        return value

    def append_with_score(self, key, item, item_score):
        if self.encoder:
            value = self.encoder(item)
        else:
            value = item

        logger.debug('[%06x] - SortedSet append_with_score, key: %s, item: %s, score: %s', 0, key, value, item_score)
        self.conn.zadd(key, value, item_score)

    def append_items(self, key, items):
        params = []
        for item in items:
            score = self.score_func(item)

            if self.encoder:
                value = self.encoder(item)
            else:
                value = item

            params.append(value)
            params.append(score)

        logger.debug('[%06x] - SortedSet append_items, key:%s, params:%s', 0, key, params)
        self.conn.zadd(key, *params)

    def append_items_with_multi_keys(self, keys, items):
        pipeline = self.conn.pipeline()

        # 每个key，对应一个item，只能为同一个key追加一个item
        assert len(keys) == len(items)

        encoded_values = []
        for i, key in enumerate(keys):
            item = items[i]
            score = self.score_func(item)

            if self.encoder:
                value = self.encoder(item)
            else:
                value = item

            pipeline.zadd(key, value, score)
            encoded_values.append(value)

        pipeline.execute()

        return encoded_values

    def _get_item_by_index(self, key, index):
        items = self.conn.zrange(key, index, index)
        assert(len(items) <= 1)

        logger.debug('[%06x] - SortedSet _get_item_by_index, key: %s, index: %s, items: %s', 0, key, index, items)

        if items:
            if self.decoder:
                return self.decoder(items[0])
            else:
                return items[0]

        return None

    def get_first_item(self, key):
        item = self._get_item_by_index(key, 0)

        logger.debug('[%06x] - SortedSet get_first_item, key: %s, item: %s', 0, key, item)
        return item

    def get_last_item(self, key):
        item = self._get_item_by_index(key, -1)
        logger.debug('[%06x] - SortedSet get_last_item, key: %s, item: %s', 0, key, item)
        return item

    def get_last_item_score(self, *keys):
        pipeline = self.conn.pipeline()
        for key in keys:
            pipeline.zrange(key, -1, -1, withscores=True)

        items = pipeline.execute()
        # item -> [(value, score), ...] for every key
        # item[0] -> (value, score)
        # item[0][1] -> score
        scores = [int(item[0][1]) if item else 0 for item in items]
        logger.debug('[%06x] - SortedSet get_last_item_score, keys: %s, scores: %s', 0, keys, scores)

        return scores

    def fetch(self, key, min_, max_, count=None, reverse=False):
        max_ = max_ or self.max_score
        count = count or self.max_items_within_one_fetch

        if reverse:
            items = self.conn.zrevrangebyscore(key, max_, min_, 0, count)
        else:
            items = self.conn.zrangebyscore(key, min_, max_, 0, count)

        logger.debug('[%06x] - SortedSet fetch, key: %s, min: %s, max: %s, num: %s, item: %s',
                     0, key, min_, max_, count, items)

        if self.decoder:
            for i in range(len(items)):
                items[i] = self.decoder(items[i])
            return items
        else:
            return items

    def remove_by_score(self, key, score):
        count = self.remove_by_scores(key, score, score)

        logger.debug('[%06x] - SortedSet remove_by_score, key: %s, score: %s, count: %s', 0, key, score, count)
        assert count == 1 or count == 0
        return count

    def remove_by_scores(self, key, min_score, max_score):
        count = self.conn.zremrangebyscore(key, min_score, max_score)
        logger.debug('[%06x] - SortedSet remove_by_score, key: %s, min: %s, max: %s, count: %s',
                     0, key, min_score, max_score, count)

        return count

    def count(self, *keys):
        pipeline = self.conn.pipeline()
        for key in keys:
            pipeline.zcard(key)

        counts = pipeline.execute()
        logger.debug('- Set count, keys: %s, counts: %s', keys, counts)

        if len(keys) == 1:
            return counts[0]
        else:
            return counts

    def key_exists(self, key):
        result = self.conn.exists(key)
        return result


def test():
    import redis
    r = redis.Redis(host='192.168.1.11', port=6379, db=0)
    m = SortedSet(r, hash)
    return m


if __name__ == '__main__':
    pass
