#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-12-10

import logging
from gcommon.app import const
from gcommon.data.cache.keyvalue import KeyValue


logger = logging.getLogger('SCACHE')


class SecurityCache(object):
    def __init__(self):
        self._ip_counter_key = 'counter.ip.%s'
        self._black_list_key = 'banned.ip.%s'
        self._counter_initial = 1

    @classmethod
    def init_cache(cls, connection):
        cls.ip_counters = KeyValue(connection)
        cls.blacklist = KeyValue(connection)

    def get_counter_key(self, ip):
        return self._ip_counter_key % ip

    def get_blacklist_key(self, ip):
        return self._black_list_key % ip

    def ban_ip(self, ip, ttl=const.BAN_IP_TTL):
        logger.warning('ip %s banned for %s seconds', ip, ttl)
        self.blacklist.set(self._black_list_key % ip, True, ttl)

    def is_ip_banned(self, ip):
        return self.blacklist.keys(self._black_list_key % ip)

    def add_ip_counter(self, ip, ttl=const.IP_COUNTER_TTL):
        counter_key = self.get_counter_key(ip)
        counter_value = self.ip_counters.get(counter_key)

        if counter_value:
            counter_value = int(counter_value)
            if counter_value >= const.HTTP_REQUEST_LIMIT_IN_COUNTER_TTL:
                # IP exceeded request limit, ban it
                self.ban_ip(ip)
                return False
            else:
                pttl = self.ip_counters.pttl(counter_key)
                counter_value += 1
                self.ip_counters.set(counter_key, counter_value)
                self.ip_counters.pexpire(counter_key, pttl)
                return True
        else:
            # New ip
            self.ip_counters.set(counter_key, self._counter_initial, ttl)
            return True



