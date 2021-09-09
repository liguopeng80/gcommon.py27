#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: '2015-01-27'

from gcommon.data.app.team_cache import TeamCache
from gcommon.data.app.user_cache import UserCache
from gcommon.data.app.security_cache import SecurityCache


def init_data_layer(redis):
    UserCache.init_cache(redis.conn_user)

    TeamCache.init_cache(redis.conn_team, redis.conn_pub)

    SecurityCache.init_cache(redis.conn_security)



