#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-01-14

from __future__ import absolute_import

from gcommon.data.cache.script_utils import SlimScriptsManager
from .msgsub import ChannelSubscriber
from .msgsub import SlimSubscriberManager

from .msgcache import MessageCacher
from .msgcache import SlimMsgCacheManager

# txredis subscriber has no "select/auth" method
Redis_DB_Pub_Sub = 0

Redis_DB_Presence = 1
Redis_DB_Team_Cache = 2
Redis_DB_User_Cache = 3
Redis_DB_Message = 4
Redis_DB_Security = 6

# to read/write user presence information
# 
# user presence: {uid : count_of_active_devices}
conn_presence = None

# Message publish format definition
#
# { message_id : id,
#   message_type : type,
#   target_type : type,
#   target : target,
#   content : None if the message is a system event, otherwise user content
#   created : timestamp
# }

conn_message = None


# to read/write team info in cache, including
# 1. team members
# 2. team profile
#
# team members: { team_id : team_member_list}
# team profile: { team_id : team_profile}
# channel profile: {channel_id : channel_profile}
conn_team = None


# to read/write user profile in cache
#
# user profile: {user_id : user_profile}
conn_user = None


# message pub/sub
conn_pub = None


# Connection for security issues
conn_security = None


sub_manager = None
script_manager = None


def _create_redis_connection(host, port, db_index):
    from redis import Redis

    conn = Redis(host=host, port=port, db=db_index)
    return conn


def _create_redis_connection_from_config(cfg, prefix):

    if prefix in 'pubsub':
        params = {
            'host': cfg.get('redis.redis.server'),
            'port': cfg.get('redis.redis.port'),
            'db_index': Redis_DB_Pub_Sub
        }
    else:
        params = {
            'host': cfg.get('redis.redis_%s.server' % prefix) or cfg.get('redis.redis.server'),
            'port': cfg.get('redis.redis_%s.port' % prefix) or cfg.get('redis.redis.port'),
            'db_index': cfg.get('redis.redis_%s.db_index' % prefix),
        }

    assert params['host'] and params['port'] and params['db_index'] is not None

    return _create_redis_connection(**params)


def init(cfg, *args):
    """Init specified redis connections."""

    g = globals()
    for db_conn in args:
        # change global variable, like conn_message
        g_db_name = 'conn_%s' % db_conn
        if g.has_key(g_db_name):
            g[g_db_name] = _create_redis_connection_from_config(cfg, db_conn)


def init_subscriber(cfg):
    global sub_manager

    sub_manager = SlimSubscriberManager()

    server = cfg.get('redis.redis.server')
    port = cfg.get_int('redis.redis.port')

    assert server and port

    d = sub_manager.create_subscriber(server, port, Redis_DB_Pub_Sub)

    ChannelSubscriber.set_sub_manager(sub_manager)

    return d


def init_script_manger():
    global script_manager
    script_manager = SlimScriptsManager(conn_pub)

# Test Codes
if __name__ == "__main__":
    print 'Done'
