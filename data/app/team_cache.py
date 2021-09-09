#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: '2015-02-04'

from gcommon.utils.jsonobj import JsonObject
from gcommon.data.cache.keyvalue import KeyValue
from gcommon.data.cache.set import Set


class Dict(dict):
    pass


class TeamCache(object):
    """
    1. team members
    2. team profile

    team members: { team_id : team_member_list}
    team profile: { team_id : team_profile}
    group members: { group_id : group_member_list }
    group profile: { group_id : group_profile}"""

    _cache_prefix = Dict()
    _cache_prefix.team_profile = 'team.p'
    _cache_prefix.user_profile = 'user.p'
    _cache_prefix.group_profile = 'group.p'
    _cache_prefix.team_public_groups = 'team.pg'
    _cache_prefix.team_private_groups = 'team.sg'
    _cache_prefix.user_groups = 'user.g'
    _cache_prefix.team_members = 'team.m'
    _cache_prefix.group_members = 'group.m'

    @classmethod
    def init_cache(cls, conn_team, conn_pub=None):
        cls._team_profile_cache = KeyValue(conn_team, cls._team_profile_encoder, cls._decoder)
        cls._user_profile_cache = KeyValue(conn_team, cls._user_profile_encoder, cls._decoder)
        cls._group_profile_cache = KeyValue(conn_team, cls._group_profile_encoder, cls._decoder)
        cls._team_group_cache = Set(conn_team, decoder=cls._set_decoder)
        cls._team_member_cache = Set(conn_team, decoder=cls._set_decoder)
        cls._group_member_cache = Set(conn_team, decoder=cls._set_decoder)
        cls._conn_pub = conn_pub

    def __init__(self, team_id):
        self._team_id = team_id

    @staticmethod
    def _user_profile_encoder(user_profile):
        return user_profile.dumps()

    @staticmethod
    def _team_profile_encoder(team_profile):
        return team_profile.dumps()

    @staticmethod
    def _group_profile_encoder(group_profile):
        return group_profile.dumps()

    @staticmethod
    def _decoder(profile_string):
        profile = JsonObject.loads(profile_string)

        return profile

    @staticmethod
    def _set_decoder(obj_id_string):
        return int(obj_id_string)

    def _get_key_name(self, prefix, obj_id):
        return '%s.%s' % (prefix, obj_id)

    def get_team_profile(self, team_id):
        cache_name = self._get_key_name(self._cache_prefix.team_profile, team_id)
        return self._team_profile_cache.get(cache_name)

    def get_user_profiles(self, *args):
        cache_names = []
        for id in args:
            cache_names.append(self._get_key_name(self._cache_prefix.user_profile, id))

        if cache_names:
            return self._user_profile_cache.mget(cache_names)
        else:
            return []

    def get_group_profiles(self, *group_ids):
        # get profiles of those group_ids
        keys = []
        for group_id in group_ids:
            key = self._get_key_name(self._cache_prefix.group_profile, group_id)
            keys.append(key)

        if keys:
            return self._group_profile_cache.mget(keys)
        else:
            return []

    def set_team_profile(self, team):
        key = self._get_key_name(self._cache_prefix.team_profile, team.team_id)
        self._team_profile_cache.set(key, team)

    def set_group_profiles(self, *groups):
        assert groups
        keys = []
        for group in groups:
            key = self._get_key_name(self._cache_prefix.group_profile, group.group_id)
            keys.append(key)

        profile_mapping = self._group_profile_cache.mset(keys, groups)

        return profile_mapping

    def set_user_profiles(self, *users):
        assert users
        keys = []
        for user in users:
            key = self._get_key_name(self._cache_prefix.user_profile, user.user_id)
            keys.append(key)

        self._user_profile_cache.mset(keys, users)

    def get_team_public_groups(self):
        key = self._get_key_name(self._cache_prefix.team_public_groups, self._team_id)
        return self._team_group_cache.get_all(key)

    def set_team_public_groups(self, *groups):
        key = self._get_key_name(self._cache_prefix.team_public_groups, self._team_id)
        return self._team_group_cache.add(key, *groups)

    def get_team_private_groups(self):
        key = self._get_key_name(self._cache_prefix.team_private_groups, self._team_id)
        return self._team_group_cache.get_all(key)

    def set_team_private_groups(self, *groups):
        key = self._get_key_name(self._cache_prefix.team_private_groups, self._team_id)
        return self._team_group_cache.add(key, *groups)

    def is_visible_group(self, user_id, group_id):
        key = self._get_key_name(self._cache_prefix.user_groups, user_id)
        visible = self._team_group_cache.ismember(key, group_id)
        if not visible:
            key = self._get_key_name(self._cache_prefix.team_public_groups, self._team_id)
            visible = self._team_group_cache.ismember(key, group_id)

        return visible

    def is_user_group(self, user_id, group_id):
        key = self._get_key_name(self._cache_prefix.user_groups, user_id)
        return self._team_group_cache.ismember(key, group_id)

    def get_user_groups(self, user_id):
        key = self._get_key_name(self._cache_prefix.user_groups, user_id)
        return self._team_group_cache.get_all(key)

    def set_user_groups(self, user_id, *groups):
        key = self._get_key_name(self._cache_prefix.user_groups, user_id)
        self._team_group_cache.add(key, *groups)

    def remove_user_groups(self, user_id, *groups):
        key = self._get_key_name(self._cache_prefix.user_groups, user_id)
        self._team_group_cache.remove(key, *groups)

    def is_team_member(self, user_id):
        key = self._get_key_name(self._cache_prefix.team_members, self._team_id)
        return self._team_member_cache.ismember(key, user_id)

    def get_team_members(self):
        key = self._get_key_name(self._cache_prefix.team_members, self._team_id)
        members = self._team_member_cache.get_all(key)
        # members = [int(member) for member in members]
        return members

    def get_group_members(self, group_id):
        key = self._get_key_name(self._cache_prefix.group_members, group_id)
        members = self._group_member_cache.get_all(key)
        return members

    def get_group_members_count(self, group_id):
        key = self._get_key_name(self._cache_prefix.group_members, group_id)
        return self._group_member_cache.count(key)

    def set_team_members(self, *members):
        key = self._get_key_name(self._cache_prefix.team_members, self._team_id)
        self._team_member_cache.add(key, *members)

    def set_group_members(self, group_id, *members):
        key = self._get_key_name(self._cache_prefix.group_members, group_id)
        self._group_member_cache.add(key, *members)

    def remove_group_members(self, group_id, *members):
        key = self._get_key_name(self._cache_prefix.group_members, group_id)
        self._group_member_cache.remove(key, *members)

    def publish_unread_status(self, sub_id, status):
        self._conn_pub.publish(sub_id, status)

    def get_user_push_token(self, user_id):
        # TODO:
        return "Not Implemented."

    def key_id_map(self, key_prefix):
        def __map(obj_id):
            return self._get_key_name(key_prefix, obj_id)

        return __map

    def empty_team_members(self):
        key = self._get_key_name(self._cache_prefix.team_members, self._team_id)
        self._team_member_cache.remove_keys(key)

    def empty_team_member_profiles(self, *user_ids):
        assert user_ids
        keys = map(self.key_id_map(self._cache_prefix.user_profile), user_ids)

        self._user_profile_cache.remove_keys(*keys)

    def empty_team_groups(self):
        # remove group profiles
        group_ids = self.get_team_public_groups()
        group_ids += self.get_team_private_groups()
        keys = map(self.key_id_map(self._cache_prefix.group_profile), group_ids)

        self._group_profile_cache.remove_keys(*keys)

        # remove team groups
        key_pub = self._get_key_name(self._cache_prefix.team_public_groups, self._team_id)
        key_pri = self._get_key_name(self._cache_prefix.team_private_groups, self._team_id)
        self._team_group_cache.remove_keys(key_pub, key_pri)

    def empty_group_members(self, group_ids):
        assert group_ids
        keys = map(self.key_id_map(self._cache_prefix.group_members), group_ids)

        self._group_member_cache.remove_keys(*keys)

    def empty_user_groups(self, user_ids):
        assert user_ids
        keys = map(self.key_id_map(self._cache_prefix.user_groups), user_ids)

        self._team_group_cache.remove_keys(*keys)


if __name__ == '__main__':
    from redis import Redis

    conn_team = Redis(host='localhost', port=6379, db=2)
    TeamCache.init_cache(conn_team)
    cache = TeamCache(1)
    redis_conn = Redis(host='localhost', port=6379, db=4)
    SharedPersonalMessageEventCache.init_cache(redis_conn)

    from gcommon.utils import tm
    from datetime import datetime

    print 'active_group_members', cache.get_active_group_members(10005)
    print 'active_team_members', cache.get_team_active_members()
    m = cache.get_group_members(10005)
    print m

    # for i in xrange(5):
    #     u = JsonObject()
    #     u.user_id = 2
    #     u.nickname = 'nick%d' % i
    #     u.email = u.nickname, "@email.com"
    #     u.avatar = ''
    #     u.first_name = 'firstNick%d' % i
    #     u.last_name = 'lastNick%d' % i
    #     u.job_role = 'engineer%d' % i
    #     u.phone_number = '10086%d' % i
    #     u.updated = tm.to_timestamp(datetime.now())
    #
    #     cache.set_team_members(u)
    #
    # cache.set_team_members(u)
    #
    # u = cache.get_team_members()
    # print u




