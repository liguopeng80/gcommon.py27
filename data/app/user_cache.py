#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-08-31

from gcommon.app import const
from gcommon.utils.jsonobj import JsonObject
from gcommon.data.cache.keyvalue import KeyValue


class UserCache(object):
    """ Currently used only in Role based access control """
    def __init__(self):
        # stoken.<host_id>
        self._security_token_template = 'stoken.%s'
        # atoken.<host_id>.sys.<sys_name>.<sys_id>
        self._admin_token_template = 'atoken.%s.sys.%s.%s'

    @classmethod
    def init_cache(cls, conn_user):
        cls._auth_token_cache = KeyValue(conn_user, cls._encoder, cls._decoder)
        cls._admin_token_cache = KeyValue(conn_user, cls._encoder, cls._decoder)

    @staticmethod
    def _encoder(token_context):
        return token_context.dumps()

    @staticmethod
    def _decoder(value):
        token_context = JsonObject.loads(value)

        return token_context

    def _get_stoken_key_name(self, host_id):
        return self._security_token_template % host_id

    def _get_atoken_key_name(self, host_id, sub_sys_name, sys_id):
        return self._admin_token_template % (host_id, sub_sys_name, sys_id)

    def _get_atoken_id_pattern(self, host_id):
        return self._admin_token_template % (host_id, "*", "*")

    def add_auth_token(self, host_id, token_context):
        key = self._get_stoken_key_name(host_id)
        self._auth_token_cache.set(key, token_context, const.AUTH_TOKEN_EXPIRATION)

    def verify_auth_token(self, host_id, token_text):
        pass

    def verify_role_token(self, host_id, sub_sys_name, sub_sys_id, token, accepted_roles):
        cached_token = self.get_role_token(host_id, sub_sys_name, sub_sys_id)
        if not cached_token:
            # No matching token in cache
            return False
        elif cached_token.token != token:
            # Token value mismatched
            return False
        elif not set(accepted_roles).union(set(cached_token.roles)):
            # No demanded roles
            return False
        else:
            return True

    def get_role_token(self, host_id, sub_sys_name, sub_sys_id):
        key = self._get_atoken_key_name(host_id, sub_sys_name, sub_sys_id)
        return self._admin_token_cache.get(key)

    def update_or_add_role_token(self, host_id, sub_sys_name, token_context, sub_sys_id):
        key = self._get_atoken_key_name(host_id, sub_sys_name, sub_sys_id)

        # Cached roles shouldn't be dropped
        cached_token = self._admin_token_cache.get(key)
        if cached_token:
            valid_roles = list(set(cached_token.roles).union(set(token_context.roles)))
            token_context.roles = valid_roles

        self._admin_token_cache.set(key, token_context, const.ADMIN_TOKEN_EXPIRATION)

    def delete_role_token(self, host_id, sub_sys_name, sub_sys_id):
        key = self._get_atoken_key_name(host_id, sub_sys_name, sub_sys_id)

        self._admin_token_cache.remove_keys(key)


if __name__ == "__main__":
    from redis import Redis

    conn_team = Redis(host='192.168.1.158', port=6379, db=2)
    UserCache.init_cache(conn_team)
    cache = UserCache()
    cache.test("haha", "hehe")
