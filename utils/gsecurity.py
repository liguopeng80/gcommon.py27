#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-19

import uuid
import hmac
import hashlib
import binascii
import datetime
import OpenSSL
import base64


def md5(message):
    if not message:
        return ""

    return hashlib.md5(message.encode()).hexdigest()


def secure_uuid():
    return uuid.UUID(bytes=OpenSSL.rand.bytes(16))


def generate_l1key():
    return str(secure_uuid()).replace("-", "")[:16]


def generate_master_authentication_transport_key_groups(key_group_count=1):
    """
    生成指定组数的 master key(km), authentication key(ka), transport key(kt), 默认生成一组。
    :param key_group_count: key 的组数
    :return:  e.g. [{'km':'23edba278823ca9f'},'kt':'ca9f1f23ea278823','ka':'21fdba278823ca9f'}}, ...]

    """
    key_groups = []
    while key_group_count > 0:
        # master key, authentication key, transport key
        key_group = {'km': generate_l1key(), 'kt': generate_l1key(), 'ka': generate_l1key()}
        key_groups.append(key_group)
        key_group_count -= 1
    return key_groups


def auto_generate_password():
    return (str(secure_uuid())[:8]).lower()


def security_hash(str, key='1234567890'):
    key = bytearray(key.encode('ascii'))
    dig = hmac.new(key, msg=str, digestmod=hashlib.sha256).digest()
    return binascii.hexlify(dig)


def generate_salt():
    return base64.b64encode(OpenSSL.rand.bytes(6))


def pwd_hash_sha512(pwd, salt):
    return binascii.hexlify(hashlib.sha512(pwd + salt).digest())


if __name__ == "__main__":
    from gcommon.utils import gtime

    dt = datetime.datetime.now()
    print(dt)
    ts = gtime.date_to_timestamp(dt)
    print(ts)
    print("Done")
