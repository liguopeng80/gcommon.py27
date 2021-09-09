# -*- coding: utf-8 -*- 
# created: 2021-07-12
# creator: liguopeng@liguopeng.net


def split(list_obj, count):
    return list_obj[:count], list_obj[count:]


def to_string(x):
    return str(x) if type(x) != unicode else x.encode("utf-8")


def to_string_list(items):
    return [to_string(item) for item in items]
