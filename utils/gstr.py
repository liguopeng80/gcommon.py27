# -*- coding: utf-8 -*- 
# created: 2021-08-04
# creator: liguopeng@liguopeng.net
import re


def SnakeToCamel(name):
    """变量名风格转换"""
    return ''.join(word.title() for word in name.split('_'))


def snakeToCamel(name):
    """变量名风格转换，首字母小写"""
    words = name.split('_')
    return words[0] + ''.join(word.title() for word in words[1:])


_camel_pat_1 = re.compile('(.)([A-Z][a-z]+)')
_camel_pat_2 = re.compile('__([A-Z])')
_camel_pat_3 = re.compile('([a-z0-9])([A-Z])')


def camel_to_snake(name):
    """变量名风格转换"""
    name = _camel_pat_1.sub(r'\1_\2', name)
    name = _camel_pat_2.sub(r'_\1', name)
    name = _camel_pat_3.sub(r'\1_\2', name)
    return name.lower()
