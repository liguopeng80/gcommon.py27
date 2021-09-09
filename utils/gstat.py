#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2011-12-08


def count_combinations(n, m):
    """从 n 个数中，取出 m 个数的组合数"""
    assert (n >= m)

    value = 1
    for i in range(m):
        value = value * (n - i)
        value = value / (i + 1)

    return int(value)


def count_full_combinations(n):
    """从 n 个数中，分别取出 0 - n 个数的全组合数 """
    value = 0

    for i in range(n + 1):
        value += count_combinations(n, i)

    return value
