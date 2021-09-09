#!/usr/bin/python
# -*- coding: utf-8 -*-


def min_with_default(*args, **kwargs):
    default = kwargs.get('default', None)

    kwargs.pop('default', None)

    min_value = min(*args, **kwargs)

    if min_value is None and default:
        min_value = default

    return min_value


def correct_int_value(value, min_value, max_value):
    """修整客户端的输入。用户输入必须在系统允许的最大和最小值范围内。"""
    if not value:
        return min_value

    elif value < min_value:
        return min_value

    elif value > max_value:
        return max_value

    return value


# Test Codes
if __name__ == "__main__":
    print 'Done'
