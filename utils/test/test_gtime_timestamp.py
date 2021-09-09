# -*- coding: utf-8 -*- 
# created: 2021-08-26
# creator: liguopeng@liguopeng.net
from gcommon.utils import gtime


def test():
    values = (
        "2021-08-25 12:12:12",
        "2021-08-25 12:12:12Z",
        "2021-08-25T12:12:12Z",
        "2021-08-25T12:12:12+00:00",
        "2021-08-25T20:12:12+08:00",
    )

    for value in values:
        dt = gtime.Timestamp.parse(value)
        print(dt, dt.tzinfo)


if __name__ == '__main__':
    test()
