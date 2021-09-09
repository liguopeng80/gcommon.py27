# -*- coding: utf-8 -*- 
# created: 2021-07-30
# creator: liguopeng@liguopeng.net
from gcommon.utils.grand import RandomSequence


def test_rand_sequence():
    seq = RandomSequence()
    v = seq.next()
    print(v)
    assert v


test_rand_sequence()
