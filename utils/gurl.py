#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-10-22


def remove_trailing_slash(path):
    if path.endswith('/'):
        path = path[:-1]

    return path


def ensure_trailing_slash(path):
    if path.endswith("/"):
        return path

    return path + "/"
