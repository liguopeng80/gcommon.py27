#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-04-15


def set_options_methods(request, post=False, get=False, put=False, delete=False, allowed_methods=None):
    methods = ['OPTIONS']
    if post:
        methods.append('POST')
    if get:
        methods.append('GET')
    if put:
        methods.append('PUT')
    if delete:
        methods.append('DEL')
    if allowed_methods:
        methods = list(set(methods).union(set(allowed_methods)))

    request.setHeader('Access-Control-Allow-Origin', '*')
    request.setHeader('Access-Control-Allow-Methods', ', '.join(methods))
    request.setHeader("Access-Control-Allow-Headers", "X-Requested-With, Content-Type, Authorization, Content-Length")