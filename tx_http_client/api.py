#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2016-01-18

from twisted.internet import reactor
from twisted.web.client import Agent
from client import HTTPClient


def get(url, **kwargs):
    return _client().get(str(url), **kwargs)


def post(url, data, **kwargs):
    return _client().post(str(url), data, **kwargs)


def put(url, data, **kwargs):
    return _client().put(str(url), data, **kwargs)


def delete(url, data, **kwargs):
    return _client().delete(str(url), data, **kwargs)


def _client(agent=None):
    if agent:
        _agent = agent
    else:
        _agent = Agent(reactor)
    return HTTPClient(_agent)
