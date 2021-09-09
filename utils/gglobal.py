# -*- coding: utf-8 -*- 
# created: 2021-06-28
# creator: liguopeng@liguopeng.net

from gcommon.utils.gyaml import YamlConfigParser


class Global(object):
    config = YamlConfigParser()

    @classmethod
    def set_config(cls, config):
        cls.config = config


