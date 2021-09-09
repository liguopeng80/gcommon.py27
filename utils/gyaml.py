#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 30 Dec 2014
# author: "Guo Peng Li" <liguopeng@liguopeng.net>

"""Parse server config file(s)."""
import io
import os
import re
import platform

import yaml

from gcommon.utils.gjsonobj import JsonObject


class ConfigParser(object):
    project_root = ""

    _name_is_case_insensitive = True

    @classmethod
    def set_name_sensitive(cls):
        cls._name_is_case_insensitive = False

    def __init__(self, defaults=None):
        self._options = JsonObject()
        self._defaults = JsonObject(defaults or {})

    def get_str(self, name):
        """Get a string value from current config set."""
        ret = self._get(self._options, name)
        if ret:
            return str(ret)
        else:
            return ''

    def get_bool(self, name):
        value = self.get_str(name)
        value = value.lower()

        if value in ('t', 'true'):
            return True

        value = self.get_int(name)
        if value:
            return True

        return False

    def get_int(self, name):
        """Get an int value from current config set."""
        ret = self._get(self._options, name)
        try:
            return int(ret)
        except:
            return 0
        
    def get_float(self, name):
        """Get an int value from current config set."""
        ret = self._get(self._options, name)
        try:
            return float(ret)
        except:
            return 0

    def _parse_group(self, option_values, params=None):
        options = {}

        for name, value in option_values.items():
            name = name.strip()

            if self._name_is_case_insensitive:
                name = name.lower()

            if type(value) is dict:
                value = self._parse_group(value, params)
            elif type(value) is str:
                # from "$(CLOUD)" to "%(CLOUD)s"
                value = value.strip()
                if params and value.find('$') != -1:
                    value = re.sub(r"\$\((\w+)\)", "%(\\1)s", value)
                    value = value % params
            else:
                value = value

            options[name] = value

        return options

    def get(self, name, default=None):
        """Get a value from current config set."""

        # return value of the name.
        # The value could be a dict object
        value = self._get(self._options, name)
        return value or default

    def option(self, name):
        # return an option object
        option = self._get(self._options, name)

        cp = ConfigParser()
        cp._options = option

        return cp

    @classmethod
    def _get_option(cls, options, name):
        if cls._name_is_case_insensitive:
            name = name.lower()

        names = name.split('.')

        root = options

        for i in range(len(names) - 1):
            if root and names[i] in root:
                root = root[names[i]]

        # print names[-1], root
        if root and names[-1] in root:
            return root[names[-1]]

        return None

    def _get(self, options, name):
        # try get the value from config file
        value = self._get_option(options, name)

        if value is None:
            name = name.lower()
            if value is None:
                return self._defaults.get(name, None)

        return value


class YamlConfigParser(ConfigParser):
    args = []
    _config_root = ""

    def read(self, filename, params=None, encoding='utf-8'):
        if not filename:
            return

        filename = os.path.join(os.getcwd(), filename)
        filename = os.path.abspath(filename)
        self._config_root, _ = os.path.split(filename)

        # params is a dict object, which will be used to replace variables in
        # config file.
        #
        # For example, in config file:
        # cert_dir=/slim/$(CLOUD)/confs
        #
        # $(CLOUD) is a variable. It will be replaced by params['CLOUD'].

        """Parse a config file."""
        with io.open(filename, encoding=encoding) as f:
            file_options = yaml.safe_load(f) or {}

        options = self._parse_group(file_options, params)
        self._options = JsonObject(options)

    def load_module(self, module_name, filename, params=None, encoding='utf-8', defaults=None):
        config = YamlConfigParser(defaults=defaults)
        config.read(filename, params, encoding)

        self._options[module_name] = config._options
        self._defaults[module_name] = config._defaults

    def load_module_in_config_folder(self, module_name, filename="", params=None, encoding='utf-8', defaults=None):
        config = YamlConfigParser(defaults=defaults)

        filename = filename or (module_name + ".yaml")
        filename = os.path.join(self._config_root, filename)
        config.read(filename, params, encoding)

        self._options[module_name] = config._options
        self._defaults[module_name] = config._defaults


# Test Codes
if __name__ == "__main__":
    default_values = {
        'mysql.server': 'localhost',
        'mysql.poolsize': 10,
        'ucloud.test': 1,
    }

    p = {'service': 'gatekeeper'}

    print(platform.system())
    demo_filename = './test/demo_data.yaml'

    parser = YamlConfigParser(default_values)
    parser.read(demo_filename, p)

    print(parser.get_str("mysql.server"))
    print(parser.get("ucloud.region"))
    print(parser.get("ucloud.test"))
    assert parser.get("ucloud.test") == "test"

    print('Done')

