#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 30 Dec 2014
# author: "Guo Peng Li" <liguopeng@liguopeng.net>

"""Parse server config file(s)."""

import os
import re
import platform


class ConfigParser(object):
    def __init__(self):
        self._options = {}
        self._default_group = ''

    def set_default_group(self, default):
        # Default group is the group whose name is current service (gatekeeper,
        # postman, etc), and can be replaced with 'd.'.        
        self._default_group = default.upper()

    def read(self, file_name, params=None):
        if not file_name:
            return

        file_name = os.path.join(os.getcwd(), file_name)
        file_name = os.path.abspath(file_name)

        # params is a dict object, which will be used to replace variables in
        # config file.
        #
        # For example, in config file:
        # cert_dir=/slim/$(CLOUD)/confs
        # 
        # $(CLOUD) is a variable. It will be replaced by params['CLOUD'].

        """Parse a config file."""
        self._options = self._read(file_name, params)

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

    def _read(self, file_name, params=None):
        """Read and parse a config file."""
        options = {}

        file_name = os.path.join(os.getcwd(), file_name)
        file_name = os.path.abspath(file_name)

        f = open(file_name)
        lines = f.readlines()
        f.close()

        current_option = 'DEFAULTS'
        for line in lines:
            line = line.strip()

            pos = line.find('#')
            if pos != -1:
                line = line[:pos]

            if not line:
                """blank line"""
            elif line[0] == '[':
                # [group], a netw option group
                pos = line.find(']')
                if pos != -1:
                    current_option = line[1:pos].strip().upper()

            elif line:
                # name=value
                name, value = line.split('=', 1)
                name = name.strip().upper()
                value = value.strip()

                # print name, value

                # from "$(CLOUD)" to "%(CLOUD)s"
                if params and value.find('$') != -1:
                    value = re.sub(r"\$\((\w+)\)", "%(\\1)s", value)
                    value = value % params
                    # print value

                # remove quotes
                if value[0] == '"' and value[-1] == '"':
                    value = value[1:-1]
                elif value[0] == "'" and value[-1] == "'":
                    value = value[1:-1]

                if current_option in options:
                    options[current_option][name] = value
                    options[name] = value
                else:
                    options[current_option] = {name: value}
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

    def _get(self, options, name):
        name = name.upper()

        if name.startswith("D.") and self._default_group:
            name = self._default_group + "." + name[2:]

        names = name.split('.')
        root = options

        for i in range(len(names) - 1):
            if root and names[i] in root:
                root = root[names[i]]

        # print names[-1], root
        if root and names[-1] in root:
            return root[names[-1]]

        return None


class DefaultConfigParser(ConfigParser):
    """支持缺省设置，支持模块嵌套的配置文件解析器。"""
    Defaults = {}

    def __init__(self, default_values=None):
        super(DefaultConfigParser, self).__init__()

        if default_values:
            self.Defaults = dict(default_values)
        else:
            self.Defaults = {}

    def read(self, file_name, params=None):
        if not file_name:
            return

        file_name = os.path.join(os.getcwd(), file_name)
        file_name = os.path.abspath(file_name)

        root = self._read(file_name, params)

        paths = self._get(root, 'defaults.default_search_path')

        if not paths:
            path_list = []
        elif platform.system() == 'Windows':
            path_list = paths.split(',')
        else:
            path_list = paths.split(':')

        # add current working directory
        path_list.append(os.getcwd())
        # add the path to current config file
        current_root, _ = os.path.split(file_name)
        path_list.append(current_root)

        # common config for all services
        modules = self._get(root, 'defaults.default_module_list')

        if modules:
            module_list = modules.split(',')
        else:
            module_list = []

        if 'SERVICE' in params:
            # also load the service's config file
            module_list.append(params['SERVICE'].upper())
            self.set_default_group(params['SERVICE'].upper())

        for module in module_list:
            # print module, root
            module = module.strip()

            module_file_name = self._get(root, module + '.config_filename')
            if module_file_name:
                module_option = self._load_module(path_list, module_file_name, params)
                root[module.upper()] = module_option

        self._options = root

    def _load_module(self, path_list, file_name, params):
        # print '_load_module'
        for path in path_list:
            # print path, file_name
            abs_name = os.path.join(path, file_name)
            try:
                options = self._read(abs_name, params)
            except IOError:
                continue
            else:
                return options

        return {}

    def _get(self, options, name):
        # try get the value from config file
        value = ConfigParser._get(self, options, name)

        if value is None:
            # The option is not in config file, try get a default value.
            name = name.lower()
            sections = name.split('.')
            if len(sections) > 2 and sections[0] in ('d', self._default_group.lower()):
                if sections[1] == 'defaults':
                    # d.defaults or service.defaults
                    name = '.'.join(sections[2:])
                    if name in self.Defaults:
                        return self.Defaults.get(name)
                    else:
                        return self.Defaults.get('defaults.' + name, None)
                else:
                    # d.xxx or service.xxx
                    name = '.'.join(sections[1:])
                    return self.Defaults.get(name, None)

            if value is None:
                # try the attribute's full name
                name = '.'.join(sections)
                return self.Defaults.get(name, None)

        return value


# Test Codes
if __name__ == "__main__":
    default_values = {
        'mysql.server': 'localhost',
        'mysql.poolsize': 10,
    }

    p = {'service': 'gatekeeper'}

    parser = DefaultConfigParser(default_values)

    print(platform.system())
    if platform.system() == 'Windows':
        filename = '../../../etc/gatekeeper.conf'
    else:
        # filename = '/home/guli/default.conf'
        filename = '../../../deploy/config/default.cfg'

    parser.read(filename, p)

    print(parser.get_str('huaxia_server'))
    # print(parser.get('mysql.server'))

    print('Done')


