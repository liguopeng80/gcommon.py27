#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-09-17

import os
import re

from exceptions import NotImplementedError

from gcommon.app import const
from gcommon.data.app import scripts
from gcommon.utils.jsonobj import JsonObject


class SlimScript(object):
    """ Object holding a redis lua script """
    def __init__(self, script_dir, script_filename, manager=None, script_body=None):
        self.name = script_filename
        self.signature = None
        self._manager = manager

        if script_body:
            self.body = script_body
        else:
            self.body = open(os.path.join(script_dir, script_filename)).read()


class SlimScriptsManager(object):
    def __init__(self, redis_conn, fresh_load=True):
        self._conn = redis_conn
        self.scripts = {}
        self.script_dir = os.path.dirname(scripts.__file__)

        if fresh_load:
            self.load_scripts()

    @staticmethod
    def upload_scripts_to_redis(manager, _, files):
        """ Read script file and construct script handler """
        files = [filename for filename in files if const.SCRIPT_POSTFIX in filename]
        for filename in files:
            script_body = manager.pre_process(filename)
            script = SlimScript(manager.script_dir, filename, manager, script_body=script_body)
            manager.scripts[script.name] = script

        for name, script in manager.scripts.iteritems():
            script.signature = manager._conn.script_load(script.body)

    def load_scripts(self):
        """ Load all script files """
        os.path.walk(self.script_dir, SlimScriptsManager.upload_scripts_to_redis, self)

    def flush_scripts(self):
        """ Redis SCRIPT FLUSH command """
        return self._conn.script_flush()

    def check_scripts(self, *script_names):
        """ Redis SCRIPT EXIST command """
        result = []
        for name in script_names:
            if name in self.scripts.keys():
                script = self.scripts[name]
                result.extend(self._conn.script_exists(script.signature))
            else:
                result.append(False)

        return result

    def pre_process(self, script_name):
        """ Process cross file dependencies """
        origin_file_path = os.path.join(self.script_dir, script_name)
        origin_file = open(origin_file_path, 'r').read()
        include_pattern = r'(?P<include_cmd>#include[ ]*\"(?P<filename>.*?)\")'
        included_files = re.findall(include_pattern, origin_file)

        for dependency in included_files:
            filename = dependency[1]
            dependency_file = open(os.path.join(self.script_dir, filename), 'r').read()
            origin_file = origin_file.replace(dependency[0], dependency_file)

        return origin_file

    def run(self, script_name, key_cnt=0, keys=None, args=None):
        """ Run script via script name """
        assert(reduce(lambda x, y: x and y, self.check_scripts(self.scripts[script_name].name)))
        if script_name in self.scripts.keys():
            script = self.scripts[script_name]
            arguments = []
            if keys:
                arguments.extend(keys)
            if args:
                arguments.extend(args)

            for idx, argument in enumerate(arguments):
                if isinstance(argument, JsonObject):
                    arguments[idx] = argument.dumps()

            return self._conn.evalsha(script.signature, key_cnt, *arguments)
        else:
            raise NotImplementedError


# Test Code
if __name__ == "__main__":
    from redis import Redis
    conn = Redis(host='192.168.1.11')
    test_manager = SlimScriptsManager(conn)

    result = test_manager.check_scripts(test_manager.scripts[const.SCRIPT_NAME_TEST_SCRIPT].name)
    #result.extend(test_manager.check_scripts(test_manager.scripts[const.SCRIPT_SET].name))
    #result.extend(test_manager.check_scripts(test_manager.scripts[const.SCRIPT_TEST].name))
    print result

    # test_manager.run(const.SCRIPT_SET, 0, keys=[], args=['12345', 'This is profile for 12345'])
    # test_manager.run(const.SCRIPT_SET, 1, keys=["test2"], args=[2])
    # add_result = test_manager.run(const.SCRIPT_TEST, 2, keys=["test1", "test2"])
    # print add_result

    test_json = JsonObject()
    test_json.a = 'haha'
    test_json.b = 'hehe'

    active = test_manager.run(const.SCRIPT_NAME_TEST_SCRIPT, 0, args=[1, test_json, "group", [2,8]])
    print active


