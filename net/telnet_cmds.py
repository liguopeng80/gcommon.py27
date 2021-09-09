#!/usr/bin/python
# -*- coding: utf-8 -*- 
# author: Guo Peng Li

import logging
from telnet_base import *
from gcommon.utils.counters import Counter
from gcommon.utils.counters import Timer

logger = logging.getLogger('telnet')


class CmdIdle(Command):
    Command_Name = 'idle'
    
    def process(self, handler):
        return 200, 'Keep Alived.'


class CmdExit(Command):
    Command_Name = 'exit'
    
    def process(self, handler):
        return 200, 'Bye.', Command.Action_Close


class CmdCapability(Command):
    Command_Name = 'cap'
    
    def process(self, handler):
        return 200, 'MaxLine = 2048'


class CmdGetCounter(Command):
    Command_Name = 'get_counter'

    def parse(self, command_line):
        args = command_line.split()
        self.counter_name = args[1:]

    def process(self, handler):
        if not self.counter_name:
            logger.debug('No counter names in your request.')
            return 0

        name = self.counter_name[0]
        counter = Counter.get(name).value
        return counter


class CmdGetGuage(Command):
    Command_Name = 'get_guage'

    def parse(self, command_line):
        args = command_line.split()
        self.guage_name = args[1:]

    def process(self, handler):
        if not self.guage_name:
            logger.debug('No guage name in your request.')
            return 0

        name  = self.guage_name[0]
        counter = Counter.get(name).value
        return counter


class CmdGetTime(Command):
    Command_Name = 'get_time'

    def parse(self, command_line):
        args = command_line.split()
        self.timer_name = args[1:]

    def process(self, handler):
        if not self.timer_name:
            logger.debug('No time name in your request.')
            return 0

        name = self.timer_name[0]
        time = Timer.get(name).clear()
        return time


class CmdGabageCollection(Command):
    Command_Name = 'gc'

    def parse(self, command_line):
        self.cmd_name, self.cmd_gc = command_line.split(None, 1)

    def process(self, handler):
        import gc

        result = ""
        expr = "result = %s" % self.cmd_gc
        exec(expr)

        result = str(result)
        if len(result) > 10240:
            result = result[:10240]

        return 200, result

register(globals().values())

# Test Codes
if __name__ == "__main__":
    print CommandRegistry.Commands
