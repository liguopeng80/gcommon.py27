#!/usr/bin/python
# -*- coding: utf-8 -*- 
# author: Guo Peng Li


class CommandRegistry:

    Commands = {}

    @staticmethod
    def register(name, cmd_class):
        CommandRegistry.Commands[name] = cmd_class

    @staticmethod
    def get(name):
        return CommandRegistry.Commands.get(name, None)


class Command:
    Action_Close = 0
    
    def is_multiple_lines(self):
        return False
    
    def is_binary(self):
        return False
    
    def parse(self, command_line):
        pass
    
    def finished(self):
        return True
    
    def process(self, handler):
        pass


class MultipleLineCommand(Command):
    def __init__(self):

        self.lines = []
        self.total_lines = 0
        
    def is_multiple_lines(self):
        return True

    def line_received(self, line):
        self.lines.append(line)

    def remain_lines(self):
        return self.total_lines - len(self.lines)

    def finished(self):
        return len(self.lines) == self.total_lines


class BinaryCommand(Command):
    def __init__(self):
        self.payload = ""
        self.total_bytes = 0

    def is_binary(self):
        return True

    def finished(self):
        return len(self.payload) == self.total_bytes

    def remain_bytes(self):
        return self.total_bytes - len(self.payload)

    def bytes_received(self, bytes):
        self.payload = self.payload + bytes


class CommandParser:
    class ParseError(Exception): pass

    @staticmethod
    def parse(command_line):
        pos = command_line.find(' ')
        if pos != -1:
            command_name = command_line[:pos]
        else:
            command_name = command_line

        command_name = command_name.lower()        
        cmd_class = CommandRegistry.get(command_name)

        if cmd_class:
            cmd = cmd_class()

            try:
                cmd.parse(command_line)
            except:
                raise CommandParser.ParseError(400, 'Failed to parse the command')
            else:
                return cmd
        else:
            raise CommandParser.ParseError(404, 'Command is not supported.')


def register(names):
    import inspect
    
    for name in names:
        if inspect.isclass(name) and issubclass(name, Command):
            if hasattr(name, 'Command_Name'):
                CommandRegistry.register(getattr(name, 'Command_Name'), name)


# Test Codes
if __name__ == "__main__":
    print 'Done'
