#!/usr/bin/python
# -*- coding: utf-8 -*- 
# author: "Li Guo Peng" <liguopeng@liguopeng.net>

"""
Generic socket server. 
"""

import logging

from twisted.internet import reactor
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

logger = logging.getLogger('telnet')


class SocketServer(LineReceiver):
    """
    Protocol:
    
    <command> [<SP> parameter]* <\r\n>
    [binary payload | text payload]    
    """

    STATUS_UNKNOWN = 0
    STATUS_WAIT_FOR_USERNAME = 1
    STATUS_WAIT_FOR_PASSWORD = 2
    STATUS_AUTHENTICATED = 3

    MAX_LENGTH = 2000
    
    # Status
    Receive_Command = 0
    Receive_Raw_Payload = 1
    Receive_Line_Payload = 2

    Telnet_Prompt = '$'
    
    def __init__(self, factory):
        self.factory = factory
        self.state = self.Receive_Command
        self.command = None

        self.status = self.STATUS_UNKNOWN
        
    def connectionMade(self):
        self.transport.write("Login: ")
        self.status = self.STATUS_WAIT_FOR_USERNAME

    def connectionLost(self, reason):
        pass

    def writePrompt(self):
        self.transport.write(self.Telnet_Prompt + " ")

    def lineReceived(self, line):
        if self.status == self.STATUS_WAIT_FOR_USERNAME:
            username = line.strip()
            # skip username verification

            self.transport.write("Password: ")
            self.status = self.STATUS_WAIT_FOR_PASSWORD
            return

        elif self.status == self.STATUS_WAIT_FOR_PASSWORD:
            password = line.strip()
            # skip password verification
            self.status = self.STATUS_AUTHENTICATED
            self.sendLine("Welcome to Server!")
            self.writePrompt()
            return

        if self.state == self.Receive_Command:
            try:
                command = self.factory.parser.parse(line)
                
            except self.factory.parser.ParseError, e:
                code, msg = e.args
                self._send_result(code, msg)
                self.writePrompt()
            else:
                self.command = command
                
                if command.finished():
                    self._process_command()
                    self.writePrompt()
                    
                elif command.is_multiple_lines():
                    self.state = self.Receive_Line_Payload
                    
                elif command.is_binary():
                    self.state = self.Receive_Raw_Payload
                    self.setRawMode()
                    
                else:
                    """never go here"""
                    
        elif self.state == self.Receive_Line_Payload:
            self.command.line_received(line)
            
            if not self.command.remain_lines():
                # we have received a whole command
                self._process_command()
                self.writePrompt()
                
    def rawDataReceived(self, data):
        # self.state == self.Receive_Raw_Payload
        remains = self.command.remain_bytes()
        
        if remains > len(data):
            self.command.bytes_received(data)
        else:
            self.command.bytes_received(data[:remains])
            data = data[remains:]
            
            self.setLineMode(data)            
            
            self._process_command()
            self.writePrompt()
            
    def _process_command(self):
        command, self.command = self.command, None
        self.state = self.Receive_Command
        
        try:
            ret = command.process(self.factory.handler)

            if type(ret) is not tuple:
                self.sendLine(str(ret))
            elif len(ret) == 1:
                self._send_result(ret)
                
            elif len(ret) == 2:
                code, msg = ret
                self._send_result(code, msg)
                
            elif len(ret) == 3:
                code, msg, action = ret
                self._send_result(code, msg)
                
                if action == command.Action_Close:
                    self.transport.loseConnection()
            else:
                raise Exception('Wrong return value', ret)
            
        except Exception, e:
            self._send_result(500, str(e))
            
    def _send_result(self, code, extra_message='', message=''):
        responses = {
            100: 'Continue',
            101: 'Switching Protocols',
            
            200: 'OK',
            201: 'Created',
            202: 'Accepted',
            203: 'Non-Authoritative Information',
            204: 'No Content',
            205: 'Reset Content',
            206: 'Partial Content',
            
            300: 'Multiple Choices',
            301: 'Moved Permanently',
            302: 'Found',
            303: 'See Other',
            304: 'Not Modified',
            305: 'Use Proxy',
            306: '(Unused)',
            307: 'Temporary Redirect',
            
            400: 'Bad Request',
            401: 'Unauthorized',
            402: 'Payment Required',
            403: 'Forbidden',
            404: 'Not Found',
            405: 'Method Not Allowed',
            406: 'Not Acceptable',
            407: 'Proxy Authentication Required',
            408: 'Request Timeout',
            409: 'Conflict',
            410: 'Gone',
            411: 'Length Required',
            412: 'Precondition Failed',
            413: 'Request Entity Too Large',
            414: 'Request-URI Too Long',
            415: 'Unsupported Media Type',
            416: 'Requested Range Not Satisfiable',
            417: 'Expectation Failed',
            
            500: 'Internal Server Error',
            501: 'Not Implemented',
            502: 'Bad Gateway',
            503: 'Service Unavailable',
            504: 'Gateway Timeout',
            505: 'HTTP Version Not Supported',
            }
        
        if not message:
            message = responses.get(code, "")
            
        if extra_message:
            result = '%d %s. %s' % (code, message, extra_message)
        else:
            result = '%d %s.' % (code, message)
            
        self.sendLine(result)


class SocketServerFactory(Factory):
    def __init__(self, cmd_parser, cmd_handler):
        self.parser = cmd_parser
        self.handler = cmd_handler
        
    def buildProtocol(self, addr):
        return SocketServer(self)


def create(port, cmd_parser, cmd_handler):
    reactor.listenTCP(port, SocketServerFactory(cmd_parser, cmd_handler))  # @UndefinedVariable


def start_socket_server(port):
    from gcommon.net import telnet_base
    from gcommon.net import telnet_cmds
    create(port, telnet_base.CommandParser, None)

if __name__ == '__main__':
    pass
