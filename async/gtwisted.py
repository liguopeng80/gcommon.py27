#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# author: Guopeng Li
# created: 27 Aug 2008

from multiprocessing import Process

from functools import partial
import sys

from twisted.internet.defer import Deferred

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.protocols import basic


exit_code = 0


def exit_process(code=0):
    global exit_code
    exit_code = code
    reactor.stop()


class ProcessExitException(Exception):
    def __init__(self, ret_code, *args, **kwargs):
        Exception.__init__(*args, **kwargs)
        self.ret_code = ret_code


def start_process(func, *args, **kwargs):

    def _process_main():
        reactor.callWhenRunning(func, *args, **kwargs)
        reactor.run()

        sys.exit(exit_code)

    p = Process(target=_process_main)
    p.start()

    return p


class FileTailer(object):
    def __init__(self, filename, event_sink):
        self.filename = filename
        self.proc_tail = SubprocessProtocol(self)
        self._event_sink = event_sink

    def start(self):
        reactor.spawnProcess(self.proc_tail, 'tail', ['-c +10000', '-c10', '-F', self.filename])

    def stop(self):
        reactor.stop()

    def on_stdout_received(self, proc, line):
        self._event_sink.on_stdout_received(proc, line)

    def on_stderr_received(self, proc, line):
        self._event_sink.on_stderr_received(proc, line)

    def on_proc_existed(self, proc, status):
        self._event_sink.on_proc_existed(proc, status)


class SubprocessProtocol(protocol.ProcessProtocol):
    def __init__(self, manager):
        self.manager = manager

        self.outLiner = basic.LineReceiver()
        self.outLiner.lineReceived = partial(self.manager.on_stdout_received, self)
        self.outLiner.delimiter = '\n'

        self.errLiner = basic.LineReceiver()
        self.errLiner.lineReceived = partial(self.manager.on_stderr_received, self)
        self.errLiner.delimiter = '\n'

    def outReceived(self, data):
        self.outLiner.dataReceived(data)

    def errReceived(self, data):
        self.errLiner.dataReceived(data)

    def processExited(self, status):
        """ This is called when the child process has been reaped, and receives
        information about the process' exit status. The status is passed in the
        form of a Failure instance, created with a .value that either holds a
        ProcessDone object if the process terminated normally (it died of natural
        causes instead of receiving a signal, and if the exit code was 0), or a
        ProcessTerminated object (with an .exitCode attribute) if something went
        wrong.
        """
        self.manager.on_proc_existed(self, status)

    def processEnded(self, reason):
        """This is called when all the file descriptors associated with the child
        process have been closed and the process has been reaped.

        This means it is the last callback which will be made onto a ProcessProtocol.

        The status parameter has the same meaning as it does for processExited.
        """


class Observer(object):
    def __init__(self, name=""):
        self._d = None
        self.name = name

    def register(self):
        assert self._d is None
        self._d = Deferred()

    def wait(self):
        assert self._d
        return self._d

    def notify(self, msg=None):
        if self._d:
            self._d.callback(msg)
            self._d = None

    def __nonzero__(self):
        return self._d is not None

