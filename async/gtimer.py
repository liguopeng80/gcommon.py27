#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2014-12-02

"""Timer implemented by twisted."""

from twisted.internet import reactor
from twisted.internet.defer import Deferred, timeout, inlineCallbacks

import logging
logger = logging.getLogger('timer')


class AsyncTimer(object):
    @staticmethod
    def start(seconds):
        d = Deferred()
        reactor.callLater(seconds, d.callback, None)
        return d

    @staticmethod
    def wait(seconds):
        """ wait until interrupted by callback or timeout that will raise a CancelledError """
        d = Deferred()
        _delayed_call = reactor.callLater(seconds, timeout, d)
        d._slim_delayed_call = _delayed_call

        def _on_finished(result):
            logger.debug(">>> AsyncTimer _on_finished: %s, defer: %s", result, d)
            if _delayed_call.active():
                logger.debug("--- AsyncTimer.wait, job done! cancel timer.")
                _delayed_call.cancel()

            return result

        d.addBoth(_on_finished)

        return d

    @staticmethod
    def cancel_timer(d):
        d._slim_delayed_call.cancel()


class Timer(object):
    Not_Started = 0
    Started = 1
    Timed_Out = 2
    Cancelled = 3
    
    def __init__(self, seconds, timeout_handler, *args, **kwargs):
        self.status = self.Not_Started
        
        self.seconds = seconds

        self.timeout_handler = timeout_handler
        self._args = args
        self._kwargs = kwargs
        
        self._delayed_call = None
        
    def start(self):
        if self.status != self.Not_Started:
            return None
            
        self.status = self.Started
        self._delayed_call = reactor.callLater(self.seconds, self._on_timeout)  # @UndefinedVariable
        
        return self

    def restart(self, seconds = 0):
        if seconds:
            self.seconds = seconds
            
        if self.status == self.Started:
            self.cancel()
            
        self.status = self.Not_Started
        self.start()
        
        return self
        
    def cancel(self):
        if self.status != self.Started:
            return
            
        self.status = self.Cancelled
        self._delayed_call.cancel()
        
    def _on_timeout(self):
        if self.status != self.Started:
            return
            
        self.status = self.Timed_Out        
        self.timeout_handler(*self._args, **self._kwargs)


@inlineCallbacks
def test():
    from twisted.internet.defer import CancelledError
    max_count = 10
    
    # def foo():
    #     print 'haha', count
    #     if count > max_count:
    #         reactor.stop()  # @UndefinedVariable
    #         return
    #
    #     t = Timer(0.01, foo, count + 1).start()
    #
    # t = Timer(0.01, foo, 0).start()

    # @inlineCallbacks
    def __error_or_timeout_callback(failure):
        print "__error_or_timeout_callback %s" % failure

        # result = AsyncTimer.wait(1)
        # result.addErrback(__error_or_timeout_callback)
        # r = yield result
        # print r

        # return failure

    result = AsyncTimer.wait(5)
    result.addErrback(__error_or_timeout_callback)
    result.addCallback(__error_or_timeout_callback)
    # result.errback(Exception("adfa"))

    try:
        yield result
    except CancelledError:
        print "timeout"

    r = yield result
    print r


    print "Done"


# Test Codes
if __name__ == "__main__":
    test()
    reactor.run()  # @UndefinedVariable
    print 'Done'
