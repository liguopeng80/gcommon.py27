#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2016-01-18

from zope.interface import implements

from twisted.python.components import proxyForInterface
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, succeed
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer, IResponse

from gcommon.utils.gjsonobj import JsonObject


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class HTTPResponse(proxyForInterface(IResponse)):
    def __init__(self, original):
        self.original = original
        self.raw_body = None

    def raw_response(self):
        return self._original

    @inlineCallbacks
    def raw_content(self):
        if not self.raw_body:
            self.raw_body = yield readBody(self.original)
        returnValue(self.raw_body)


class HTTPClient(object):
    def __init__(self, agent, body_producer=StringProducer):
        self._agent = agent
        self._body_producer = body_producer

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, data=None, **kwargs):
        return self.request('POST', url, data=data, **kwargs)

    def put(self, url, data=None, **kwargs):
        return self.request('PUT', url, data=data, **kwargs)

    def delete(self, url, data=None, **kwargs):
        return self.request('DELETE', url, data=data, **kwargs)

    def request(self, method, url, **kwargs):
        # TODO: merge url params from kwargs['params']
        # TODO: parse headers from kwargs['header']
        headers = Headers({})
        for k, v in kwargs.iteritems():
            if k != "data":
                headers.addRawHeader(str(k), str(v))

        # Get body producer for POST and PUT
        body_producer = None
        data = kwargs.get('data')
        if data:
            if isinstance(data, JsonObject):
                data = data.dumps()
            body_producer = self._body_producer(data)

        d = self._agent.request(method,
                                url,
                                headers=headers,
                                bodyProducer=body_producer) \
                       .addCallback(HTTPResponse)
        return d


@inlineCallbacks
def main():
    from gcommon.utils.gjsonobj import JsonObject
    client = HTTPClient(Agent(reactor))
    # resp = yield client.request('GET', 'http://localhost:8080/api/otap/hello')
    # body = yield resp.raw_content()
    # body = JsonObject().loads(body)
    # print resp.length
    # print body.dumps()

    test_dat = JsonObject()
    test_dat.a = 'FOO'
    test_dat.b = 'BAR'
    resp = yield client.request('POST', 'http://localhost:8080/api/otap/world', data=test_dat)
    body = yield resp.raw_content()
    body = JsonObject().loads(body)
    print resp.length
    print body.dumps()


def _get_expiration_timestamp():
    import time
    import datetime
    return int(time.mktime(datetime.datetime(2017, 1, 20, 10, 10, 10).timetuple()))


def _calculate_eap_pinhash(pincode, email, application_id, expiration):
    from gcommon.utils import security
    layer_one_pinhash = security.slim_security_hash("%s:%s" % (email, "nexus_layer_one.FdU4782j"), pincode)
    eap_pinhash = security.slim_security_hash("%s:%s:%s" % (application_id, "", expiration), layer_one_pinhash)

    return eap_pinhash


@inlineCallbacks
def test_add_pinhash():
    client = HTTPClient(Agent(reactor))

    req = JsonObject()
    req.email = 'gy@senlime.com'
    req.application_id = 'com.senlime.nexus.app.browser'
    req.app_servers = [227259]
    # req.expiration = _get_expiration_timestamp() * 1000

    pincode = "abcdefgh"
    # req.pinhash = _calculate_eap_pinhash(pincode, req.email, req.application_id, req.expiration)
    req.pinhash = _calculate_eap_pinhash(pincode, req.email, req.application_id, '')

    header = JsonObject()
    header["host-id"] = 227257
    header["otap-token"] = "d819225b9b20b82d8047539b01595e0cf7cb88a3887d5bbeaf05efcf730baa33"
    header["Content-Type"] = "application/json"
    resp = yield client.request('POST', 'http://localhost:8070/api/otap/pin', data=req, **header)
    body = yield resp.raw_content()
    body = JsonObject().loads(body)
    print body.dumps()

# Test
if __name__ == "__main__":
    reactor.callLater(0, test_add_pinhash)
    reactor.run()



