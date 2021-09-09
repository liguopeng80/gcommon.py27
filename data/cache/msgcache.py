from twisted.internet import reactor, protocol
from twisted.internet.defer import inlineCallbacks
from txredis.client import RedisClient

class MessageCacher(object):
    _MsgCache_Manager = None

    @classmethod
    def set_cache_manager(cls, cache_mgr):
        cls._MsgCache_Manager = cache_mgr
    """
    msg.id
    msg.sender
    msg.type
    msg.target
    msg.target_type
    msg.content
    msg.status
    msg.created
    """

class SlimMsgCacheManager(object):
    def __init__(self):
        self.cacher = None

    def create_msg_cacher(self, server, port, db = None):
        client_creator = protocol.ClientCreator(reactor, SlimMessageCacher, manager = self, db = db)
        d = client_creator.connectTCP(server, port)

        d.addCallback(self._set_redis_msg_cacher, self._connection_failed)

    def _set_redis_msg_cacher(self, cacher, _):
        self.cacher = cacher
        return cacher

    def _connection_failed(self, reason):
        # TODO: retry
        pass

class SlimMessageCacher(RedisClient):
    def __init__(self, manager, *args, **kws):
        super(SlimMessageCacher, self).__init__(*args, **kws)
        self.manager = manager

    @inlineCallbacks
    def set(self, key, value, preserve=False, getset=False, expire=None):
        super(SlimMessageCacher, self).set(key, value, preserve, getset, expire)

    @inlineCallbacks
    def get(self, key):
        super(SlimMessageCacher, self).get(key)

# Test Codes
if __name__ == "__main__":
    print 'Done'