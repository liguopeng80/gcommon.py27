#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-01-14

"""Message subscriber"""

import logging

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from txredis.client import RedisSubscriber, RedisSubscriberFactory


logger = logging.getLogger('pubsub')


class BadSubscriptionStatus(Exception): pass


class ChannelSubscriber(object):
    """Redis Channel Subscriber"""
    _Sub_Manager = None
    
    @classmethod
    def set_sub_manager(cls, sub_manager):
        cls._Sub_Manager = sub_manager
    
    def __init__(self):
        """chid -> channel ID"""
        self._is_alive = False
        self._subscription_deferred = None
        self._channel_ids = set()

    def is_alive(self):
        return self._is_alive

    def subscribe(self, channel_id):
        self._is_alive = True
        self._channel_ids.add(channel_id)

        subscribed = self._Sub_Manager.subscribe(self, channel_id)
        if subscribed:
            self.on_sub_registered(channel_id)
        elif self._subscription_deferred:
            raise BadSubscriptionStatus("A pending subscription is not finished yet.")
        else:
            self._subscription_deferred = Deferred()
            return self._subscription_deferred

    def unsubscribe(self, channel_id):
        if len(self._channel_ids) == 1:
            self._is_alive = False
        if self._subscription_deferred:
            d = self._subscription_deferred
            self._subscription_deferred = None
            d.callback(None)

        # for channel_id in self._channel_ids:
        self._Sub_Manager.unsubscribe(self, channel_id)

        logger.debug('unsubscribed channel \'%s\'', channel_id)

        if channel_id in self._channel_ids:
            self._channel_ids.remove(channel_id)

        # self._channel_ids = set()

    def on_sub_notification(self, channel_id, message):
        """Received a new message from the target channel."""
        raise NotImplemented('for sub-class')    
        
    def on_sub_registered(self, channel_id):
        """Subscription on one channel has been resumed."""
        if self._subscription_deferred:
            d = self._subscription_deferred
            self._subscription_deferred = None
            reactor.callLater(0, d.callback, None)

        if not self._is_alive:
            reactor.callLater(0, self.unsubscribe, channel_id)

    def on_sub_disconnected(self):
        """The server cannot be reached."""
        self._is_alive = False
        raise NotImplemented('for sub-class')


class SlimSubscriberManager(object):
    """Connection resume and clients management."""
    def __init__(self):        
        # {channel_id : [channels]} 
        self.clients = {}
        self.subscriber = None
        
        self._subscribed_channels = set()

    def on_connected(self):
        """Connection resumed. Try re-subscribe all channels."""
        logger.info('connection to redis resumed')
        for chid in self.clients.iterkeys():
            self.subscriber.subscribe(chid)

    def on_disconnected(self):
        """Connection to redis server has been closed somehow, so we will not
        be able to send push notification to clients."""
        logger.critical('connection to REDIS lost!!!!')
        self._subscribed_channels = set()
        for _channel_id, clients in self.clients.iteritems():
            for client in clients:
                client.on_sub_disconnected()
        
    def on_message(self, channel_id, message):
        """Broad the incoming message to all clients which have subscription
        on the channel."""
        clients = self.clients.get(channel_id, None)
        if clients is None:
            return
        
        bad_clients = []
        for client in clients:
            if client.is_alive():
                client.on_sub_notification(channel_id, message)
            else:
                bad_clients.append(client)
                
        for client in bad_clients:
            clients.remove(client)

        if not clients:
            del self.clients[channel_id]
            self.subscriber.unsubscribe(channel_id)

    def subscribe(self, client, channel_id):
        """Subscribe to a chid.
        
        If this is the first client of the chid, current subscriber must
        send "subscribe" request to redis server.

        return: True if the channel has been subscribed.
        """
        need_subscribe = False
        channel_id = str(channel_id)
        
        clients = self.clients.get(channel_id, None)
        if not clients:
            clients = set()
            self.clients[channel_id] = clients
            
            need_subscribe = True
            
        clients.add(client)
        
        if need_subscribe:
            # this function return None
            self.subscriber.subscribe(channel_id)
            return False

        elif channel_id in self._subscribed_channels:
            # the channel has been subscribed
            return True

    def unsubscribe(self, client, channel_id):
        """Unsubscribe to a channel ID.
        
        If this is the last client of the chid, current subscriber must
        send "unsubscribe" request to redis server.
        """
        clients = self.clients.get(channel_id, None)
        if not clients:
            return

        if client in clients:
            clients.remove(client)

        if not clients:
            # no client subscribed on this channel...
            del self.clients[channel_id]

            if channel_id in self._subscribed_channels:
                # the channel maybe is under subscribing - ignore it
                self.subscriber.unsubscribe(channel_id)

    def channel_subscribed(self, channel_id, num):
        self._subscribed_channels.add(channel_id)
        
        clients = self.clients.get(channel_id, None)
        
        if not clients:
            self.subscriber.unsubscribe(channel_id)
            if clients is not None:   
                del self.clients[channel_id]
        else:
            for client in clients:
                client.on_sub_registered(channel_id)
            
    def channel_unsubscribed(self, channel_id, _num):
        self._subscribed_channels.remove(channel_id)
        
        # clients = self.clients.get(channel_id, None)
        # if clients:
        #     for client in clients:
        #        client.on_sub_disconnected()
        
    def create_subscriber(self, server, port, db=None):
        factory = SlimRedisSubscriberFactory(manager=self, db=db)
        reactor.connectTCP(server, port, factory)
        d = factory.deferred
        d.addCallbacks(self._set_redis_subscriber, self._connection_failed)

        return d
        
    def _set_redis_subscriber(self, subscriber):
        self.subscriber = subscriber
        return subscriber
    
    def _connection_failed(self, reason):
        # TODO: retry
        return reason


class SlimRedisSubscriber(RedisSubscriber):
    """A connection for subscription."""
    def __init__(self, manager, *args, **kws):
        super(SlimRedisSubscriber, self).__init__(*args, **kws)
        self.manager = manager

    def connectionMade(self):
        RedisSubscriber.connectionMade(self)
        self.manager._set_redis_subscriber(self)
        self.manager.on_connected()

    def connectionLost(self, reason):
        RedisSubscriber.connectionLost(self, reason)
        self.manager.on_disconnected()
        
    def messageReceived(self, channel_id, message):
        """Broad the incoming message to all clients which have subscription
        on the channel."""
        self.manager.on_message(channel_id, message)
        
    def channelSubscribed(self, channel_id, numSubscriptions):
        self.manager.channel_subscribed(channel_id, numSubscriptions)

    def channelUnsubscribed(self, channel_id, numSubscriptions):
        self.manager.channel_unsubscribed(channel_id, numSubscriptions)

    def channelPatternSubscribed(self, channel_id, numSubscriptions):
        self.manager.channel_subscribed(channel_id, numSubscriptions)

    def channelPatternUnsubscribed(self, channel_id, numSubscriptions):
        self.manager.channel_unsubscribed(channel_id, numSubscriptions)


class SlimRedisSubscriberFactory(RedisSubscriberFactory):
    protocol = SlimRedisSubscriber


# Test Codes
if __name__ == "__main__":
    print 'Done'
