#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-01-14

"""Message subscriber"""

import logging
import fnmatch

from gcommon.data.cache import SlimSubscriberManager

logger = logging.getLogger('pubsub')


class SlimPatternSubscriberManager(SlimSubscriberManager):
    """Connection resume and clients management."""
    def __init__(self):
        SlimSubscriberManager.__init__(self)

    def on_connected(self):
        """Connection resumed. Try re-subscribe all channels."""
        logger.info('connection to redis resumed')
        for chid in self.clients.iterkeys():
            self.subscriber.psubscribe(chid)

    def on_message(self, channel_id, message):
        """Broad the incoming message to all clients which have subscription
        on the channel."""
        logger.access('-- SlimPatternSubscriberManager subscribe, channel_id: %s, message: %s', channel_id, message)

        clients = None
        for key in self.clients.iterkeys():
            # redis 仅支持 glob-style 的正则
            if fnmatch.fnmatchcase(channel_id, key):
                clients = self.clients.get(key, None)
                break

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
            self.subscriber.punsubscribe(channel_id)

    def subscribe(self, client, channel_id):
        """Subscribe to a chid.

        If this is the first client of the chid, current subscriber must
        send "subscribe" request to redis server.

        return: True if the channel has been subscribed.
        """
        logger.access('-- SlimPatternSubscriberManager subscribe, channel_id: %s', channel_id)

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
            self.subscriber.psubscribe(channel_id)
            logger.debug('SlimSubscriberManger need subscribe')
            return False

        elif channel_id in self._subscribed_channels:
            # the channel has been subscribed
            logger.debug('SlimSubscriberManager have subscribed')
            return True
        else:
            logger.debug('SlimSubscriberManager return NONE!!!!!!!')

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
                self.subscriber.punsubscribe(channel_id)

    def channel_subscribed(self, channel_id, num):
        logger.debug('-- SlimPatternSubscriberManager channel_subscribed, channel_id: %s, num: %s', channel_id, num)
        self._subscribed_channels.add(channel_id)

        clients = self.clients.get(channel_id, None)

        if not clients:
            self.subscriber.punsubscribe(channel_id)
            logger.debug('-- SlimPatternSubscriberManager channel_subscribed, after punsub')
            if clients is not None:
                del self.clients[channel_id]
        else:
            for client in clients:
                client.on_sub_registered(channel_id)

    def channel_unsubscribed(self, channel_id, _num):
        self._subscribed_channels.remove(channel_id)
        

# Test Codes
if __name__ == "__main__":
    print 'Done'
