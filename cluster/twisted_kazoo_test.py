#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-04-29

import logging
import threading
from kazoo.protocol.states import KazooState, KeeperState
import time
from gcommon.cluster.twisted_kazoo import twisted_callback

formatter = '%(asctime)-15s %(levelname)-3s %(name)-8s %(message)s'
logging.basicConfig(format=formatter, level=logging.DEBUG)

logger = logging.getLogger()

from twisted.internet import reactor
from gcommon.cluster.zookeeper import ZookeeperObserver, ZookeeperClientManager


PATH = "/test/guli/favorite"
APP_ROOT = "/test/guli/app"
APP_PATH = "/test/guli/app/gatekeeper"


class MyObserver(ZookeeperObserver):
    def __init__(self):
        super(MyObserver, self).__init__()

    def on_connection_failed(self):
        ZookeeperObserver.on_connection_failed(self)
        reactor.callInThread(self._on_conn_failed)

    def on_connection_status_changed(self, state):
        """示例代码，演示如何捕获和处理连接状态变化事件。"""
        logger.debug('watch func called in thread: %s', threading.currentThread())
        if state == KazooState.CONNECTED:
            reactor.callInThread(self._on_conn_opened)
            if self.client_manager.kazoo_client.client_state == KeeperState.CONNECTED_RO:
                logger.debug("Read only mode!")
            else:
                logger.debug("Read/Write mode!")
        elif state == KazooState.LOST:
            reactor.callInThread(self._on_conn_lost)
            logger.debug('kazoo connection lost (client closed)')
        elif state == KazooState.SUSPENDED:
            reactor.callInThread(self._on_conn_suspended)
            logger.debug('kazoo connection suspended (maybe the server is gone)')

    @twisted_callback
    def on_children_changed(self, children):
        logger.debug('children changed - %s', children)

    @twisted_callback
    def on_data_changed(self, data, stat, event):
        logger.debug("data changed - version: %s, data: %s" % (stat.version, data.decode("utf-8")))

    def _on_conn_lost(self):
        pass

    def _on_conn_opened(self):
        self.zk.ensure_path(PATH)
        self.zk.ChildrenWatch(PATH, self.on_children_changed)
        self.zk.DataWatch(PATH, self.on_data_changed)


        data = "hehe-%s" % time.time()

        self.zk.ensure_path(APP_PATH)
        self.zk.create(APP_PATH, data, ephemeral=True, sequence=True)
        self.zk.ChildrenWatch(APP_ROOT, self.on_app_children_changed)
        self.zk.DataWatch(APP_ROOT, self.on_app_data_changed)

        # reactor.callLater(5, self._gen_test_data)

    @twisted_callback
    def on_app_children_changed(self, children):
        logger.debug('-- app children changed - %s', children)

    @twisted_callback
    def on_app_data_changed(self, data, stat, event):
        logger.debug("-- app data changed - version: %s, data: %s" % (stat.version, data.decode("utf-8")))

    def _gen_test_data(self):
        # path = PATH + '/' + str(time.time())
        # self.zk.create(path, b"a value")
        self.zk.set(PATH, "wahaha-%s" % time.time())
        reactor.callLater(2, self._gen_test_data)

    def _on_conn_suspended(self):
        pass

    def _on_conn_failed(self):
        pass

    def start(self):
        self.client_manager.start()

    def stop(self):
        self.client_manager.stop()
        self.client_manager.wait()

if __name__ == '__main__':
    hosts = '192.168.1.14:2181'
    observer = MyObserver()

    manager = ZookeeperClientManager(observer, hosts)
    observer.set_client_manager(manager)

    reactor.callWhenRunning(observer.start)
    reactor.run()

