#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-04-22

"""ZooKeeper 的同步通信客户端。

和 twisted 联用时请注意：所有 watch observer 必须使用 reactor.callFromThread() 将 watch 结果返回给 twisted 线程。

为调用方便，请使用 twisted_kazoo.twisted_callback 对回调进行封装。
"""

import threading
from multiprocessing import Queue

from kazoo.client import KazooClient
from kazoo.client import KazooState


import logging
from kazoo.handlers.threading import KazooTimeoutError
from gcommon.cluster.twisted_kazoo import twisted_call
from gcommon.net.netutil import ConnectionStatus

logger = logging.getLogger('kazoo')


class ZookeeperObserver(object):
    ZK_Conn_Connecting = 'CONNECTING'

    def __init__(self):
        self.client_manager = None

        self.conn_status = ConnectionStatus.Initialized

        self._zk_conn_status = self.ZK_Conn_Connecting

    def set_client_manager(self, client_manager):
        self.client_manager = client_manager
        self.zk = self.client_manager.kazoo_client

    def on_connection_failed(self, reason=None):
        """Client Manager 回调"""
        logger.error('cannot connect to zookeeper, reason: %s', reason)

        self.conn_status = ConnectionStatus.Closed
        self._zk_conn_status = KazooState.LOST

        twisted_call(self._on_conn_failed)

    def _on_conn_opened(self):
        """连接打开或者恢复"""
        pass

    def _on_conn_lost(self):
        """会话断开"""
        pass

    def _on_conn_suspended(self):
        """连接断开，会话挂起，尝试恢复中"""
        pass

    def _on_conn_failed(self):
        """第一次连接失败，无法建立会话"""
        pass

    def on_connection_status_changed(self, state):
        logger.debug('connection status changed from %s to %s', self._zk_conn_status, state)
        self._zk_conn_status = state

        if state == KazooState.CONNECTED:
            self.conn_status = ConnectionStatus.Connected
            twisted_call(self._on_conn_opened)
        elif state == KazooState.LOST:
            self.conn_status = ConnectionStatus.Closed
            twisted_call(self._on_conn_lost)
        elif state == KazooState.SUSPENDED:
            self.conn_status = ConnectionStatus.Suspended
            twisted_call(self._on_conn_suspended)


class _ZookeeperClientThread(threading.Thread):
    """运行 kazoo 客户端的专用线程。"""
    def __init__(self, client):
        threading.Thread.__init__(self)
        self._client = client

    def run(self):
        logger.info('enter kazoo thread')
        self._client.thread_main()
        logger.info('leave kazoo thread')


class ZookeeperClientManager(object):
    """Kazoo 客户端管理器"""
    def __init__(self, observer, server_addr):
        self._observer = observer

        self._kazoo_client = KazooClient(hosts=server_addr)
        self._q_service_control = Queue()

        self._is_running = True
        self._thread = _ZookeeperClientThread(self)

    @property
    def kazoo_client(self):
        return self._kazoo_client

    def is_running(self):
        return self._is_running

    def send_control_message(self, message):
        """发送控制消息，控制消息必须在客户端的启动线程中处理"""
        self._q_service_control.put(message)

    def _process_service_control_message(self):
        """处理控制消息"""
        message = self._q_service_control.get()
        logger.debug('process control message: %s', message)

        if message == "stop":
            self._is_running = False
            self._kazoo_client.stop()

    def start(self):
        """启动独立线程运行 zookeeper 客户端"""
        logger.info('start kazoo client')

        self._kazoo_client.add_listener(self._observer.on_connection_status_changed)
        self._thread.start()

    def stop(self):
        logger.info('stop kazoo client')
        self.send_control_message('stop')

    def wait(self):
        logger.info('wait kazoo client exiting')
        self._thread.join()
        logger.info('kazoo client stopped')

    def thread_main(self):
        """尝试连接服务器，如果多次连接失败则抛出超时错"""
        try:
            self._kazoo_client.start()
        except KazooTimeoutError, e:
            self._observer.on_connection_failed(e)
            return
        except Exception, e:
            self._observer.on_connection_failed(e)
            return

        while self.is_running():
            self._process_service_control_message()

if __name__ == '__main__':
    format = '%(asctime)-15s %(levelname)-3s %(name)-8s %(message)s'
    logging.basicConfig(format=format, level=logging.DEBUG)

    import threading
    lock = threading.Lock()
    lock.acquire()

    class MyObserver(ZookeeperObserver):
        def on_connection_failed(self):
            ZookeeperObserver.on_connection_failed(self)
            lock.release()

        def on_connection_status_changed(self, state):
            ZookeeperObserver.on_connection_status_changed(self, state)

            if state == KazooState.SUSPENDED:
                lock.release()

    hosts = '192.168.1.14:2181'
    observer = MyObserver()

    manager = ZookeeperClientManager(observer, hosts)
    observer.set_client_manager(manager)

    manager.start()

    # 等待事件
    lock.acquire()

    manager.stop()
    manager.wait()

