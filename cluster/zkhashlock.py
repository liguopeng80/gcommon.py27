#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-10-22

"""基于 zookeeper 的动态一致性哈希锁。

1. 对资源进行互斥保护
2. 适用于资源数量多、服务节点少的应用场景。
3. 资源在集群中的分配随着新的节点加入会动态达到平衡
4. 如果一个节点长时间不能达到平衡点，需要 watchdog 强制重启
"""

import logging

from functools import partial

import rbtree
import hash_ring
from gcommon.cluster.twisted_kazoo import twisted_callback


logger = logging.getLogger('hashlock')


class HashLockObserver(object):
    def yield_resources(self, hash_lock):
        """需要出让部分资源"""
        obsoleted_keys = filter(lambda key: not hash_lock.is_my_resource(key), self._iter_resources())
        map(self._drop_resource, obsoleted_keys)
        # print 'drop: ', obsoleted_keys, len(obsoleted_keys)

    def _iter_resources(self):
        """当前服务器管理的所有资源 —— 派生类实现。

        :return: 可迭代对象，资源的 key 列表。
        """
        return [str(key) for key in range(30)]

    def _drop_resource(self, key):
        """从当前服务器管理的资源中，删除 key 对应的资源，即出让给其它服务器进行处理。

        :param key: str
        """
        pass


class HashLockNode(object):
    """集群中的一个服务节点"""
    def __init__(self, service_uid, service_seq, lock_seq='',
                 active=False, locked=False):
        self.service_uid = service_uid
        self.service_seq = service_seq

        self.lock_seq = lock_seq or self.service_seq

        self.active = active
        self.locked = locked

    def update(self, new_status):
        """更新当前节点的状态

        :param new_status:
        :type new_status: HashLockNode
        """
        assert new_status.service_seq == self.service_seq
        assert new_status.service_uid == self.service_uid
        # assert new_status.lock_seq > self.lock_seq

        self.lock_seq = max(self.lock_seq, new_status.lock_seq)

        self.active = self.active or new_status.active
        self.locked = self.locked or new_status.locked

    def set_inactive(self):
        """活跃节点中被删除（或者尚未创建）"""
        self.active = False

    def set_unlocked(self):
        """锁节点中被删除（或者尚未创建）"""
        self.locked = False

    def has_been_deleted(self):
        """服务节点被删除 —— 两种节点均不存在"""
        return not (self.active or self.locked)

    def update_lock(self, new_lock_seq):
        """
        :param new_lock_seq: 锁节点的新值。
        :type new_lock_seq: str
        """
        assert new_lock_seq >= self.lock_seq
        self.lock_seq = new_lock_seq

    def format_lock_node_path(self, new_lock=""):
        return '%s-%s-%s' % (self.service_uid, self.service_seq, new_lock or self.lock_seq)

    def format_active_node_path(self):
        return '%s-%s' % (self.service_uid, self.service_seq)

    def clone(self, active=False, locked=False):
        return HashLockNode(self.service_uid, self.service_seq, self.lock_seq, active, locked)

    def __str__(self):
        return '%s, active: %s, locked: %s' % (self.format_lock_node_path(), self.active, self.locked)

    @staticmethod
    def load_node_path(node_name, active=False, locked=False):
        """从 node name 加载节点对象。

        :param node_name: 节点名称，可以是 active node 或者 lock node.
        :type node_name: str
        :return: A ServiceNode object which is created based on node_name.
        :rtype: HashLockNode
        """
        params = node_name.split('-')
        return HashLockNode(*params, active=active, locked=locked)

    def __cmp__(self, other):
        """
        :param other: 另一个服务节点。
        :type other: HashLockNode
        """
        return cmp(self.service_seq, other.service_seq)

    def has_found(self, other):
        """是否已经发现了另一个节点。

        :param other: 其它节点。
        :type other: HashLockNode
        :return:
        """
        if self > other:
            # 节点比我早
            return True

        return self.lock_seq >= other.service_seq


class HashLock(object):
    """
    在创建节点时，首先创建服务节点，然后创建锁节点。
    但由于 zookeeper 的特殊工作机制，客户端收到的通知顺序可能相反。
    因此客户端不能依赖通知的顺序（必须都能正确处理）。
    """
    def __init__(self, observer, uid, seq):
        """
        :param observer: 一致性锁客户端
        :param uid: 当前服务器的 UID。
        :param seq: 当前服务器在服务队列中的序号。
        """
        self._observer = observer

        self._lock_nodes = rbtree.rbtree()
        self._my_node = HashLockNode(uid, seq, active=True)

        # 系统中至少有自己存在
        self._lock_nodes[uid] = self._my_node

    @property
    def my_node(self):
        return self._my_node

    @twisted_callback
    def update_zk_nodes(self, zk_node_names, active=False, locked=False):
        """处理 Zookeeper 中节点的变更通知。

        :param zk_node_names: 新的锁节点列表。
        :type zk_node_names: list
        """
        # todo: 等待第一次回调之后才算初始化成功。
        zk_nodes = [HashLockNode.load_node_path(name, active=active, locked=locked) for name in zk_node_names]

        zk_node_uids = [node.service_uid for node in zk_nodes]
        deleted_nodes = self._calc_deleted_nodes(zk_node_uids, self._lock_nodes, active=active, locked=locked)

        map(self._on_node_updated, zk_nodes)
        map(self._on_node_deleted, deleted_nodes)

        self._yield_resources(zk_nodes)
        self._observer.yield_resources()

    @staticmethod
    def _calc_deleted_nodes(zk_uids, cached_nodes, active=False, locked=False):
        """
        计算被删除的节点 —— 缓存中存在，但是 ZK 上已经不存在。

        :param zk_uids: zookeeper 上的节点 UID 列表。
        :param cached_nodes: 本地缓存的节点列表。
        :param active: 从 active node 得到通知。
        :param locked: 从 lock node 得到通知。
        :return: 被删除的节点列表，并表明发现被删除的途径。
        """
        deleted_uids = [uid for uid in cached_nodes if uid not in zk_uids]
        return [cached_nodes[uid].clone(active, locked) for uid in deleted_uids]

    def _on_node_updated(self, node):
        """检查节点有没有更新。

        :param node: zookeeper 上的节点
        :type node: HashLockNode
        """
        if node == self._my_node:
            return
        elif node.service_uid not in self._lock_nodes:
            # 新服务器进入集群，需要出让资源
            self._lock_nodes[node.service_uid] = node
            logger.info('new node created: %s', node)
        else:
            self._lock_nodes[node.service_uid].update(node)
            logger.info('update node status: %s', self._lock_nodes[node.service_uid])

    def _on_node_deleted(self, node):
        """处理可能被删除的节点 —— 也许是假删除。

        :param node: 被删除的锁节点（被删除节点在 zookeeper 上的属性）。
        :type node: HashLockNode
        :return:
        """
        if node == self._my_node:
            # 防止错误更新自己的 locked 状态
            return

        cached_node = self._lock_nodes[node.service_uid]
        if not cached_node:
            return

        if node.active:
            cached_node.set_inactive()
            # logger.info('active node not found: %s', cached_node)

        if node.locked:
            cached_node.set_unlocked()
            # logger.info('lock node not found: %s', cached_node)

        if cached_node.has_been_deleted():
            del self._lock_nodes[node.service_uid]
            logger.info('delete node: %s', cached_node)

    def _yield_resources(self, new_nodes):
        """有新增节点加入集群，放弃部分资源。"""
        if not new_nodes:
            return

        latest_node = max(new_nodes)
        if latest_node.service_seq <= self._my_node.lock_seq:
            return

        self._observer.yield_resources()
        self._observer.update_lock(self._my_node, latest_node.service_seq)

    def is_my_resource(self, key):
        if not self._is_all_agreed():
            if self._will_take_by_previous_nodes(key):
                # 被前面的节点拿走了
                return False

        return self._will_take_by_me(key)

    def _is_all_agreed(self):
        """判断我前面的所有节点是否已经看到我"""
        for node in self._lock_nodes.values():
            if node == self._my_node:
                return True

            if not node.has_found(self._my_node):
                return False

    def _will_take_by_me(self, key):
        uids = [node.service_uid for node in self._lock_nodes.values()]
        ring = hash_ring.HashRing(uids)
        return ring.get_node(key) == self._my_node.service_uid

    def _will_take_by_previous_nodes(self, key):
        """我前面的部分节点还没有发现我"""
        for node in self._lock_nodes.values():
            if not node.has_found(self._my_node):
                # 检查之前的节点是否要拿走该对象
                uids = self._get_node_uids_by_lock_seq(node.lock_seq)
                ring = hash_ring.HashRing(uids)
                if ring.get_node(key) == node.service_uid:
                    return True

        return False

    def _get_node_uids_by_lock_seq(self, lock_seq):
        return [node.service_uid for node in self._lock_nodes.values()
                if node.service_seq <= lock_seq]


class SlimHashLock(object):
    """
    使用一致性哈希对任务目标进行分区，并确保分区操作的原子性（同一时刻不会
    有两个服务器同时使用一个任务目标）。

    服务节点名称：<service_uid> + "-" + <service_sequence>
    锁节点的名称：<service_uid> + "-" + <service_sequence> + "-" + <lock_sequence>

    其中 lock_sequence >= service_sequence.

    如果某个在线服务节点的锁节点缺失，则等价于默认锁节点存在，即：
    node.lock_sequence = node.service_sequence
    """
    SERVICE_SEQUENCES = "nodes"
    RESOURCE_LOCKS = 'locks'

    def __init__(self, zk_client, lock_path, service_uid, observer, service_name="", lock_name=""):
        """
        :param zk_client: A :class:`~kazoo.client.KazooClient` instance.
        :param lock_path: The zookeeper path to consistent locks.
        """
        self._path = lock_path
        self._service_path = self._path + "/" + (service_name or self.SERVICE_SEQUENCES)
        self._lock_path = self._path + "/" + (lock_name or self.RESOURCE_LOCKS)

        # service nodes:
        self._service_nodes = []

        self._zk_client = zk_client
        self._enable_cluster(service_uid)

        self._observer = observer

    def _enable_cluster(self, service_uid):
        """创建需要的节点，并启动 zk watch."""
        self._zk_client.ensure_path(self._service_path)
        self._zk_client.ensure_path(self._lock_path)

        active_node_path = self._service_path + "/" + '%s-' % service_uid
        active_node = self._zk_client.create(active_node_path, sequence=True, ephemeral=True)
        _, seq = active_node.split('-')

        self._cluster = HashLock(self, service_uid, seq)

        self._zk_client.ChildrenWatch(self._service_path, partial(self._cluster.update_zk_nodes, active=True))
        self._zk_client.ChildrenWatch(self._lock_path, partial(self._cluster.update_zk_nodes, locked=True))

    def update_lock(self, node, new_lock):
        """更新当前服务节点的锁信息。

        同时存在多个锁不会引起逻辑问题。
        1. 新锁的值必须比旧锁大。
        2. 必须首先创建新锁，然后删除旧锁。
        """
        new_lock_path = node.format_lock_node_path(new_lock=new_lock)
        new_lock_path = self._lock_path + '/' + new_lock_path

        self._zk_client.create(new_lock_path, ephemeral=True)
        logger.info('create new lock: %s, %s', node, new_lock)

        """删除持有的旧锁"""
        if node.locked:
            # todo: abort on failure
            old_lock_path = node.format_lock_node_path()
            old_lock_path = self._lock_path + '/' + old_lock_path

            self._zk_client.delete(old_lock_path)
            logger.info('delete old lock: %s, %s', node, new_lock)

        node.locked = True
        node.lock_seq = new_lock

    def is_my_resource(self, key):
        return self._cluster.is_my_resource(key)

    def yield_resources(self):
        self._observer.yield_resources(self)

    def cleanup(self):
        """删除锁节点以及活跃节点"""
        # todo: 判断 zk client 是否已经处于连接状态
        # todo: 增加 retry 支持
        node_path = self._service_path + "/" + self._cluster.my_node.format_active_node_path()
        self._zk_client.delete(node_path)

        if self._cluster.my_node.locked:
            lock_path = self._lock_path + "/" + self._cluster.my_node.format_lock_node_path()
            self._zk_client.delete(lock_path)
