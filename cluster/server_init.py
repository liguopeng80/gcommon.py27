#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-03-09

"""Slim IM server 所依赖的数据库、缓存等系统初始化。"""

import logging

from gcommon.cluster.cluster_manager import ClusterManager
from gcommon.cluster.svc_zookeeper import ZookeeperService

from gcommon.data.app import redis_cache
from gcommon.utils.file import remove_trailing_slash

from gcommon.data import db
from gcommon.data import cache

logger = logging.getLogger('server')


def init_db_engine(cfg):

    db_conn_template = 'mysql://%(user)s:%(pass)s@%(server)s/%(db)s?charset=utf8'

    db_conn_param = {
        'user' : cfg.get('mysql.username'),
        'pass' : cfg.get('mysql.password'),

        'server' : cfg.get('mysql.server'),
        'port' : cfg.get_int('mysql.port'),

        'db' : cfg.get('mysql.dbname'),
    }

    db_conn_str = db_conn_template % db_conn_param

    db.init(db_conn_str)


def init_redis_client(cfg):
    cache.init(cfg, 'team')


def init_data_layer():
    redis_cache.init_data_layer(cache)

    
########################################################################
# Zookeeper 服务管理
########################################################################
def create_zookeeper_service():
    logger.debug('create zookeeper service')

    server = ClusterManager.server
    controller = ClusterManager.controller

    hosts = server.cfg.get('zookeeper.hosts')
    interval = server.cfg.get_int('zookeeper.connection_interval')

    zk_service = ZookeeperService(hosts, interval)

    controller.register_service(zk_service)

    return zk_service


########################################################################
# 已经启动的节点管理（已经启动，但是不一定正在提供服务）
########################################################################
def zk_create_alive_node(zk_client, data=None):
    """创建已经启动的服务节点"""
    node_path = get_parent_path_to_alive_service() + "/" + ClusterManager.server.unique_server_name

    zk_client.ensure_path(get_parent_path_to_alive_service())
    result = zk_client.create(node_path, data, ephemeral=True)

    return result


def get_parent_path_to_alive_service(service_name=''):
    """某个服务的已近启动的节点根目录"""
    working_root = ClusterManager.server.cfg.get('zookeeper.path_alive_apps')
    working_root = working_root % {'service': service_name or ClusterManager.server.SERVICE_NAME}

    return remove_trailing_slash(working_root)


########################################################################
# 活跃服务节点管理（活跃：在线并可以提供服务）
########################################################################
def zk_create_working_node(zk_client, data=None):
    """创建活跃的服务节点"""
    node_path = get_parent_path_to_working_service() + "/" + ClusterManager.server.unique_server_name

    zk_client.ensure_path(get_parent_path_to_working_service())
    result = zk_client.create(node_path, data, ephemeral=True)

    return result

    
def zk_delete_working_node(zk_client):
    """删除活跃的服务节点"""
    return zk_client.delete(get_path_to_working_node())


def get_path_to_working_node():
    """当前服务的活跃节点根目录"""
    root_path = get_parent_path_to_working_service()
    node_path = root_path + "/" + ClusterManager.server.unique_server_name

    return node_path


def get_parent_path_to_working_service(service_name=''):
    """某个服务的活跃节点根目录"""
    working_root = ClusterManager.server.cfg.get('zookeeper.path_working_apps')
    working_root = working_root % {'service': service_name or ClusterManager.server.SERVICE_NAME}

    return remove_trailing_slash(working_root)


########################################################################
# 哈希锁节点管理
########################################################################
def get_parent_node_to_hash_lock(service_name=''):
    """某个服务对团队的锁定"""
    path = ClusterManager.server.cfg.get('zookeeper.path_app_hash_locks')
    path = path % {'service': service_name or ClusterManager.server.SERVICE_NAME}

    return remove_trailing_slash(path)
