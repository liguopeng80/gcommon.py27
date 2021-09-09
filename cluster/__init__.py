#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-03-16

"""服务器通用的启动、配置等工具库。"""

from cluster_manager import NodeManager, ClusterManager

Failover_Enabled = False

# 所有需要进行 fail over 的服务器节点
All_Node_Managers = ()


if __name__ == '__main__':
    print 'Done'
