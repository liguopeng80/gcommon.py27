#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-05-04

"""服务器状态监控和管理"""
import logging
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from gcommon.cluster.twisted_kazoo import twisted_call
from gcommon.utils.errors import format_exception_stack
from gcommon.utils.observer import SimpleObservableSubject

logger = logging.getLogger('server')


class ServerStatus(object):
    """服务器的运行时状态"""
    UNKNOWN = 0

    WAITING = 10                # 等待外部依赖
    STARTING = 11               # 正在启动
    RUNNING = 12                # 正在运行
    STOPPING = 20               # 正在关闭（优雅的关闭）
    STOPPED = 21                # 服务已经停止

    ALL_STATUS = (WAITING, STARTING, RUNNING, STOPPING, STOPPED)
    ACTIVE_STATUS = (WAITING, STARTING, RUNNING)
    STANDBY_STATUS = (STOPPING, STOPPED)

    def __init__(self, status, desc=''):
        self.status = status
        self.status_desc = desc

    def is_waiting(self):
        return self.status == self.WAITING

    def is_starting(self):
        return self.status == self.STARTING

    def is_running(self):
        return self.status == self.RUNNING

    def is_stopped(self):
        return self.status == self.STOPPED

    def is_stopping(self):
        return self.status == self.STOPPING

    def is_active(self):
        return self.status in self.ACTIVE_STATUS

    def is_standby(self):
        return self.status in self.STANDBY_STATUS

    def change_status(self, new_status, desc=''):
        self.status = new_status
        self.status_desc = desc

    def __str__(self):
        return str(self.status)


class ServerEvent(object):
    # 服务器转换为活动状态
    Event_Active = 1
    # 服务器转换为备用状态
    Event_Stop = 2

    # 外部服务变为不可用状态
    Event_External_Service_Changed = 10
    # 外部依赖就绪，启动
    Event_External_Service_Ready = 11

    # 服务器启动
    Event_Start_Failed = 20     # 启动失败
    Event_Started = 21          # 启动成功

    # 停止
    Event_Stopped = 31


class ServerStatusController(SimpleObservableSubject):
    """对应用服务器的状态进行管理和维护

    TODO: 实现 active/standby 状态切换

    当 service 被激活后，会尝试启动所有依赖的外部服务。
    """
    RETRY_INTERVAL = 3     # 每三秒钟重试一次

    def __init__(self, app_server, status=ServerStatus.UNKNOWN):
        SimpleObservableSubject.__init__(self)

        # 当前运行的应用服务器
        self.app_server = app_server

        # 应用服务器所以来的外部服务
        self.services = {}

        # 服务器状态
        self.status = ServerStatus(status)

        self.State_Transition_Table = self.init_transition_table()

        self._in_processing = False
        self._pending_events = []

    def start(self):
        self.schedule_event(ServerEvent.Event_Active)

    def is_running(self):
        return self.status.is_running()

    def schedule_event(self, event):
        """缓存事件队列"""
        logger.info('schedule event: %s', event)
        twisted_call(self._do_schedule_event, event)

    def _do_schedule_event(self, event):
        self._pending_events.insert(0, event)
        reactor.callLater(0, self._process_events)

    @inlineCallbacks
    def _process_events(self):
        """处理事件以及事件的所有后续事件"""
        logger.debug('process_events, in processing: %s, pending events: %s, status: %s',
                     self._in_processing, self._pending_events, self.status)

        if self._in_processing:
            logger.debug('waiting, as handler can only process one event at the same time')
            return

        while self._pending_events:
            self._merge_event_list()

            if not self._pending_events:
                break

            event = self._pending_events.pop()
            try:
                action = self._get_event_action(event)
                logger.debug('start processing event, event: %s, status: %s, action: %s',
                             event, self.status, action and action.__name__)
                if action:
                    yield maybeDeferred(action)
            except Exception, e:
                # todo: check exception type
                logger.debug('event exception, event: %s, error: %s', event, e)
            else:
                pass

            logger.debug('event processed, event: %s, status: %s', event, self.status)

        self._in_processing = False

    def _merge_event_list(self):
        """合并重复的事件"""
        # TODO: merge event list

    def _get_event_action(self, event):
        """处理事件，并返回后续的事件（如果有）"""
        for tbl_status, tbl_event, tbl_action in self.State_Transition_Table:
            # 状态为空，标识可以匹配任何状态
            if isinstance(tbl_status, tuple):
                status_allowed = self.status.status in tbl_status
            else:
                status_allowed = self.status.status == tbl_status

            # 事件类型为空表示可以匹配任何事件
            if tbl_event:
                event_allowed = (tbl_event == event)
            else:
                event_allowed = True

            if status_allowed and event_allowed:
                return tbl_action

    def _cleanup(self):
        """清除系统中的旧对象..."""
        # todo: code

    def _handle_evt_activate_server(self):
        """服务器进入（或者重新进入）活跃状态"""
        self.status.change_status(ServerStatus.WAITING, 'activate server')

        # 清理资源
        self._cleanup()

        # 启动并等待所有外部服务
        for service in self.services.itervalues():
            reactor.callLater(0, service.start)

    def _handle_evt_external_service_changed(self):
        """依赖的外部服务可用"""
        if self.status.is_waiting():
            # 不在等待状态，忽略
            if self._are_all_services_ready():
                self.schedule_event(ServerEvent.Event_External_Service_Ready)

        elif self.status.is_running():
            if not self._are_all_services_ready():
                self._switch_status_from_running_to_waiting()
                self.notify_observers()

    def _handle_evt_external_services_ready(self):
        """依赖的外部服务全部可用"""
        logger.info('evt: service ready')

        if not self.status.is_waiting():
            # 不在等待状态，忽略
            return

        if self._are_all_services_ready():
            self._switch_status_from_waiting_to_starting()
        else:
            logger.error('evt: services are not ready yet')

    def _handle_evt_server_started(self):
        """服务器启动成功"""
        self.status.change_status(ServerStatus.RUNNING)
        self.notify_observers()

    def _handle_evt_start_failed(self):
        """启动服务器失败"""
        self.status.change_status(ServerStatus.STARTING)
        for service in self.services.itervalues():
            service.start_service()

    def _handle_evt_stop_server(self):
        """服务器进入备用状态"""
        # TODO: 实现....
        self.status.change_status(ServerStatus.STOPPING)
        return ServerEvent.Event_Stop

    def _handle_evt_server_stopped(self):
        """服务器停止成功"""
        # TODO: 实现....
        self.status.change_status(ServerStatus.STOPPED)

    def _switch_status_from_running_to_waiting(self):
        """running -> waiting"""
        self.status.change_status(ServerStatus.WAITING, 'key service offline')

    def _switch_status_from_waiting_to_starting(self):
        """waiting -> starting"""
        self.status.change_status(ServerStatus.STARTING)

        # 等待派生类执行服务器就绪的初始化操作
        try:
            self.notify_observers()
        except Exception, e:
            logger.error('exception when starting server - %s : %s', e, format_exception_stack())
            self.schedule_event(ServerEvent.Event_Start_Failed)
        else:
            self.schedule_event(ServerEvent.Event_Started)

    def init_transition_table(self):
        """服务器的状态迁移表"""
        return (
            # 等待状态
            (ServerStatus.WAITING, ServerEvent.Event_External_Service_Changed, self._handle_evt_external_service_changed),
            (ServerStatus.WAITING, ServerEvent.Event_External_Service_Ready, self._handle_evt_external_services_ready),

            # 启动状态
            (ServerStatus.STARTING, ServerEvent.Event_Started, self._handle_evt_server_started),
            (ServerStatus.STARTING, ServerEvent.Event_Start_Failed, self._handle_evt_start_failed),

            # 运行状态
            (ServerStatus.RUNNING, ServerEvent.Event_External_Service_Changed, self._handle_evt_external_service_changed),

            # 停止状态
            (ServerStatus.STOPPING, ServerEvent.Event_Stopped, self._handle_evt_server_stopped),

            # 从活跃状态转备用
            (ServerStatus.ACTIVE_STATUS, ServerEvent.Event_Stop, self._handle_evt_stop_server),

            # 从备用状态重新激活
            (ServerStatus.STANDBY_STATUS, ServerEvent.Event_Active, self._handle_evt_activate_server),

            # 忽略重复的激活和备用请求
            (ServerStatus.ACTIVE_STATUS, ServerEvent.Event_Active, None),
            (ServerStatus.STANDBY_STATUS, ServerEvent.Event_Stop, None),

            # 服务器启动之后的状态变迁
            (ServerStatus.UNKNOWN, ServerEvent.Event_Active, self._handle_evt_activate_server),
            (ServerStatus.UNKNOWN, ServerEvent.Event_Stop, self._handle_evt_server_stopped),
        )

    def register_service(self, service):
        """注册外部服务"""
        logger.info('register service: %s', service.name)
        self.services[service.name] = service
        service.subscribe(self.change_service_status)

    def unregister_service(self, service_name):
        """删除外部服务"""
        logger.info('unregister service: %s', service_name)

        service = self.services.pop(service_name, None)
        service.unsubscribe(self.change_service_status)

    def change_service_status(self, service):
        assert service.name in self.services
        self.schedule_event(ServerEvent.Event_External_Service_Changed)

    def on_service_status_changed(self, service):
        """外部服务的状态发生了变化"""
        if service.is_good():
            if service.is_crucial():
                logger.critical('crucial external service recovered, start server - %s', service)
                self._try_recovering_server()
            else:
                logger.critical('trivial external service recovered, ignore - %s', service)
        else:
            if service.is_crucial():
                # 核心外部服务不可用
                logger.critical('crucial external service error, stop server - %s', service)
                self._try_stopping_server()
            else:
                logger.warn('trivial external service error, ignore - %s', service)

    def _are_all_services_ready(self):
        """是否所有外部服务都已经就绪"""
        for service in self.services.itervalues():
            if service.is_crucial() and service.is_bad():
                return False

        return True



