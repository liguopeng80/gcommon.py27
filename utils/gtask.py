# -*- coding: utf-8 -*- 
# created: 2021-08-11
# creator: liguopeng@liguopeng.net
from enum import Enum


class TaskStatus(Enum):
    """通用任务状态"""
    no_task = 0
    created = 1

    running = 10
    paused = 50
    finished = 90
    cancelled = 91
    aborted = 92

    @property
    def is_cancelled(self):
        return self == self.cancelled

    @property
    def is_running(self):
        return self == self.running

    @property
    def is_finished(self):
        return self.value > self.finished.value

