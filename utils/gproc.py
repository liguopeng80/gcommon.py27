#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: 2015-02-27

"""进程辅助工具。"""

import os
import shlex
import socket
import subprocess


import logging


logger = logging.getLogger("proc")


def _hostname():
    return socket.gethostname().replace('-', '').lower()


def get_process_id(service_name, instance_id=0):
    """进程的标识。

    service_name: 服务名称 (gatekeeper, governor, postman, etc)
    instance_id:  服务的实例编号 (0, 1, 2, etc)
    """
    # server01.gateway.00
    if instance_id:
        proc_id = '%s.%s.%02d' % (_hostname(), service_name, instance_id)
    else:
        proc_id = '%s.%s' % (_hostname(), service_name)

    return proc_id


def get_process_unique_id(service_name, instance_id=0):
    """进程的唯一标识。

    service_name: 服务名称 (gatekeeper, governor, postman, etc)
    instance_id:  服务的实例编号 (0, 1, 2, etc)
    """
    pid = os.getpid()

    # server01.gateway.00.87312
    myid = '%s.%s.%02d.%s' % (_hostname(), service_name, instance_id, pid)

    return myid


def execute_command(cmd_str, working_dir="",
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
    """执行系统命令，并返回 process 对象。
    
    例如：
    cmd = 'sh -c "ps -ef | grep -i python | grep -i %s | grep -v grep"' % sid
    proc = execute(cmd)
    """
    logger.debug("execute: %s (in %s)", cmd_str, working_dir or os.curdir)
        
    old_curdir = os.curdir
    if working_dir:
        os.chdir(working_dir)

    try:
        args = _parse_cmd_str(cmd_str)
        p = subprocess.Popen(args, stdin=stdin, stdout=stdout, stderr=stderr)
    finally:
        if working_dir:
            os.chdir(old_curdir)

    return p


def _parse_cmd_str(cmd_str):
    """解析命令行参数"""
    args = shlex.split(cmd_str)
    return args


def execute_and_read_lines(cmd_str) -> [str]:
    """执行命令，并且读取所有的输出之后返回"""
    proc = execute_command(cmd_str)

    lines = []
    while True:
        line = proc.stdout.readline()
        if not line:
            break

        line = line.strip()
        lines.append(line)

    return lines


def execute_and_wait(cmd_str, working_dir="", ensure_success=True,
                     stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=0):
    p = execute_command(cmd_str, working_dir, stdin, stdout, stderr)

    if timeout:
        p.wait(timeout)
    else:
        p.wait()

    for line in p.stdout.readlines():
        line = line.strip().decode('utf-8')
        logger.debug("%s", line)

    if p.returncode ==0:
        logger.debug("success on cmd: %s", cmd_str)
    else:
        message = "failed on cmd: %s (code: %s)" % (cmd_str, p.returncode)
        logger.error("failed on cmd: %s (code: %s)", cmd_str, p.returncode)
        if ensure_success:
            raise RuntimeError(message)

    return p.returncode


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    execute_and_wait("ls -l")
    execute_and_wait("wget https://www.baidu.com")
    execute_and_wait("mv not-existed-file wrong-target")

