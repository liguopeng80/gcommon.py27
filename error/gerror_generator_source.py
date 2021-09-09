#!/usr/bin/python
# -*- coding: utf-8 -*-
# created: '2015-02-01'

from gcommon.utils.genum import Enum, EnumItem

Domain = Enum(
    general=100,
    user=110,
    device=120,

    rpc=150,

    internal=188,
    protocol=199,

    third_party_base=900,
)

default_domain = EnumItem('general', 0, '')

domain_base = 1000
code_length = 6

error_defines = (
    (default_domain, 0, 'ok', '操作成功'),

    # 通用
    (Domain.general, 1, 'gen_server_internal', '操作失败'),
    (Domain.general, 2, 'gen_bad_request', '请求错误'),
    (Domain.general, 3, 'gen_partial_completed', '部分操作被完成'),
    (Domain.general, 4, 'gen_permission_denied', '没有权限'),
    (Domain.general, 5, 'gen_exceed_request_limit', '时间段内访问次数过多'),
    (Domain.general, 6, 'gen_ip_banned', 'ip被封禁'),
    (Domain.general, 7, 'gen_not_completed_yet', '操作尚未完成'),
    (Domain.general, 8, 'gen_target_not_found', '对象不存在'),

    # 用户注册
    (Domain.user, 1, 'err_user_exists', '用户名已经存在，无法注册'),
    (Domain.user, 2, 'reg_too_much_reg_in_current_loc', '当前服务器用户过多，无法注册新用户'),
    (Domain.user, 3, 'err_token_invalid', 'token错误'),
    (Domain.user, 4, 'err_token_expired', '验证token已过期'),
    (Domain.user, 5, 'err_user_inactive', '账号已存在但未激活'),
    (Domain.user, 6, 'err_bad_token', 'token无效'),
    (Domain.user, 7, 'err_user_banned', '用户被禁用'),
    (Domain.user, 8, 'err_user_activated', '用户已激活'),
    (Domain.user, 9, 'err_user_pending_email_validation', '用户邮箱未验证'),

    # 登录错误、用户身份认证错误
    (Domain.user, 101, 'gen_need_authentication', '用户尚未登录'),
    (Domain.user, 102, 'auth_invalid_user_or_pass', '用户名或密码错误，登录失败。请检查您的输入是否正确?'),
    (Domain.user, 103, 'auth_too_much_errors', '该用户登录失败次数过多。为保护帐号安全，屏蔽该用户登录10分钟。请稍后重试'),
    (Domain.user, 104, 'auth_invalid_account_status', '用户帐号的状态异常'),
    (Domain.user, 105, 'auth_permission_denied', '无子系统权限'),

    # 服务器内部错误
    (Domain.internal, 1, 'server_too_much_users', '当前服务器的活跃用户已超出服务器的容量，请等待服务器空闲时重试'),
    (Domain.internal, 2, 'server_not_implemented', '服务器尚未实现此功能'),

    # 协议、客户端版本错误
    (Domain.protocol, 1, 'gen_client_version_expired', '客户端版本太低'),
    (Domain.protocol, 2, 'gen_config_version_expired', '配置文件版本太低'),
    (Domain.protocol, 3, 'gen_protocol_not_supported', '不支持'),
)

