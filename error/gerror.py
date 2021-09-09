#!/usr/bin/python
# -*- coding: utf-8 -*- 
# author: "Li Guo Peng" <liguopeng@liguopeng.net>


class GError(object):
    def __init__(self, code, name, desc):
        self.code = code
        self.name = name
        self.desc = desc

    @staticmethod
    def create(code, error_defines):
        name, desc = error_defines[code]
        return GError(code, name, desc)

    def clone(self, name="", desc=""):
        return GError(self.code, name or self.name, desc or self.desc)

    def raise_if(self, cond, desc="", **kwargs):
        if cond:
            raise GExcept(self, desc or self.desc, **kwargs)

    def r(self, desc="", **kwargs):
        raise GExcept(self, desc or self.desc, **kwargs)

    def raise_(self, desc="", **kwargs):
        raise GExcept(self, desc or self.desc, **kwargs)

    def equals(self, other):
        if type(other) == GError:
            return self.code == other.code
        elif type(other) == GExcept:
            return self.code == other.cmd_error
        else:
            return False

    def assert_(self, cond, desc="", **kwargs):
        if not cond:
            raise GExcept(self, desc or self.desc, **kwargs)


class GExcept(Exception):
    def __init__(self, error_obj, message="", **kwargs):
        # desc = '%s (%s): %s' % (error_obj.code, error_obj.name, error_obj.desc + message)
        desc = '%s (%s): %s' % (error_obj.code, error_obj.name, message)
        Exception.__init__(self, desc)

        self.cmd_error = error_obj.code
        self.message = desc

        self.kwargs = kwargs

    def __eq__(self, other):
        if type(other) == GError:
            return self.cmd_error == other.code
        elif type(other) == GExcept:
            return self.cmd_error == other.cmd_error
        else:
            return False


class GInputError(GExcept):
    # 输入不符合要求
    pass


class GRPCError(GExcept):
    # RPC 调用异常
    pass


class GRequestFailed(GExcept):
    # 用户的请求的操作无法完成
    pass


class GUnauthorized(GExcept):
    # 用户需要首先进行身份认证
    pass


class GUnsupported(GExcept):
    # 不支持请求的操作
    pass


