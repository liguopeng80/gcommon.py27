# -*- coding: utf-8 -*- 
# created: 2017-02-13
# creator: liguopeng@liguopeng.net
import inspect


class RegistryItemNotExisting(RuntimeError):
    pass


class Registry(object):
    def __init__(self, register_name, default=None, except_on_not_found=False):
        self.register_name = register_name

        self.default = default
        self.except_on_not_found = except_on_not_found

        self._objects = {}

    def register(self, name, value):
        """添加注册项目"""
        self._objects[name] = value

    def unregister(self, name):
        """添加注册项目"""
        if name in self._objects:
            del self._objects[name]

    def get(self, name):
        """查找注册项"""
        value = self._objects.get(name, self.default)
        if self.except_on_not_found and value == self.default:
            msg = "cannot found %s in registry %s" % (name, self.register_name)
            raise RegistryItemNotExisting(msg)

        return value

    def register_classes(self, base_class, context, func_get_name=None, allow_base=False):
        """注册某个上下文中的所有子类"""
        if inspect.ismodule(context):
            context = dir(context)

        for name, value in context.items():
            if inspect.isclass(value) and issubclass(value, base_class):
                if not allow_base and (value == base_class):
                    continue

                if func_get_name:
                    name = func_get_name(value)
                self._objects[name] = value

    def register_instances(self, base_class, context, func_get_name=None, allow_base=False):
        """注册某个上下文中的所有对象"""
        if inspect.ismodule(context):
            context = dir(context)

        for name, value in context.items():
            if not allow_base and (type(value) == base_class):
                continue

            if isinstance(value, base_class):
                if func_get_name:
                    name = func_get_name(value)
                self._objects[name] = value



