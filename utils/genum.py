#!/usr/bin/python
# -*- coding: utf-8 -*-
# author: "Guo Peng Li" <liguopeng@liguopeng.net>


class EnumItem(object):
    def __init__(self, name, value, desc):
        self.name = name
        self.value = value
        self.desc = desc

    def __str__(self):
        return self.name

    def __int__(self):
        return self.value

    def description(self):
        return self.desc


class Enum(object):
    def __init__(self, *names, **kw):
        self.__names = {}
        self.__values = {}

        if 'starts_from' in kw:
            index = kw.get('starts_from')
            del kw['starts_from']
        else:
            index = 0

        for name in names:
            # Description is not required for items which have no specified values.
            if isinstance(name, str):
                self.add_item(name, index)
                index = index + 1
            elif isinstance(name, EnumItem):
                self._add_enum_item(name)
            elif isinstance(name, Enum):
                for item in name.__names.values():
                    self.add_item(item.name, item.value)
            else:
                raise Exception('bad argument: invalid data type: %s' % name)

        for name, value in kw.items():
            if isinstance(value, tuple):
                if len(value) == 2:
                    value, desc = value
                    self.add_item(name, value, desc)
                else:
                    raise Exception('bad argument: invalid tuple')
            else:
                self.add_item(name, value)

    def _add_enum_item(self, item):
        self.__names[item.name] = item
        self.__values[item.value] = item

    def add_item(self, name, value, desc=''):
        item = EnumItem(name, int(value), desc)
        setattr(self, name, item)

        self._add_enum_item(item)

    def name(self, value):
        item = self.__values.get(int(value), None)

        if item:
            return item.name
        else:
            return ''

    def value(self, name):
        item = self.__names.get(name, None)

        if item:
            return item.value
        else:
            return -1

    def has_name(self, name):
        return name in self.__names

    def has_value(self, value):
        return value in self.__values

    def iteritems(self):
        return self.__names.items()


class PlainEnum(object):
    """The value a plain-enum object is specified by user, instead of
    'EnumItem' object.
    """

    def __init__(self, *names, **kw):
        self.__names = {}
        self.__values = {}

        if 'starts_from' in kw:
            index = kw.get('starts_from')
            del kw['starts_from']
        else:
            index = 0

        for name in names:
            self.add_item(name, index)
            index = index + 1

        for name, value in kw.items():
            if isinstance(value, tuple):
                if len(value) == 2:
                    value, desc = value
                    self.add_item(name, value, desc)
                else:
                    raise Exception('bad argument: invalid tuple')
            else:
                self.add_item(name, value)

    def add_item(self, name, value, desc=''):
        item = EnumItem(name, int(value), desc)
        setattr(self, name, value)

        self.__names[name] = item
        self.__values[value] = item

    def name(self, value, default=None):
        if not default:
            return self.__values[int(value)].name

        item = self.__values.get(int(value), None)

        if item:
            return item.name
        else:
            return default

    def value(self, name, default=0):
        if not default:
            return self.__names[name].value

        item = self.__names.get(name, None)

        if item:
            return item.value
        else:
            return default

    def all_names(self):
        return self.__names.keys()

    def all_values(self):
        return self.__values.keys()

    def iteritems(self):
        return self.__names.items()


# Test Codes
if __name__ == "__main__":
    EnumService = Enum('Email', 'Calendar', 'Attachemnt', 'AddressBook')

    print(EnumService.Email)

    print(EnumService.Calendar)
    print(EnumService.name(1))
    print(EnumService.name(EnumService.Calendar))

    print(EnumService.value('Attachment'))

    e100 = Enum('a', 'b', 'c', starts_from=0x100)
    print(e100.a.value, e100.b.value, e100.c.value)

    print('Done')
