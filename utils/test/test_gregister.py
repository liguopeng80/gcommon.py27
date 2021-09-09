# -*- coding: utf-8 -*- 
# created: 2021-04-18
# creator: liguopeng@liguopeng.net
from gcommon.utils.gregister import Registry, RegistryItemNotExisting


class DemoBase(object): pass


class DemoA(DemoBase): pass


class DemoB(DemoBase): pass


def test_register_string():
    r1 = Registry("str reg")
    r1.register("test1", 1)
    r1.register("test2", 2)
    assert r1.get("test2") == 2


def test_register_classes():
    # Register for class
    r2 = Registry("class reg", except_on_not_found=True)
    r2.register_classes(DemoBase, globals(), lambda x: x.__name__)
    assert r2.get("DemoA") == DemoA
    assert len(r2._objects) == 2
    try:
        not_found = False
        demo_base_class = r2.get("DemoBase")
    except RegistryItemNotExisting as e:
        not_found = True
    assert not_found


def test_register_objects():
    # Register for obj
    dbase = DemoBase()
    da = DemoA()
    db = DemoB()
    r3 = Registry("obj reg")
    r3.register_instances(DemoBase, locals(), str)
    print(r3._objects)
    assert len(r3._objects) == 2


def test_register_objects_with_base():
    dbase = DemoBase()
    da = DemoA()
    db = DemoB()

    r4 = Registry("obj reg")
    r4.register_instances(DemoBase, locals(), str, allow_base=True)
    print(r4._objects)
    assert len(r4._objects) == 3

