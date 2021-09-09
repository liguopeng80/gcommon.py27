# -*- coding: utf-8 -*- 
# created: 2021-08-04
# creator: liguopeng@liguopeng.net

"""使用 JSONable 构造序列化对象"""

from gcommon.utils.gjsonobj import *


class MyJob(JsonObjectField):
    title = JsonField("title", desc="my title")
    level = JsonField("level", desc="my job level")
    company = JsonField("company")


class Person(JsonObjectField):
    name = JsonField(desc="my name")
    job = MyJob("job", desc="my job")
    job_history = JsonListField("job_history", meta_class=MyJob)
    used_names = JsonListField("used_names")


def test_person():
    person = Person()

    person.name = "guli"
    person.job.title = "engineer"
    person.job.level = 6
    person.job.company = "working in home"

    person.job_history = [
        MyJob.load_dict({"title": "UI designer", "level": 3, "company": "ms"}),
        MyJob.load_dict({"title": "worker", "level": 3, "company": "ms"}),
    ]

    person.used_names = ["guli1", "guli2"]

    result = person.to_json()
    result = JsonObject(result)
    print(result.dumps())

    assert result.name == 'guli'
    assert result.usedNames[0] == 'guli1'
    assert result.jobHistory[0].level == 3
    assert type(result.jobHistory[0]) == JsonObject

    new_person = person.load_dict(result)
    assert new_person.name == person.name


if __name__ == '__main__':
    test_person()
