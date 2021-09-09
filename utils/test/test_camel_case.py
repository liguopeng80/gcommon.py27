# -*- coding: utf-8 -*- 
# created: 2021-08-04
# creator: liguopeng@liguopeng.net

from gcommon.utils.gstr import camel_to_snake, snakeToCamel, SnakeToCamel


def test_camel_to_snake():
    result = camel_to_snake('getHTTPResponseCode')
    print(result)
    assert result

    result = camel_to_snake('HTTPResponseCodeXYZ')
    print(result)


def test_snake_to_camel():
    result = snakeToCamel('get_http_response_code')
    print(result)
    assert result

    result = SnakeToCamel('get_http_response_code')
    print(result)
    assert result

    result = snakeToCamel('get')
    print(result)
    assert result

    result = SnakeToCamel('get')
    print(result)
    assert result

    result = snakeToCamel('http_response_code_xyz')
    print(result)


if __name__ == '__main__':
    test_camel_to_snake()
    test_snake_to_camel()
