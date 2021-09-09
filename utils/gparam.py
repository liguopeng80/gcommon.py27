# -*- coding: utf-8 -*- 
# created: 2021-04-28
# creator: liguopeng@liguopeng.net
import functools
from datetime import datetime
from functools import partial

from gcommon.error import GErrors
from gcommon.error.gerror import GExcept
from gcommon.utils import gtime


def param_not_null(name, value):
    if not value:
        raise GExcept(GErrors.gen_bad_request, "%s cannot be empty" % name)

    return value


def param_integer(name, value, max_value=None, min_value=None):
    try:
        value = int(value)
    except:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not int" % (name, value))

    if max_value is not None and value > max_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) > max (%s)" % (name, value, max_value))

    if min_value is not None and value < min_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) < max (%s)" % (name, value, max_value))

    return value


def param_date(name, value, max_value=None, min_value=None):
    try:
        value = datetime.fromisoformat(value)
    except:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not date" % (name, value))

    if max_value is not None and value > max_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) > max (%s)" % (name, value, max_value))

    if min_value is not None and value < min_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) < max (%s)" % (name, value, max_value))

    return value


def param_simple_date(name, value, max_value=None, min_value=None):
    try:
        value = datetime.strptime(value, "%Y%m%d")
    except:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not date" % (name, value))

    if max_value is not None and value > max_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) > max (%s)" % (name, value, max_value))

    if min_value is not None and value < min_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) < max (%s)" % (name, value, max_value))

    return value


def param_timestamp(name, value, max_value=None, min_value=None):
    try:
        value = gtime.timestamp_to_date((int(value)/1000))
    except:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not timestamp" % (name, value))

    if max_value is not None and value > max_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) > max (%s)" % (name, value, max_value))

    if min_value is not None and value < min_value:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) < max (%s)" % (name, value, max_value))

    return value


def param_enum(*allowed_values):
    return partial(_param_enum, allowed_values=allowed_values)


def _param_enum(name, value, allowed_values):
    if value not in allowed_values:
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) not in %s" % (name, value, allowed_values))

    return value


def param_digital(name, value, max_length=0, min_length=0):
    if type(value) == int:
        return

    if max_length and len(value) > max_length:
        raise GExcept(GErrors.gen_bad_request, "max length: %s" % max_length)

    if min_length and len(value) < min_length:
        raise GExcept(GErrors.gen_bad_request, "min length: %s" % min_length)

    if not value.isdigit():
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not a digital string" % (name, value))

    return value


def param_alnum(name, value):
    if type(value) == int:
        return

    if not value.isalnum():
        raise GExcept(GErrors.gen_bad_request, "%s (v=%s) is not a digital string" % (name, value))

    return value

