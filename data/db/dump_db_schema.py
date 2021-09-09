#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2014-11-27

import sys
import optparse
import dbconfig
import imp


def parse_command_line(parser, argv):
    # set usage
    usage_text = """Create database schema.
    %(app)s [-u user] [-p password] [-h host] [-n database name]"""
    usage_param = {'app': argv[0]}

    parser.set_usage(usage_text % usage_param)

    # add arguments
    parser.add_option('-m', '--model', dest='model', action='store',
                      default=None, help='model path')
    argv = argv[1:]
    return parser.parse_args(argv)


def main():
    parser = optparse.OptionParser()
    options, args = parse_command_line(parser, sys.argv)

    models = imp.load_source('models', '%(model)s' % options)
    dbconfig.dump_db_schema(models._metadata)


if __name__ == '__main__':
    main()

