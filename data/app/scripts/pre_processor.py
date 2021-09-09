#!/usr/bin/python
# -*- coding: utf-8 -*- 
# created: 2015-09-21

# Simple C-like pre processor for lua
# In File -> Out File
# INCLUDE_PATH=$SCRIPTS_ROOT
# python pre_processor in.lua -o out.lua

import sys
import optparse
import re


def main(opts, args):
    output = opts['output']

    origin_file = open(args[0], 'r').read()
    include_pattern = r'(?P<include_cmd>#include[ ]*\"(?P<filename>.*?)\")'
    included_files = re.findall(include_pattern, origin_file)

    for dependency in included_files:
        filename = dependency[1]
        dependency_file = open(filename, 'r').read()
        origin_file = origin_file.replace(dependency[0], dependency_file)

    with open(output, mode='w') as fout:
            fout.write(origin_file)


def parse_command_line(parse, argv):
    # Set usage
    usage_text = """ Lua script dependencies processor
    %(app)s [input_file] [-o output_file]
    """
    usage_param = {'app': argv[0]}
    parser.set_usage(usage_text % usage_param)

    # Add arguments
    parser.add_option('-o', '--out', dest='output', action='store',
                      default='out.lua', help='output filename')
    argv = argv[1:]
    return parser.parse_args(argv)

if __name__ == '__main__':
    parser = optparse.OptionParser()
    options, args = parse_command_line(parser, sys.argv)

    main(options.__dict__, args)
