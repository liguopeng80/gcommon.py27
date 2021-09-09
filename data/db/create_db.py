#!/usr/bin/python
import optparse
import sys

import dbconfig
import imp


# This Trigger is triggered when the starred message is deleted
star_msg_trigger = """
CREATE TRIGGER star_msg_trigger AFTER UPDATE ON chat_message
FOR EACH ROW
    BEGIN
        DELETE FROM favourite_list WHERE activity_content = NEW.id;
    END;
"""

# engine.execute(star_msg_trigger)


def parse_command_line(parser, argv):
    # set usage
    usage_text = """Create database schema.
    %(app)s [-u user] [-p password] [-h host] [-n database name]"""
    usage_param = {'app': argv[0]}

    parser.set_usage(usage_text % usage_param)

    # add arguments
    parser.add_option('-u', '--user', dest='user', action='store',
                      default='admin', help='mysql username')
    parser.add_option('-p', '--password', dest='password', action='store',
                      default='password', help='mysql password')
    parser.add_option('-s', '--server', dest='server', action='store',
                      default='localhost', help='mysql host')
    parser.add_option('-n', '--name', dest='dbname', action='store',
                      default='slim', help='database name')
    parser.add_option('-e', '--encoding', dest='encoding', action='store',
                      default='utf8mb4', help='database encoding')
    parser.add_option('-m', '--model', dest='model', action='store',
                      default=None, help='model path')
    argv = argv[1:]
    return parser.parse_args(argv)


def main():
    parser = optparse.OptionParser()
    options, args = parse_command_line(parser, sys.argv)

    conn_str_template = 'mysql://%(user)s:%(password)s@%(server)s'
    options = options.__dict__
    conn_str = conn_str_template % options
    engine = dbconfig.create_engine(conn_str)

    engine.execute('create database if not exists %(dbname)s character set %(encoding)s' %
                   options)
    engine.execute('use %(dbname)s' % options)

    models = imp.load_source('models', '%(model)s' % options)
    dbconfig.create_database(models._metadata, engine)


if __name__ == '__main__':
    main()
