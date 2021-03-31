# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import bson
import json
#import datetime
import time
import logging
from configparser import ConfigParser
import pymongo
from pymongo import UpdateOne, ReplaceOne
from urllib.parse import quote_plus

from loghandler import TimedRotatingCompressedFileHandler


# global var: root_dir
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_logger(log_level=logging.DEBUG):
    """Logger initialization."""
    # init log dir
    log_dir = os.path.join(ROOT_DIR, 'log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o755)

    logger = logging.getLogger('logger')
    logger.setLevel(log_level)

    # set json format log file
    handler = TimedRotatingCompressedFileHandler(
        os.path.join(log_dir, 'datagrabber.log'),
        when='W6',
        interval=1,
        backup_count=50,
        encoding='utf-8',
    )
    # set log level
    handler.setLevel(log_level)

    # set json format
    formatter = logging.Formatter(
        '{"@timestamp":"%(asctime)s.%(msecs)03dZ","severity":"%(levelname)s","service":"jupyter-reporter",%(message)s}',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )
    formatter.converter = time.gmtime
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

def conn2db(dbconfig):
    """Connect to mongoDB."""
    if dbconfig.get('user')=='XXX':
        user = ''
        pwd = ''
    else:
        user = dbconfig.get('user')
        pwd = dbconfig.get('pwd')

    user_info = ':'.join([item for item in [user, pwd] if item])
    if user_info:
        user_info = user_info + '@'
    url = 'mongodb://%s%s' % (user_info, dbconfig.get('url'))
    #print(url)
    dbclient = pymongo.MongoClient(url)

    # check if the client is valid
    try:
        dbclient.server_info()
        print('Connect to db successfully.')
        return 'ok', dbclient
    except pymongo.errors.ServerSelectionTimeoutError as err:
        print(err)
        return 'err', dbclient


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read(os.path.join(ROOT_DIR, 'env.config'))

    # initialize logger
    json_logger = get_logger()

    # conn2db
    dbconfig = envs['mongodb']
    dbstatus, dbclient = conn2db(dbconfig)
    # if no connection to db, close the programe
    if dbstatus=='err':
        exit()

    # access real-time data changes in db
    watched_db = dbclient[dbconfig.get('watch_db')]
    try:
        with watched_db.watch() as stream:
            for line in watched_db.watch():
                print(line)
    except pymongo.errors.PyMongoError as err:
        print(err)


