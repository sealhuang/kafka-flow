# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import bson
import json
import time
from datetime import datetime
import logging
from configparser import ConfigParser
from urllib.parse import quote_plus
import pymongo
from pymongo import UpdateOne, ReplaceOne

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
        '{"@timestamp":"%(asctime)s.%(msecs)03dZ","severity":"%(levelname)s","service":"datagrabber",%(message)s}',
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

    user_info = ':'.join([quote_plus(item) for item in [user, pwd] if item])
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

def apply_changes(change, datapool):
    """Apply data changes to datapool."""
    # config collections in datapool DB
    uasge_stats_col = datapool['examUsageStats']

    ns_db = change['ns']['db']
    ns_col = change['ns']['coll']
    op_type = change['operationType']

    # apply changes in assessToken collection -- insert new token
    if ns_col=='assessToken' and op_type=='insert':
        item = {
            'certificate_type': 'token',
            'token': change['fullDocument']['code'],
            'certificate_id': change['fullDocument']['_id'],
            'usage_count': 0,
            # TODO: add agent
            #'agent': change['fullDocument']['xxx'],
            'available_exam_type': change['fullDocument']['targetType'],
        }
        # XXX: to be confirmed
        item['available_exam_id'] = [
            ele['_id'] for ele in change['fullDocument']['targets']
        ]
        item['available_exam_name'] = [
            ele['title'] for ele in change['fullDocument']['targets']
        ]
        item['creator'] = change['fullDocument']['creator']['name']
        item['create_time'] = datetime.fromtimestamp(
            change['fullDocument']['createTime']/1000
        )
        
        # TODO: insert into collection

    # changes in assessToken collection -- update token info
    elif ns_col=='assessToken' and op_type=='update':
        certificate_id = change['documentkey']['_id']









if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read(os.path.join(ROOT_DIR, 'env.config'))
    # load db config
    dbconfig = envs['mongodb']

    # initialize logger
    json_logger = get_logger()

    # keep the db connection alive
    dbstatus = 'err'
    resume_token = None
    while dbstatus=='err':
        print('Connect to db ...')
        dbstatus, dbclient = conn2db(dbconfig)

        if dbstatus=='ok':
            
            json_logger.info('"rest":"Connect to db successfully"')

            # watch data changes in db, and focus the specified collections
            watched_db = dbclient[dbconfig.get('watch_db')]
            datapool = dbclient[dbconfig.get('pool_db')]

            pipeline = [
                {'$match': {
                    'ns.coll': {
                        '$in': [
                            'assessToken',
                            'whitelistItem',
                            'examAticket',
                            'echainAticket',
                        ]
                    }
                }},
            ]
            try:
                with watched_db.watch(pipeline=pipeline,
                        resume_after=resume_token) as stream:

                    json_logger.info('"rest":"Start to watch data changes"')

                    while stream.alive:
                        change = stream.try_next()
                        # get resume token, the resume token may be updated
                        # even when no changes are returned
                        resume_token = stream.resume_token

                        # if get changes
                        if change is not None:
                            print(change)
                            apply_changes(change, datapool)
                            continue

                        # if there are no recent changes, sleep for a while
                        time.sleep(5)

            except pymongo.errors.PyMongoError as err:
                print(err)
                json_logger.error(
                    '"rest":"Error while get changes - %s"' % (str(err)),
                    exc_info=True,
                )
                dbstatus = 'err'
            else:
                print("The ChangeStreams' cursor is broken.")
                json_logger.error('"rest":"The ChangeStreams cursor is broken"')
                dbstatus = 'err'


