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
#from pymongo import UpdateOne, ReplaceOne

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

def apply_changes(change, dbclient):
    """Apply data changes to datapool."""
    # config collections in datapool DB
    datapool = dbclient[dbconfig.get('pool_db')]
    usage_stats_col = datapool['examUsageStats']
    # auxiliarytoken collection is used for search assess-token ID based on
    # assess code and the used exam/exam-chain ID.
    auxiliarytoken_col = dbclient['examstation']['auxiliarytoken']

    ns_db = change['ns']['db']
    ns_col = change['ns']['coll']
    op_type = change['operationType']

    # apply changes in assessToken collection -- insert new token
    if ns_col=='assessToken' and op_type=='insert':
        item = {
            'certificate_type': 'token',
            'token': change['fullDocument']['code'],
            'certificate_id': str(change['fullDocument']['_id']),
            'usage_count': 0,
            'available_exam_type': change['fullDocument']['targetType'],
        }
        item['available_exam'] = [
            ele['title'] for ele in change['fullDocument']['targets']
        ]
        item['creator'] = change['fullDocument']['creator']['name']
        item['create_time'] = datetime.fromtimestamp(
            change['fullDocument']['createTime']/1000
        )
        # TODO: add taker info
        if 'taker' in change['fullDocument']:
            if 'name' in change['fullDocument']['taker']:
                item['taker'] = change['fullDocument']['taker']['name']
                item['take_time'] = datetime.fromtimestamp(
                    change['fullDocument']['createTime']/1000
                )
        # TODO: add agent info
        #'agent': change['fullDocument']['xxx'],
        
        # XXX: insert into collection
        usage_stats_col.insert_one(item)

    # changes in assessToken collection -- update taker info of token
    elif ns_col=='assessToken' and op_type=='update':
        if 'taker' in change['updateDescription']['updatedFields']:
            certificate_id = str(change['documentkey']['_id'])
            taker_info = change['updateDescription']['updatedFields']['taker']
            taker_name = taker_info['name']
            take_time = datetime.fromtimestamp(
                change['updateDescription']['updatedFields']['lUTime']/1000
            )
            # XXX: update db
            usage_stats_col.update_one(
                    {'certificate_id': certificate_id},
                    {'$set': {'taker': taker_name, 'take_time': take_time}},
            )

    # handle event of exchanging eChain assess ticket
    elif ns_col=='echainAticket' and op_type=='insert':
        raw_doc = change['fullDocument']
        
        # token mode
        token_recorded = False
        if 'assessToken' in raw_doc:
            # find token id
            assess_token = raw_doc['assessToken']
            target_type = 'ExamChain'
            target_id = str(raw_doc['echain']['_id'])
            res = auxiliarytoken_col.find_one({
                'code': assess_token,
                'targetType': target_type,
                'targetId': target_id,
            })
            if isinstance(res, dict):
                certificate_id = res['tokenId']
                # assert the existence of the token in usageStats collection
                token_info = usage_stats_col.find_one({
                    'certificate_id': certificate_id
                })
                if isinstance(token_info, dict):
                    token_recorded = True
                    #certificate_type = 'token'

        # TODO: add whitelist mode
        else:
            pass

        if token_recorded:
            upfields = {
                'ticket_id': str(raw_doc['_id']),
                'user_id': raw_doc['owner'],
                'used_exam': raw_doc['echain']['title'],
            }
            if token_info['usage_count']==0:
                upfields['first_use_time'] = datetime.fromtimestamp(
                    raw_doc['lUTime']/1000
                )
            upfields['last_use_time'] = datetime.fromtimestamp(
                raw_doc['lUTime']/1000
            )
            upfields['usage_count'] = token_info['usage_count'] + 1
            # update user info
            if 'province' in raw_doc:
                upfields['province'] = raw_doc['province']
            if 'city' in raw_doc:
                upfields['city'] = raw_doc['city']
            if 'county' in raw_doc:
                upfields['region'] = raw_doc['county']
            if 'studyingStatus' in raw_doc:
                if 'school' in raw_doc['studyingStatus']:
                    upfields['school'] = raw_doc['studyingStatus']['school']
                if 'grade' in raw_doc['studyingStatus']:
                    upfields['grade'] = raw_doc['studyingStatus']['grade']
                if 'executiveClass' in raw_doc['studyingStatus']:
                    upfields['class'] = raw_doc['studyingStatus']['executiveClass']

            # XXX: update db
            usage_stats_col.update_one(
                    {'certificate_id': certificate_id},
                    {'$set': upfields},
            )
                
    # handle event of modifying user info
    elif ns_col=='echainAticket' and op_type=='update':
        to_be_update = False
        for k in ['province', 'city', 'county', 'studyingStatus']:
            if k in change['updateDescription']['updatedFields']:
                to_be_update = True
        
        if to_be_update:
            # assert the existence of the ticket in usageStats collection
            ticket_id = str(change['documentKey']['_id'])
            res = usage_stats_col.find_one({'ticket_id': ticket_id})
            if isinstance(res, dict):
                # update user info
                upfields = {}
                raw_doc = change['updateDescription']['updatedFields']
                if 'province' in raw_doc:
                    upfields['province'] = raw_doc['province']
                if 'city' in raw_doc:
                    upfields['city'] = raw_doc['city']
                if 'county' in raw_doc:
                    upfields['region'] = raw_doc['county']
                if 'studyingStatus' in raw_doc:
                    if 'school' in raw_doc['studyingStatus']:
                        upfields['school'] = raw_doc['studyingStatus']['school']
                    if 'grade' in raw_doc['studyingStatus']:
                        upfields['grade'] = raw_doc['studyingStatus']['grade']
                    if 'executiveClass' in raw_doc['studyingStatus']:
                        upfields['class'] = raw_doc['studyingStatus']['executiveClass']

                # XXX: update db
                usage_stats_col.update_one(
                    {'ticket_id': ticket_id},
                    {'$set': upfields},
                )

    # handle event of exchanging exam assess ticket
    elif ns_col=='examAticket' and op_type=='insert':
        raw_doc = change['fullDocument']
        
        # token mode
        token_recorded = False
        if 'assessToken' in raw_doc:
            # find token id
            assess_token = raw_doc['assessToken']
            target_type = 'Exam'
            target_id = str(raw_doc['exam']['_id'])
            res = auxiliarytoken_col.find_one({
                'code': assess_token,
                'targetType': target_type,
                'targetId': target_id,
            })
            if isinstance(res, dict):
                certificate_id = res['tokenId']
                # assert the existence of the token in usageStats collection
                token_info = usage_stats_col.find_one({
                    'certificate_id': certificate_id
                })
                if isinstance(token_info, dict):
                    token_recorded = True
                    #certificate_type = 'token'

        # TODO: add whitelist mode
        elif 'assessToken' not in raw_doc and 'echainAticket' not in raw_doc:
            pass

        if token_recorded:
            upfields = {
                'ticket_id': str(raw_doc['_id']),
                'user_id': raw_doc['owner'],
                'used_exam': raw_doc['exam']['title'],
            }
            if token_info['usage_count']==0:
                upfields['first_use_time'] = datetime.fromtimestamp(
                    raw_doc['lUTime']/1000
                )
            upfields['last_use_time'] = datetime.fromtimestamp(
                raw_doc['lUTime']/1000
            )
            upfields['usage_count'] = token_info['usage_count'] + 1
            # update user info
            if 'province' in raw_doc:
                upfields['province'] = raw_doc['province']
            if 'city' in raw_doc:
                upfields['city'] = raw_doc['city']
            if 'county' in raw_doc:
                upfields['region'] = raw_doc['county']
            if 'studyingStatus' in raw_doc:
                if 'school' in raw_doc['studyingStatus']:
                    upfields['school'] = raw_doc['studyingStatus']['school']
                if 'grade' in raw_doc['studyingStatus']:
                    upfields['grade'] = raw_doc['studyingStatus']['grade']
                if 'executiveClass' in raw_doc['studyingStatus']:
                    upfields['class'] = raw_doc['studyingStatus']['executiveClass']

            # XXX: update db
            usage_stats_col.update_one(
                    {'certificate_id': certificate_id},
                    {'$set': upfields},
            )

    # handle event of modifying user info
    elif ns_col=='examAticket' and op_type=='update':
        to_be_update = False
        for k in ['province', 'city', 'county', 'studyingStatus']:
            if k in change['updateDescription']['updatedFields']:
                to_be_update = True
        
        if to_be_update:
            # assert the existence of the ticket in usageStats collection
            ticket_id = str(change['documentKey']['_id'])
            res = usage_stats_col.find_one({'ticket_id': ticket_id})
            if isinstance(res, dict):
                # update user info
                upfields = {}
                raw_doc = change['updateDescription']['updatedFields']
                if 'province' in raw_doc:
                    upfields['province'] = raw_doc['province']
                if 'city' in raw_doc:
                    upfields['city'] = raw_doc['city']
                if 'county' in raw_doc:
                    upfields['region'] = raw_doc['county']
                if 'studyingStatus' in raw_doc:
                    if 'school' in raw_doc['studyingStatus']:
                        upfields['school'] = raw_doc['studyingStatus']['school']
                    if 'grade' in raw_doc['studyingStatus']:
                        upfields['grade'] = raw_doc['studyingStatus']['grade']
                    if 'executiveClass' in raw_doc['studyingStatus']:
                        upfields['class'] = raw_doc['studyingStatus']['executiveClass']

                # XXX: update db
                usage_stats_col.update_one(
                    {'ticket_id': ticket_id},
                    {'$set': upfields},
                )




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
                            apply_changes(change, dbclient)
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


