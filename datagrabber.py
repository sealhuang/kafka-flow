# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import bson
import json
import time
from datetime import datetime
import logging
from configparser import ConfigParser
import pymongo
#from pymongo import UpdateOne, ReplaceOne

from loghandler import TimedRotatingCompressedFileHandler
from utils import conn2db

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

def apply_changes(change, dbclient):
    """Apply data changes to datapool."""
    # config collections in datapool DB
    datapool = dbclient[dbconfig.get('pool_db')]
    usage_stats_col = datapool['examUsageStats']
    # auxiliarytoken collection is used for search assess-token ID based on
    # assess code and the used exam/exam-chain ID.
    auxiliarytoken_col = dbclient[dbconfig.get('exams_db')]['auxiliarytoken']

    ns_db = change['ns']['db']
    ns_col = change['ns']['coll']
    op_type = change['operationType']

    # return vars
    # apply_change_status: noChange, ok, or err
    apply_change_status = 'noChange'
    err_info = ''

    # handle event of creating new token
    if ns_col=='assessToken' and op_type=='insert':
        full_doc = change['fullDocument']
        item = {
            'certificate_type': 'token',
            'token': full_doc['code'],
            'certificate_id': str(full_doc['_id']),
            'usage_count': 0,
            'available_exam_type': full_doc['targetType'],
        }
        if 'assessTokenTagBrief' in full_doc:
            item['project'] = full_doc['assessTokenTagBrief']['title']
        item['available_exam'] = '|'.join([
            ele['title'] for ele in full_doc['targets']
        ])
        if 'name' in full_doc['creator']:
            item['creator'] = full_doc['creator']['name']
        else:
            item['creator'] = str(full_doc['creator']['_id'])

        item['create_time'] = datetime.fromtimestamp(
            full_doc['createTime']/1000
        )
        # TODO: add taker info
        #if 'taker' in change['fullDocument']:
        #    if 'name' in change['fullDocument']['taker']:
        #        item['taker'] = change['fullDocument']['taker']['name']
        #        item['take_time'] = datetime.fromtimestamp(
        #            change['fullDocument']['createTime']/1000
        #        )
 
        # insert into collection
        insert_res = usage_stats_col.insert_one(item)
        if isinstance(insert_res.inserted_id, bson.ObjectId):
            apply_change_status = 'ok'
        else:
            apply_change_status = 'err'
            err_info = 'Err while inserting new token: %s' % (
                str(change['fullDocument'])
            )

    # XXX: handle event of updating taker info of token
    #elif ns_col=='assessToken' and op_type=='update':
    #    if 'taker' in change['updateDescription']['updatedFields']:
    #        certificate_id = str(change['documentkey']['_id'])
    #        taker_info = change['updateDescription']['updatedFields']['taker']
    #        taker_name = taker_info['name']
    #        take_time = datetime.fromtimestamp(
    #            change['updateDescription']['updatedFields']['lUTime']/1000
    #        )
    #        # update db
    #        upres = usage_stats_col.update_one(
    #                {'certificate_id': certificate_id},
    #                {'$set': {'taker': taker_name, 'take_time': take_time}},
    #        )
    #        if upres.modified_count==1:
    #            apply_change_status = 'ok'
    #        else:
    #            apply_change_status = 'err'
    #            err_info = 'Err while updating taker info - %s' % (
    #                str(change['updateDescription']['updatedFields'])
    #            )

    # handle event of creating new whitelist item
    elif ns_col=='whitelistItem' and op_type=='insert':
        item = {
            'certificate_type': 'whitelist',
            'certificate_id': str(change['fullDocument']['_id']),
            'usage_count': 0,
            'available_exam_type': change['fullDocument']['targetType'],
            'available_exam': change['fullDocument']['target']['title'],
            'creator': change['fullDocument']['creator']['name'],
            'create_time': datetime.fromtimestamp(
                change['fullDocument']['createTime']/1000
            ),
            'user_id': str(change['fullDocument']['member']['_id']),
        }

        # insert into collection
        insert_res = usage_stats_col.insert_one(item)
        if isinstance(insert_res.inserted_id, bson.ObjectId):
            apply_change_status = 'ok'
        else:
            apply_change_status = 'err'
            err_info = 'Err while inserting new whitelist item: %s' % (
                str(change['fullDocument'])
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
        elif 'whiteListItemId' in raw_doc:
            # assert the existence of the whitelist in usageStats collection
            certificate_id = raw_doc['whiteListItemId']
            token_info = usage_stats_col.find_one({
                'certificate_id': certificate_id
            })
            if isinstance(token_info, dict):
                token_recorded = True

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
                upfields['district'] = raw_doc['county']
            if 'studyingStatus' in raw_doc:
                if 'school' in raw_doc['studyingStatus']:
                    upfields['school'] = raw_doc['studyingStatus']['school']
                if 'grade' in raw_doc['studyingStatus']:
                    upfields['grade'] = raw_doc['studyingStatus']['grade']
                if 'executiveClass' in raw_doc['studyingStatus']:
                    upfields['class'] = raw_doc['studyingStatus']['executiveClass']

            # update db
            upres = usage_stats_col.update_one(
                {'certificate_id': certificate_id},
                {'$set': upfields},
            )
            if upres.modified_count==1:
                apply_change_status = 'ok'
            else:
                apply_change_status = 'err'
                err_info = 'Err while adding ticket info - %s' % (
                    str(raw_doc)
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
                    upfields['district'] = raw_doc['county']
                if 'studyingStatus' in raw_doc:
                    if 'school' in raw_doc['studyingStatus']:
                        upfields['school'] = raw_doc['studyingStatus']['school']
                    if 'grade' in raw_doc['studyingStatus']:
                        upfields['grade'] = raw_doc['studyingStatus']['grade']
                    if 'executiveClass' in raw_doc['studyingStatus']:
                        upfields['class'] = raw_doc['studyingStatus']['executiveClass']

                # update db
                upres = usage_stats_col.update_one(
                    {'ticket_id': ticket_id},
                    {'$set': upfields},
                )
                if upres.modified_count==1:
                    apply_change_status = 'ok'
                else:
                    apply_change_status = 'err'
                    err_info = 'Err while updating ticket info - %s' % (
                        str(raw_doc)
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
        elif 'whiteListItemId' in raw_doc:
            # assert the existence of the whitelist in usageStats collection
            certificate_id = raw_doc['whiteListItemId']
            token_info = usage_stats_col.find_one({
                'certificate_id': certificate_id
            })
            if isinstance(token_info, dict):
                token_recorded = True

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
                upfields['district'] = raw_doc['county']
            if 'studyingStatus' in raw_doc:
                if 'school' in raw_doc['studyingStatus']:
                    upfields['school'] = raw_doc['studyingStatus']['school']
                if 'grade' in raw_doc['studyingStatus']:
                    upfields['grade'] = raw_doc['studyingStatus']['grade']
                if 'executiveClass' in raw_doc['studyingStatus']:
                    upfields['class'] = raw_doc['studyingStatus']['executiveClass']

            # update db
            upres = usage_stats_col.update_one(
                {'certificate_id': certificate_id},
                {'$set': upfields},
            )
            if upres.modified_count==1:
                apply_change_status = 'ok'
            else:
                apply_change_status = 'err'
                err_info = 'Err while adding ticket info - %s' % (
                    str(raw_doc)
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
                    upfields['district'] = raw_doc['county']
                if 'studyingStatus' in raw_doc:
                    if 'school' in raw_doc['studyingStatus']:
                        upfields['school'] = raw_doc['studyingStatus']['school']
                    if 'grade' in raw_doc['studyingStatus']:
                        upfields['grade'] = raw_doc['studyingStatus']['grade']
                    if 'executiveClass' in raw_doc['studyingStatus']:
                        upfields['class'] = raw_doc['studyingStatus']['executiveClass']

                # update db
                upres = usage_stats_col.update_one(
                    {'ticket_id': ticket_id},
                    {'$set': upfields},
                )
                if upres.modified_count==1:
                    apply_change_status = 'ok'
                else:
                    apply_change_status = 'err'
                    err_info = 'Err while updating ticket info - %s' % (
                        str(raw_doc)
                    )

    return apply_change_status, err_info


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
            watched_db = dbclient[dbconfig.get('exams_db')]

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
                            apply_change_status, err_info = apply_changes(
                                change,
                                dbclient,
                            )
                            if apply_change_status=='err':
                                json_logger.error('"rest":"%s"'%(err_info))
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


