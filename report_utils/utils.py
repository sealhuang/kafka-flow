# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
from configparser import ConfigParser
from urllib.parse import quote_plus
import pymongo

def _conn2db(env_name, config_file='env.config'):
    """Connect to mongoDB.
    env_name: choose one from `inner`, `beta`, and `production`
    """
    # check args
    assert env_name in ['inner', 'beta', 'production']

    # read configs
    envs = ConfigParser()
    envs.read(config_file)

    # connect to db
    dbconfig = envs['mongodb']

    if env_name=='inner':
        # inner env
        url_list = dbconfig.get('inner_urls').split(',')
        myclient = pymongo.MongoClient('mongodb://%s' % (','.join(url_list)))
    elif env_name=='beta':
        # beta env
        url_list = dbconfig.get('beta_urls').split(',')
        url = 'mongodb://%s/?replicaSet=%s' % (
            ','.join(url_list),
            dbconfig.get('beta_replica_set_name'),
        )
        myclient = pymongo.MongoClient(url)
    elif env_name=='production':
        user = quote_plus(dbconfig.get('production_user'))
        pwd = quote_plus(dbconfig.get('production_pwd'))
        url_list = dbconfig.get('production_urls').split(',')
        url = 'mongodb://%s:%s@%s/?authSource=%s&replicaSet=%s' % (
            user,
            pwd,
            ','.join(url_list),
            'admin',
            dbconfig.get('production_replica_set_name'),
        )
        #print(url)
        myclient = pymongo.MongoClient(url)
 
    return myclient

def conn2datapool(env_name, config_file='env.config'):
    """Connect to mongoDB.
    env_name: choose one from `inner`, `beta`, and `production`
    """
    # check args
    assert env_name in ['inner', 'beta', 'production']

    # read configs
    envs = ConfigParser()
    envs.read(config_file)

    # connect to db
    dbconfig = envs['mongodb']
    dbname = dbconfig.get('pool_db')

    if env_name=='inner':
        # inner env
        url_list = dbconfig.get('inner_urls').split(',')
        myclient = pymongo.MongoClient('mongodb://%s' % (','.join(url_list)))
    elif env_name=='beta':
        # beta env
        url_list = dbconfig.get('beta_urls').split(',')
        url = 'mongodb://%s/?replicaSet=%s' % (
            ','.join(url_list),
            dbconfig.get('beta_replica_set_name'),
        )
        myclient = pymongo.MongoClient(url)
    elif env_name=='production':
        user = quote_plus(dbconfig.get('production_user'))
        pwd = quote_plus(dbconfig.get('production_pwd'))
        url_list = dbconfig.get('production_urls').split(',')
        url = 'mongodb://%s:%s@%s/?authSource=%s&replicaSet=%s' % (
            user,
            pwd,
            ','.join(url_list),
            'admin',
            dbconfig.get('production_replica_set_name'),
        )
        #print(url)
        myclient = pymongo.MongoClient(url)
 
    return myclient[dbname]

def _get_full_tags(idx, all_tags, full_tags):
    """Get context for each tag recursively."""
    if idx in all_tags:
        full_tags.append(all_tags[idx][0])
        parent_idx = all_tags[idx][1]
        return _get_full_tags(parent_idx, all_tags, full_tags)
    else:
        return full_tags

def _get_question_infos(ttl, db):
    """Get question info from db indexed by question title."""
    # get questions' domain tags first
    tag_col = db['questionTag']
    all_tags = {}
    for item in tag_col.find({'parent': {'$exists': True}}):
        if not item['parent'] is None:
            all_tags[str(item['_id'])] = (item['title'], item['parent'])
    # get all domain tags
    domain_tags = {}
    for idx in all_tags:
        full_tags = _get_full_tags(idx, all_tags, [])
        if full_tags[-1]=='维度':
            full_tags.reverse()
            domain_tags[idx] = '-'.join(full_tags)

    # get question info
    question_col = db['Question']
    # handle composite questions
    title_parts = ttl.split('-')
    title = title_parts[0]
    subpaths = title_parts[1:]
    raw_info = question_col.find_one({'title': title})
    try:
        qinfo = {}
        # get domain tags
        qinfo['domain_'+ttl] = []
        for item in raw_info['tags']:
            if (item.get('subPath', None) is None) or \
                ('-'.join(subpaths) in item['subPath']):
                if str(item['_id']) in domain_tags:
                    qinfo['domain_'+ttl].append(domain_tags[str(item['_id'])])

        # get other properties
        qinfo['attr_'+ttl] = {}
        if not raw_info['difficulty']==0.0:
            qinfo['attr_'+ttl]['difficulty'] = raw_info['difficulty']
        _qunit = raw_info['compositeQunit']
        for sp in subpaths:
            _qunit = _qunit['subQunits'][int(sp)-1]
        if ('extFeatures' in _qunit) and isinstance(_qunit['extFeatures'], dict):
            for k in _qunit['extFeatures']:
                qinfo['attr_'+ttl][k] = _qunit['extFeatures'][k]['value']

        return qinfo 

    except:
        print('Err while fetching info of %s'%(ttl))
        return None

def get_question_infos(question_list, env_name, config_file='env.config'):
    """Get question info from db.
    question_list: a list of question titles
    env_name: choose one from `inner`, `beta`, and `production`
    """
    # connect to db
    dbclient = _conn2db(env_name, config_file=config_file)

    all_info = {}
    for qtitle in question_list:
        qinfo = _get_question_infos(qtitle, dbclient['question'])
        if isinstance(qinfo, dict):
            all_info.update(qinfo)
        else:
            print('Not find info for %s'%(qtitle))
            break

    return all_info

