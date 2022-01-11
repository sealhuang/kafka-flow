# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
import time
import multiprocessing
from configparser import ConfigParser

from kafka import KafkaConsumer

from utils import conn2db


# global var: root_dir
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read(os.path.join(ROOT_DIR, 'env.config'))

    # connect to db
    db_config = envs['mongodb']
    dbstatus, dbclient = conn2db(db_config)
    if not dbstatus=='ok':
        print('Err while connecting to db.')
        exit()

    # datapool db config
    pool_db = dbclient[db_config.get('pool_db')]
    res_coll = pool_db[db_config.get('result_collection')]

    #-- initialize kafka message receiver
    kafka_config = envs['kafka']
    consumer = KafkaConsumer(
        kafka_config.get('send_topic'),
        group_id = kafka_config.get('rec_grp'),
        # enable_auto_commit=True,
        # auto_commit_interval_ms=2,
        #api_version = (0, 10),
        sasl_mechanism = kafka_config.get('sasl_mechanism'),
        security_protocol = kafka_config.get('security_protocol'),
        sasl_plain_username = kafka_config.get('user'),
        sasl_plain_password = kafka_config.get('pwd'),
        bootstrap_servers = kafka_config.get('bootstrap_servers').split(','),
        auto_offset_reset = kafka_config.get('auto_offset_rst'),
    )

    for raw_msg in consumer:
        msg = raw_msg.value.decode('utf-8').strip()
        msg = json.loads(msg)
        if 'id' in _msg and _msg['status']=='ok':
            # for Shanghai Yuanbo - subjectSuitabilityPersonal_v1
            if msg['report_type']=='subjectSuitabilityPersonal_v1':
                ticket_id = msg['id']
                # get result_info from db
                res_item = res_coll.find_one({'ticketID': ticket_id})
                if not isinstance(ans_item, dict):
                    print('Not find ticket info of %s from results sheet' % (
                        ticket_id
                    ))
                    continue

                if res_item['project']=='远播-高一选科-高一分科':
                    token = res_item['token']
                    print(token)

            on_duty = True

