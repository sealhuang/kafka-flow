# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
import datetime
import random
import time
import gzip
import logging
import shutil
import subprocess
import multiprocessing
from configparser import ConfigParser
from logging.handlers import TimedRotatingFileHandler
import pymongo
from pymongo import UpdateOne, ReplaceOne
from urllib.parse import quote_plus
import bson

import oss2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from weasyprint import HTML


# global var: root_dir
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))



if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read(os.path.join(ROOT_DIR, 'env.config'))

    # create data queue for multiprocessing
    data_manager = multiprocessing.Manager()
    fast_in_queue = data_manager.Queue(envs.getint('general', 'in_queue_size'))
    in_queue = data_manager.Queue(envs.getint('general', 'in_queue_size'))
    out_queue = data_manager.Queue(envs.getint('general', 'out_queue_size'))
    cache_queue = data_manager.Queue(envs.getint('general', 'cache_queue_size'))

    kafka_sender = KafkaProducer(
        sasl_mechanism = envs.get('kafka', 'sasl_mechanism'),
        security_protocol = envs.get('kafka', 'security_protocol'),
        sasl_plain_username = envs.get('kafka', 'user'),
        sasl_plain_password = envs.get('kafka', 'pwd'),
        bootstrap_servers = [envs.get('kafka', 'bootstrap_servers')],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
        retries = 5,
    )
    #-- initialize kafka message receiver process
    kafka_receiver = KafkaReceiver(envs['kafka'], fast_in_queue, in_queue)
    # TODO: remove this part later
    # terminate the kafka-receiver when the main function exists
    #kafka_receiver.daemon = True
    kafka_receiver.start()

    #XXX for test: Create a message writer
    #multiprocessing.Process(target=queue_writer, args=(in_queue,)).start()

    # connect to aliyun oss
    bucket = conn2aliyun(envs['aliyun'])
    base_url = '.'.join([
        envs.get('aliyun', 'oss_bucket_name'),
        envs.get('aliyun', 'oss_endpoint_name'),
    ])
    dummy_base_url = envs.get('aliyun', 'dummy_oss_url')

    # Create multiprocessing pool to process data
    max_worker_num = envs.getint('general', 'mp_worker_num')
    pool = multiprocessing.Pool(max_worker_num)

    # logging
    json_logger = get_json_logger()

    # generate report
    cache_max_time = envs.getint('general', 'cache_max_time')
    last_time = time.time()
    while True:
        on_duty = False

        # save messages to db or file
        if (not cache_queue.empty()) and \
           (cache_queue.full() or ((time.time()-last_time) > cache_max_time)):
            msg_list = []
            while not cache_queue.empty():
                msg_list.append(cache_queue.get())
            save_ret = save_msgs(msg_list, envs['mongodb'])
            if save_ret=='msg2db_ok':
                json_logger.info('"rest":"Save msgs to db successfully"')
            elif save_ret=='msg2file_ok':
                json_logger.info('"rest":"Save msgs to file successfully"')
            elif save_ret=='msg2db_err':
                json_logger.error('"rest":"Error while save msgs to db"')
            elif save_ret=='msg2file_err':
                json_logger.error('"rest":"Error while save msgs to file"')

            last_time = time.time()
            on_duty = True

        # handle process results
        if not out_queue.empty():
            msg = out_queue.get()
            callback_flag = msg.pop('callback')
            if not callback_flag:
                continue
            #normalize_ret_dict(msg)
            #print(msg)
            if msg['status']=='ok':
                try:
                    future = kafka_sender.send(
                        envs.get('kafka', 'send_topic'),
                        msg,
                    )
                    record_metadata = future.get(timeout=30)
                    assert future.succeeded()
                except KafkaTimeoutError as kte:
                    json_logger.error(
                        '"rest":"Timeout while sending message - %s"'%(str(msg)),
                        exc_info=True,
                    )
                except KafkaError as ke:
                    json_logger.error(
                        '"rest":"KafkaError while sending message - %s"'%(str(msg)),
                        exc_info=True,
                    )
                except:
                    #print('Error!')
                    #print(msg)
                    json_logger.error(
                        '"rest":"Exception while sending message - %s"'%(str(msg)),
                        exc_info=True,
                    )
                else:
                    json_logger.info(
                        '"rest":"Generate report successfully - %s"'%(str(msg)),
                    )

            else:
                #print(msg)
                json_logger.error(
                    '"rest":"Error while printing","args":"%s"' %
                    (msg['args'].replace('\n', ';').replace('"', "'")),
                )
                print('*'*20)
                print('Error while printing!\nArgs:')
                print(msg['args'])
                print('Err:')
                print(msg['stderr'])

            on_duty = True

        # process new message
        if (pool._taskqueue.qsize() <= (3*max_worker_num)) and \
           (not in_queue.empty() or not fast_in_queue.empty()):
            if not fast_in_queue.empty():
                raw_msg = fast_in_queue.get()
            else:
                raw_msg = in_queue.get()
            msg = json.loads(raw_msg)
            if 'priority' in msg:
                msg.pop('priority')
            #print(msg)

            # validate received data
            if not msg['data']:
                json_logger.info('"rest":"No data found in %s"'%(str(msg)))
                #print('Not find data in message.')
                continue
            try:
                msg['data'] = eval(msg['data'])
            except:
                json_logger.error('"rest":"Get invalid data - %s"'%(str(msg)))
                continue

            # get report type and the data objectives
            if not msg['reportType']:
                json_logger.info(
                    '"rest": "Get unrelated message - %s"'%(str(msg))
                )
                continue

            if '|' in msg['reportType']:
                report_type, data_purpose = msg['reportType'].split('|')
            elif 'dataObjective' in msg:
                report_type = msg['reportType']
                data_purpose = msg['dataObjective']
            else:
                report_type = msg['reportType']
                data_purpose = 'REPORT'

            # if we get a test message
            if report_type=='test':
                json_logger.info('"rest":"Get test message - %s"'%(str(msg)))
                continue

            # normalize message structure
            msg['reportType'] = report_type
            msg['dataObjective'] = data_purpose
            msg['reportProcessStatus'] = 'OK'
            if 'receivedTime' not in msg:
                ts = datetime.datetime.strftime(
                    datetime.datetime.now(),
                    '%Y%m%d%H%M%S',
                )
                msg['receivedTime'] = ts
            # key `dataType` is used for choosing which collection the
            # message should be saved
            msg['dataType'] = 'raw_msgs'

            # if the objective of the message is `store`
            if data_purpose=='STORE':
                cache_queue.put(msg)
                continue

            pool.apply_async(
                generate_report,
                (
                    msg,
                    out_queue,
                    cache_queue,
                    bucket,
                    base_url,
                    dummy_base_url,
                ),
            )

            on_duty = True

        if not on_duty:
            time.sleep(0.01)

