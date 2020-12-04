# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from configparser import ConfigParser

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
import oss2


class ReportQuester():
    """Report requester for the Sundial-Report-Stream."""

    def __init__(self, env_name, env_cfg_file='./report_requester.config'):
        """Initialize report requester."""
        # read configs
        envs = ConfigParser()
        envs.read(env_cfg_file)

        if env_name=='test':
            bootstrap_servers = [
                envs.get('kafka', 'test_bootstrap_servers')
            ]
        elif env_name=='production':
            bootstrap_servers = [
                envs.get('kafka', 'production_bootstrap_servers')
            ]
        else:
            print('Invalid `env_name`, possible input: test or production.')
            return

        self.send_topic = envs.get('kafka', 'send_topic')

        self.kafka_sender = KafkaProducer(
            sasl_mechanism = envs.get('kafka', 'sasl_mechanism'),
            security_protocol = envs.get('kafka', 'security_protocol'),
            sasl_plain_username = envs.get('kafka', 'user'),
            sasl_plain_password = envs.get('kafka', 'pwd'),
            bootstrap_servers = bootstrap_servers,
            value_serializer = lambda v: json.dumps(v).encode('utf-8'),
            retries = 5,
        )

    def send(self, msgs, report_type=None, data_objective=None, priority=None):
        """Send report messages."""
        # get message list
        msg_list = []
        if isinstance(msgs, dict):
            msg_list.append(dict(msgs))
        elif isinstance(msgs, list):
            msg_list.extend(msgs)
        msg_list = [dict(item) for item in msg_list]

        # normalize messages
        for item in msg_list:
            item['db_id'] = str(item['_id'])
            item['data'] = str(item['data'])
            item['receivedTime'] = str(item['receivedTime'])
            if data_objective:
                item['dataObjective'] = data_objective
            else:
                item['dataObjective'] = 'REPORT'
            if report_type:
                item['reportType'] = report_type
            if priority and priority in ['high', 'low']:
                item['priority'] = priority
            item.pop('_id')
            item.pop('receivedTimeFormatted')

        # send request
        c = 0
        for msg in msg_list:
            try:
                future = self.kafka_sender.send(self.send_topic, msg)
                record_metadata = future.get(timeout=30)
                assert future.succeeded()
            except KafkaTimeoutError as kte:
                print('Timeout while sending message which id is %s' % (
                    msg['db_id']))
            except KafkaError as ke:
                print('KafkaError while sending message which id is %s' % (
                    msg['db_id']))
            except:
                print('Error while sending message which id is %s' % (
                    msg['db_id']))
            else:
                #print('Send report request for message %s successfully' % (
                #    msg['db_id']))
                c += 1
        print('Send %s report requests successfully' % (c))



def export_reports(msgs, name_fields, export_dir,
                   env_cfg_file='./report_requester.config'):
    # check input
    if not isinstance(name_fields, list):
        print('Error: `name_fields`should be a list!')
        return

    # get message list
    msg_list = []

    if isinstance(msgs, dict):
        if ('dataObjective' in msgs) and \
           (msgs['dataObjective']=='REPORT') and \
           ('report_url' in msgs):
            msg_list.append(dict(msgs))
        else:
            print('Invalid message for exporting report.')
            print(msgs)
            return

    elif isinstance(msgs, list):
        for item in msgs:
            if (isinstance(item, dict)) and \
               ('dataObjective' in item) and \
               (item['dataObjective']=='REPORT') and \
               ('report_url' in item):
                msg_list.append(dict(item))
            else:
                print('Invalid message for exporting report.')
                print(item)
                return

    # check name fields
    for item in msg_list:
        for k in name_fields:
            if not k in item:
                print('Data error!')
                print('field %s does not exist in data'%(k))
                print(item)
                return

    # read config
    envs = ConfigParser()
    envs.read(env_cfg_file)

    # aliyun oss auth
    auth = oss2.Auth(
        envs.get('aliyun', 'access_id'),
        envs.get('aliyun', 'access_secret'),
    )
    # get oss bucket
    bucket = oss2.Bucket(
        auth,
        envs.get('aliyun', 'oss_endpoint_name'),
        envs.get('aliyun', 'oss_bucket_name'),
    )
    # get oss base url
    dummy_base_url = envs.get('aliyun', 'dummy_oss_url')

    # rename pdfs
    for item in msg_list:
        old_url = item['report_url']
        old_url = old_url[old_url.index(dummy_base_url):]
        old_addr = '/'.join(old_url.split('/')[1:])

        # generate new file name
        old_pdf = old_addr.split('/')[-1]
        time_tag = old_pdf.split('.')[0].split('_')[-1]
        new_pdf_fields = [item[k] for k in name_fields]
        new_pdf_fields = [ele for ele in new_pdf_fields if ele]
        new_pdf_fields.append(time_tag)
        new_addr = '/'.join([export_dir, '_'.join(new_pdf_fields)+'.pdf'])

        # copy file
        rsp = bucket.copy_object(
            envs.get('aliyun', 'oss_bucket_name'),
            old_addr,
            new_addr,
        )
        if rsp.status==200:
            print('Export and rename - ok')
        else:
            print('Error occurred')
            print(item)
            break

