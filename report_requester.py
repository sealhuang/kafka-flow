# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from configparser import ConfigParser

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


class ReportQuester():
    """Report requester for the Sundial-Report-Stream."""

    def __init__(self, env_name, env_config='./report_requester.config'):
        """Initialize report requester."""
        # read configs
        envs = ConfigParser()
        envs.read(env_config)

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

    def send(self, msgs):
        """Send report messages."""
        # get message list
        msg_list = []
        if isinstance(msgs, dict):
            msg_list.append(dict(msgs))
        elif isinstance(msgs, list):
            msg_list.extend(msgs)

        # normalize messages
        for item in msg_list:
            item['db_id'] = str(item['_id'])
            item['data'] = str(item['data'])
            item['receivedTime'] = str(item['receivedTime'])
            item['reportProcessStatus'] = 'REPORT'
            item.pop('_id')

        # send request
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
                print('Send report request for message %s successfully' % (
                    msg['db_id']))

