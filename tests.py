# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from configparser import ConfigParser

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


def kafkaSender(envs):
    kafka_sender = KafkaProducer(
        sasl_mechanism = envs['sasl_mechanism'],
        security_protocol = envs['security_protocol'],
        sasl_plain_username = envs['user'],
        sasl_plain_password = envs['pwd'],
        bootstrap_servers = [envs['bootstrap_servers']],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
        retries = 5,
    )
    return kafka_sender


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read('./env.config')

    kafka_sender = kafkaSender(envs['kafka'])

    msg = {'id': 'test', 'urls': {'a': 'b'}}

    try:
        future = kafka_sender.send(envs['kafka']['send_topic'], msg)
        record_metadata = future.get(timeout=10)
        assert future.succeeded()
        print('Successfully')
    except KafkaTimeoutError as kte:
        print('Error!')
        print(msg)
        print(e)
    except KafkaError as ke:
        print('Error!')
        print(msg)
        print(e)
    except Exception as e:
        print('Error!')
        print(msg)
        print(e)

