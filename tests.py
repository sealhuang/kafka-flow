# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from configparser import ConfigParser

from kafka import KafkaConsumer, KafkaProducer


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read('./env.config')

    kafka_sender = KafkaProducer(
        sasl_mechanism = envs['kafka']['sasl_mechanism'],
        security_protocol = envs['kafka']['security_protocol'],
        sasl_plain_username = envs['kafka']['user'],
        sasl_plain_password = envs['kafka']['pwd'],
        bootstrap_servers = [envs['kafka']['bootstrap_servers']],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
        retries = 5,
    )

    msg = {'message': 'test'}

    try:
        future = kafka_sender.send(envs['kafka']['send_topic'], msg)
        future.get(timeout=10)
    except Exception as e:
        print('Error!')
        print(msg)
        print(e)

