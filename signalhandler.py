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


class KafkaReceiver(multiprocessing.Process):
    """Message Receiver for the Sundial-Report-Stream."""

    def __init__(self, envs, queue, max_len):
        """Initialize kafka message receiver process."""
        multiprocessing.Process.__init__(self)

        self.consumer = KafkaConsumer(
            envs.get('send_topic'),
            group_id = envs.get('rec_grp'),
            # enable_auto_commit=True,
            # auto_commit_interval_ms=2,
            #api_version = (0, 10),
            sasl_mechanism = envs.get('sasl_mechanism'),
            security_protocol = envs.get('security_protocol'),
            sasl_plain_username = envs.get('user'),
            sasl_plain_password = envs.get('pwd'),
            bootstrap_servers = envs.get('bootstrap_servers').split(','),
            auto_offset_reset = envs.get('auto_offset_rst'),
        )

        self.queue = queue
        self.queue_max_size = max_len

    def run(self):
        """Receive message and put it in the queue."""
        msg_list = []
        while True:
            is_working = False

            # retrive messages if there is less in cache
            if len(msg_list) < self.queue_max_size:
                try:
                    msg_pack = self.consumer.poll(
                        timeout_ms=500,
                        max_records = 2*self.queue_max_size - len(msg_list),
                    )
                    self.consumer.commit()
                except:
                    print('Err while retrive kafka messages!')
                else:
                    for tp, messages in msg_pack.items():
                        for msg in messages:
                            msg_list.append(msg.value.decode('utf-8').strip())
                            is_working = True

            # append message into queue
            if len(msg_list):
                line = msg_list[0]
                #print(line)
                try:
                    _msg = json.loads(line)
                    assert 'id' in _msg and _msg['status']=='ok'
                    if not self.queue.full():
                        self.queue.put(_msg)
                        msg_list.pop(0)
                except:
                    print('Receive invalid message')
                    print(line)
                    msg_list.pop(0)

                is_working = True

            if not is_working:
                time.sleep(0.5)




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

    # create data queue
    data_manager = multiprocessing.Manager()
    in_queue_size = 50
    in_queue = data_manager.Queue(in_queue_size)

    #-- initialize kafka message receiver process
    kafka_receiver = KafkaReceiver(
        envs['kafka'],
        in_queue,
        in_queue_size,
    )
    kafka_receiver.start()

    # handling report results
    while True:
        on_duty = False

        # process new message
        if not in_queue.empty():
            msg = in_queue.get()
            #print(msg)

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

        if not on_duty:
            time.sleep(0.01)

