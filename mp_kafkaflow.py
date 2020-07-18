# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from itertools import islice
import datetime
import random
import time
import shutil
import subprocess
import multiprocessing
from configparser import ConfigParser


import oss2
from kafka import KafkaConsumer, KafkaProducer


class KafkaReceiver(multiprocessing.Process):
    def __init__(self, envs, queue):
        """Initialize kafka message receiver process."""
        multiprocessing.Process.__init__(self)

        self.consumer = KafkaConsumer(
            envs['KAFKA_REC_TOPIC'],
            # group_id='test',
            # enable_auto_commit=True,
            # auto_commit_interval_ms=2,
            sasl_mechanism='PLAIN',
            security_protocol='SASL_PLAINTEXT',
            sasl_plain_username=envs['KAFKA_USER'],
            sasl_plain_password=envs['KAFKA_PWD'],
            bootstrap_servers = [envs['KAFKA_BOOTSTRAP_SERVERS']],
        )

        self.queue = queue

    def run(self):
        """Receive message and put it in the queue."""
        # read message
        for msg in self.consumer:
            #line = msg.value.decode('utf-8')
            #line = line.strip()
            #print(line)
            self.queue.put(msg)


def get_envs(env_file='.env'):
    """Get env variables."""
    with open(env_file, 'r') as f:
        env_content = f.readlines()

    env_content = [line.strip().split('=') for line in env_content
                    if '=' in line]
    envs = dict(env_content)
    return envs

def get_report_gallery(config_file='./report_gallery.config'):
    """Read report gallery config file."""
    config = ConfigParser()
    config.read(config_file)

    return config

def conn2aliyun(envs):
    """Connect to aliyun."""
    # aliyun oss auth
    auth = oss2.Auth(envs['ACCESS_KEY_ID'], envs['ACCESS_KEY_SECRET'])
    # get oss bucket
    bucket = oss2.Bucket(
        auth,
        envs['OSS_ENDPOINT_NAME'],
        envs['OSS_BUCKET_NAME'],
    )

    return bucket

def generate_report(user_id, report_type, data_dict=None,
                    bucket=None, base_url=None):
    """Workflow for generating report."""
    report_gallery = get_report_gallery()

    if report_type not in report_gallery:
        print('Error! Not find report type named %s'%(report_type))
        return None

    report_cfg = report_gallery[report_type]

    # dir config
    base_dir = report_cfg['base_dir']
    # init user data dir
    data_dir = os.path.join(base_dir, 'user_data')
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, mode=0o755)
    # init pdf dir
    pdf_dir = os.path.join(base_dir, 'pdfs')
    if not os.path.exists(pdf_dir):
        os.makedirs(pdf_dir, mode=0o755)

    # save input data as json file
    json_file = None
    if isinstance(data_dict, dict):
        json_file = os.path.join(base_dir, '%s_data.json'%(user_id))
        with open(json_file, 'w') as jf:
            jf.write(json.dumps(data_dict)+'\n')

    # run ipynb file
    ipynb_name = report_cfg['entry']
    ipynb_file = os.path.join(base_dir, ipynb_name)
    html_file = os.path.join(base_dir, 'raw_report_%s.html'%(user_id))
    nbconvert_cmd = [
        'jupyter-nbconvert',
        '--execute',
        '--to html',
        '--template=' + os.path.join(base_dir,'templates','report_sample.tpl'),
        ipynb_file,
        '--output ' + html_file,
    ]
    subprocess.run(' '.join(nbconvert_cmd), shell=True,
                   env=dict(os.environ, USERID=user_id))
    # check nbconvert output status
    if not os.path.exists(html_file):
        print('Error in nbconvert stage!')
        return None

    # convert html file to standard format
    if eval(report_cfg['add_heading_number']):
        heading_number_param = '--add_heading_number'
    else:
        heading_number_param = ''
    if eval(report_cfg['include_foreword']):
        foreword_param = '--include_foreword'
    else:
        foreword_param = ''
    if eval(report_cfg['include_article_summary']):
        article_summary_param = \
            '--include_article_summary=' + report_cfg['include_article_summary']
    else:
        article_summary_param = ''


    std_html_file = os.path.join(base_dir, 'std_report_%s.html'%(user_id))
    trans2std_cmd = [
        'trans2std',
        '--in ' + html_file,
        '--out_file ' + std_html_file,
        '--toc_level ' + report_cfg['toc_level'],
        heading_number_param,
        foreword_param,
        article_summary_param,
    ]
    subprocess.run(' '.join(trans2std_cmd), shell=True)
    # check trans2std output status
    if not os.path.exists(std_html_file):
        print('Error in trans2std stage!')
        return None

    # convert html to pdf
    pdf_file = os.path.join(pdf_dir, '%s_report.pdf'%(user_id))
    weasyprint_cmd = ['weasyprint', std_html_file, pdf_file]
    subprocess.run(' '.join(weasyprint_cmd), shell=True)
    # check weasyprint output status
    if not os.path.exists(pdf_file):
        print('Error in weasyprint stage!')
        return None

    # clean
    if isinstance(json_file, str):
        ts = datetime.datetime.strftime(
            datetime.datetime.now(),
            '%Y%m%d%H%M%S',
        )
        targ_file = os.path.join(data_dir, '%s_%s.json'%(user_id, ts))
        shutil.move(json_file, targ_file)
    os.remove(html_file)
    os.remove(std_html_file)

    remote_file = os.path.join(report_cfg['oss_dir'], '%s_report.pdf'%(user_id))
    
    # upload file
    remote_url = upload_file(bucket, base_url, pdf_file, remote_file)
    uploaded_msg = {
        'user_id': user_id,
        urls: {report_type: remote_url},
    }
    print(uploaded_msg)
    
    # if uploaded_msg is not empty, send message
    if uploaded_msg:
        # XXX: add user id to uploaded_msg
        # TODO: kafka_producer can be an instance of new class which
        # wraps send_topic var
        #kafka_producer.send(envs['KAFKA_SEND_TOPIC'], uploaded_msg)
        pass


def upload_file(bucket, base_url, src_file, remote_file):
    """Upload files to aliyun oss.
    Arguments:
        bucket: oss bucket instance.
        base_url: url of oss bucket.
    """
    # kafka message of upload files successfully
    uploaded_msg = {}
    rsp = bucket.put_object_from_file(
        remote_file,
        src_file,
    )
    # if upload successfully
    # XXX: should check file content using etag field (md5)
    if rsp.status==200:
        return 'https://'+base_url+'/'+remote_file
    else:
        print('%s error while uploading file %s'%(rsp.status, src_file))
        return None

def delete_files(bucket, oss_list):
    for oss_file in oss_list:
        bucket.delete_object(oss_file)

def queue_writer(q):
    for i in range(20):
        time.sleep(random.random() * 10)
        message = f'Message {i}'
        q.put(message)


if __name__ == '__main__':
    # config
    worker_num = 2

    # get access key
    envs = get_envs()

    # create data queue for multiprocessing
    data_manager = multiprocessing.Manager()
    queue = data_manager.Queue(5)


    #-- initialize kafka msg receiver process
    #kafka_receiver = KafkaReceiver(envs, queue)
    #kafka_receiver.start()

    # for test
    # Create a writer
    multiprocessing.Process(target=queue_writer, args=(queue,)).start()


    #-- process data
    # initialize kafka producer
    producer = KafkaProducer(
        sasl_mechanism='PLAIN',
        security_protocol='SASL_PLAINTEXT',
        sasl_plain_username=envs['KAFKA_USER'],
        sasl_plain_password=envs['KAFKA_PWD'],
        bootstrap_servers = [envs['KAFKA_BOOTSTRAP_SERVERS']],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
    )

    # connect aliyun
    bucket = conn2aliyun(envs)
    base_url = '.'.join([envs['OSS_BUCKET_NAME'], envs['OSS_ENDPOINT_NAME']])

    # Create multiprocessing pool to process data
    pool = multiprocessing.Pool(worker_num)

    # generate report
    c = 1
    while True:
        if not queue.empty():
            msg = queue.get()
            print(msg)
            # XXX: get report type and user data from kafka message
            report_type = 'sample'
            data_dict = {
                'user_id': 's00'+str(c),
                'var1': 1,
                'var2': 2,
            }
            pool.apply_async(
                generate_report,
                (
                    data_dict['user_id'],
                    report_type,
                ),
                {
                    'data_dict': data_dict,
                    'bucket': bucket,
                    'base_url': base_url,
                },
            )
            c += 1
        else:
            time.sleep(0.01)


    # remove file
    #oss_list = [upload_files[k][1] for k in upload_files]
    #delete_files(bucket, oss_list)
    
    #-- list files
    #for obj in oss2.ObjectIterator(bucket, delimiter='/'):
    #    # list dir
    #    if obj.is_prefix():
    #        print('-'*10)
    #        print(obj.key)
    #    # list files
    #    else:
    #        print(obj.key)

