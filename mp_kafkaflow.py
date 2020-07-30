# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from itertools import islice
import datetime
import random
import time
import logging
import logging.handlers
import shutil
import subprocess
import multiprocessing
from configparser import ConfigParser

import oss2
from kafka import KafkaConsumer, KafkaProducer


class KafkaReceiver(multiprocessing.Process):
    """Message Receiver for the Sundial-Report-Stream."""

    def __init__(self, envs, queue):
        """Initialize kafka message receiver process."""
        multiprocessing.Process.__init__(self)

        self.consumer = KafkaConsumer(
            envs['rec_topic'],
            group_id = envs['rec_grp'],
            # enable_auto_commit=True,
            # auto_commit_interval_ms=2,
            sasl_mechanism = envs['sasl_mechanism'],
            security_protocol = envs['security_protocol'],
            sasl_plain_username = envs['user'],
            sasl_plain_password = envs['pwd'],
            bootstrap_servers = [envs['bootstrap_servers']],
            auto_offset_reset = envs['auto_offset_rst'],
        )

        self.queue = queue

    def run(self):
        """Receive message and put it in the queue."""
        # read message
        for msg in self.consumer:
            line = msg.value.decode('utf-8').strip()
            #print(line)
            self.queue.put(line)

def get_logger(log_level=logging.DEBUG):
    logger = logging.getLogger('logger')

    # set log file size
    handler = logging.handlers.RotatingFileHandler(
        'logs/runinfo.json',
        maxBytes=10*1024*1024,
        backupCount=20,
        encoding='utf-8',
    )

    # set log level
    logger.setLevel(log_level)
    handler.setLevel(log_level)

    # set log format
    #formatter = logging.Formatter('[%(asctime)s] %(levelname)s %(message)s')
    formatter = logging.Formatter(
        '{"@timestamp":"%(asctime)s.%(msecs)03dZ","severity":"%(levelname)s","service":"jupyter-reporter",%(message)s}',
        datefmt='%Y-%m-%dT%H:%M:%S')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

def get_report_gallery(config_file='./report_gallery.config'):
    """Read report gallery config file."""
    config = ConfigParser()
    config.read(config_file)

    return config

def conn2aliyun(envs):
    """Connect to aliyun."""
    # aliyun oss auth
    auth = oss2.Auth(envs['access_id'], envs['access_secret'])
    # get oss bucket
    bucket = oss2.Bucket(
        auth,
        envs['oss_endpoint_name'],
        envs['oss_bucket_name'],
    )

    return bucket

def generate_report(user_id, report_type, out_queue, data_dict=None,
                    bucket=None, base_url=None):
    """Workflow for generating report."""
    # init return message
    uploaded_msg = {
        'id': user_id,
        'report_type': report_type,
        'status': 'ok',
    }

    report_gallery = get_report_gallery()

    if report_type not in report_gallery:
        uploaded_msg['status'] = 'error'
        uploaded_msg['detail'] = 'Not find report type.'
        out_queue.put(json.dumps(uploaded_msg))
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
    ret = subprocess.run(
        ' '.join(nbconvert_cmd),
        shell = True,
        env = dict(os.environ, USERID=user_id),
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        encoding = 'utf-8',
    )
    # check nbconvert output status
    if not ret.returncode==0:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ret.args
        uploaded_msg['stderr'] = ret.stderr
        out_queue.put(json.dumps(uploaded_msg))
        #print('Error in nbconvert stage!')
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
    ret = subprocess.run(' '.join(trans2std_cmd), shell=True)
    # check trans2std output status
    if not ret.returncode==0:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ret.args
        uploaded_msg['stderr'] = ret.stderr
        out_queue.put(json.dumps(uploaded_msg))
        #print('Error in trans2std stage!')
        return None

    # convert html to pdf
    ts = datetime.datetime.strftime(
        datetime.datetime.now(),
        '%Y%m%d%H%M%S',
    )
    pdf_filename = 'report_%s_%s.pdf'%(user_id, ts)
    pdf_file = os.path.join(pdf_dir, pdf_filename)
    weasyprint_cmd = ['weasyprint', std_html_file, pdf_file]
    ret = subprocess.run(' '.join(weasyprint_cmd), shell=True)
    # check weasyprint output status
    if not ret.returncode==0:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ret.args
        uploaded_msg['stderr'] = ret.stderr
        out_queue.put(json.dumps(uploaded_msg))
        #print('Error in weasyprint stage!')
        return None

    # clean
    if isinstance(json_file, str):
        targ_file = os.path.join(data_dir, '%s_%s.json'%(user_id, ts))
        shutil.move(json_file, targ_file)
    os.remove(html_file)
    os.remove(std_html_file)

    remote_file = os.path.join(report_cfg['oss_dir'], pdf_filename)
    
    # upload file
    remote_url = upload_file(bucket, base_url, pdf_file, remote_file)
    if remote_url:
        uploaded_msg['status'] = 'ok'
        uploaded_msg['urls'] = {report_type: remote_url}
        out_queue.put(json.dumps(uploaded_msg))
    else:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = 'Uploads to oss.'
        uploaded_msg['stderr'] = 'Falied to upload pdf file.'
        out_queue.put(json.dumps(uploaded_msg))

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

def queue_writer(q):
    """For test."""
    for i in range(10):
        time.sleep(random.random() * 10)
        flag = random.randint(1, 51)
        if flag>28:
            msg = {
                'reportType': 'mathDiagnosisK8_v1',
                'data': '',
            }
        else:
            msg = {
                'reportType': 'mathDiagnosisK8_v1',
                'data': {
                    'ticketID': '00'+str(i),
                    'var1': 1,
                    'var2': 2,
                },
            }
        q.put(json.dumps(msg))


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read('./env.config')

    # create data queue for multiprocessing
    data_manager = multiprocessing.Manager()
    in_queue = data_manager.Queue(int(envs['general']['in_queue_size']))
    out_queue = data_manager.Queue(int(envs['general']['out_queue_size']))

    kafka_sender = KafkaProducer(
        sasl_mechanism = envs['kafka']['sasl_mechanism'],
        security_protocol = envs['kafka']['security_protocol'],
        sasl_plain_username = envs['kafka']['user'],
        sasl_plain_password = envs['kafka']['pwd'],
        bootstrap_servers = [envs['kafka']['bootstrap_servers']],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
        retries = 5,
    )
    #-- initialize kafka message receiver process
    kafka_receiver = KafkaReceiver(envs['kafka'], in_queue)
    kafka_receiver.start()

    #XXX for test: Create a message writer
    #multiprocessing.Process(target=queue_writer, args=(in_queue,)).start()

    # connect to aliyun oss
    bucket = conn2aliyun(envs['aliyun'])
    base_url = '.'.join([
        envs['aliyun']['oss_bucket_name'],
        envs['aliyun']['oss_endpoint_name'],
    ])

    # Create multiprocessing pool to process data
    pool = multiprocessing.Pool(int(envs['general']['mp_worker_num']))

    # logging
    logger = get_logger()

    # generate report
    while True:
        if not in_queue.empty():
            raw_msg = in_queue.get()
            msg = json.loads(raw_msg)
            print(msg)
            # check the data validation
            if not msg['data']:
                logger.info('"rest": "No data found in %s"'%(str(msg)))
                #print('Not find data in message.')
                continue
            data_dict = eval(msg['data'])
            #data_dict = msg['data']
            pool.apply_async(
                generate_report,
                (
                    data_dict['ticketID'],
                    msg['reportType'],
                    out_queue,
                ),
                {
                    'data_dict': data_dict,
                    'bucket': bucket,
                    'base_url': base_url,
                },
            )
        elif not out_queue.empty():
            msg = out_queue.get()
            msg = json.loads(msg)
            if msg['status']=='ok':
                try:
                    future = kafka_sender.send(envs['kafka']['send_topic'], msg)
                    future.get(timeout=10)
                except Exception as e:
                    #print('Error!')
                    #print(msg)
                    #print(e)
                    logger.error(
                        '"rest":"Error while sending kafka message - %s"'%(str(msg)),
                        exc_info=True,
                    )
            else:
                #print(msg)
                logger.error(
                    '"rest":"Error while printing","args":"%s"' %
                    (msg['args'].replace('\n', ';').replace('"', "'")),
                )
        else:
            time.sleep(0.01)


