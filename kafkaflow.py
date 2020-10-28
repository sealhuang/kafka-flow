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
from urllib.parse import quote_plus
import bson

import oss2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


class TimedRotatingCompressedFileHandler(TimedRotatingFileHandler):
    """Extended version of TimedRotatingFileHandler that compress 
    logs on rollover.
    """
    def __init__(self, filename='', when='W6', interval=1,
                 backup_count=50, encoding='utf-8'):
        super(TimedRotatingCompressedFileHandler, self).__init__(
            filename=filename,
            when=when,
            interval=int(interval),
            backupCount=int(backup_count),
            encoding=encoding,
        )

    def doRollover(self):
        super(TimedRotatingCompressedFileHandler, self).doRollover()
        log_dir = os.path.dirname(self.baseFilename)
        to_compress = [
            os.path.join(log_dir, f) for f in os.listdir(log_dir)
            if f.startswith(
                os.path.basename(os.path.splitext(self.baseFilename)[0])
            ) and not f.endswith(('.gz', '.json'))
        ]
        for f in to_compress:
            if os.path.exists(f):
                with open(f, 'rb') as _old, gzip.open(f+'.gz', 'wb') as _new:
                    shutil.copyfileobj(_old, _new)
                os.remove(f)


class KafkaReceiver(multiprocessing.Process):
    """Message Receiver for the Sundial-Report-Stream."""

    def __init__(self, envs, queue):
        """Initialize kafka message receiver process."""
        multiprocessing.Process.__init__(self)

        self.consumer = KafkaConsumer(
            envs.get('rec_topic'),
            group_id = envs.get('rec_grp'),
            # enable_auto_commit=True,
            # auto_commit_interval_ms=2,
            sasl_mechanism = envs.get('sasl_mechanism'),
            security_protocol = envs.get('security_protocol'),
            sasl_plain_username = envs.get('user'),
            sasl_plain_password = envs.get('pwd'),
            bootstrap_servers = [envs.get('bootstrap_servers')],
            auto_offset_reset = envs.get('auto_offset_rst'),
        )

        self.queue = queue

    def run(self):
        """Receive message and put it in the queue."""
        # read message
        for msg in self.consumer:
            line = msg.value.decode('utf-8').strip()
            #print(line)
            self.queue.put(line)


def get_json_logger(log_level=logging.DEBUG):
    """Logger initialization."""
    # init log dir
    log_dir = os.path.join(os.path.curdir, 'log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o755)

    logger = logging.getLogger('logger')
    logger.setLevel(log_level)

    # set json format log file
    handler = TimedRotatingCompressedFileHandler(
        os.path.join(log_dir, 'log.json'),
        when='W6',
        interval=1,
        backup_count=50,
        encoding='utf-8',
    )
    # set log level
    handler.setLevel(log_level)

    # set json format
    formatter = logging.Formatter(
        '{"@timestamp":"%(asctime)s.%(msecs)03dZ","severity":"%(levelname)s","service":"jupyter-reporter",%(message)s}',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )
    formatter.converter = time.gmtime
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
    auth = oss2.Auth(envs.get('access_id'), envs.get('access_secret'))
    # get oss bucket
    bucket = oss2.Bucket(
        auth,
        envs.get('oss_endpoint_name'),
        envs.get('oss_bucket_name'),
    )

    return bucket

def generate_report(msg, out_queue, cache_queue,
                    bucket=None, base_url=None, dummy_base_url=None):
    """Workflow for generating report."""
    # local vars init
    data_dict = msg['data']
    user_id = data_dict['ticketID']
    report_type = msg['reportType']

    # init return message
    uploaded_msg = {
        'id': user_id,
        'report_type': report_type,
        'status': 'ok',
    }

    report_gallery = get_report_gallery()

    if report_type not in report_gallery:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ''
        uploaded_msg['stderr'] = ''
        uploaded_msg['detail'] = 'Not find report type.'
        out_queue.put(json.dumps(uploaded_msg))
        # add message to cache
        msg['reportProcessStatus'] = 'ERR'
        cache_queue.put(msg)
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
    # def img dir
    img_dir = os.path.join(base_dir, 'imgs')

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
        '--ExecutePreprocessor.timeout=60',
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
        # add message to cache
        msg['reportProcessStatus'] = 'ERR'
        cache_queue.put(msg)
        #print('Error in nbconvert stage!')
        # clean img dir
        user_img_dir = os.path.join(img_dir, user_id)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
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
        # add message to cache
        msg['reportProcessStatus'] = 'ERR'
        cache_queue.put(msg)
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
        # add message to cache
        msg['reportProcessStatus'] = 'ERR'
        cache_queue.put(msg)
        #print('Error in weasyprint stage!')
        # clean img dir
        user_img_dir = os.path.join(img_dir, user_id)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
        return None

    # clean
    if isinstance(json_file, str):
        targ_file = os.path.join(data_dir, '%s_%s.json'%(user_id, ts))
        shutil.move(json_file, targ_file)
    os.remove(html_file)
    os.remove(std_html_file)
    user_img_dir = os.path.join(img_dir, user_id)
    if os.path.exists(user_img_dir):
        shutil.rmtree(user_img_dir)

    remote_file = os.path.join(report_cfg['oss_dir'], pdf_filename)
    
    # upload file
    remote_url = upload_file(bucket, base_url, pdf_file, remote_file)

    if remote_url:
        uploaded_msg['status'] = 'ok'
        dummy_remote_url = 'https://'+dummy_base_url+'/'+remote_file
        uploaded_msg['urls'] = {report_type: dummy_remote_url}
        out_queue.put(json.dumps(uploaded_msg))
        # add message to cache
        cache_queue.put(msg)
    else:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = 'Uploads to oss.'
        uploaded_msg['stderr'] = 'Falied to upload pdf file.'
        out_queue.put(json.dumps(uploaded_msg))
        msg['reportProcessStatus'] = 'ERR2OSS'
        cache_queue.put(msg)

def upload_file(bucket, base_url, src_file, remote_file):
    """Upload files to aliyun oss.
    Arguments:
        bucket: oss bucket instance.
        base_url: url of oss bucket.
    """
    # kafka message of upload files successfully
    uploaded_msg = {}
    try:
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
    except:
        print('%s error while uploading file %s'%(rsp.status, src_file))
        return None

def save_msgs(msg_list, db_config):
    """Save message."""
    insert2db_err = False

    if db_config.getboolean('msg2db'):
        try:
            # normalize data
            nmsgs = [dict(msg) for msg in msg_list]
            for item in nmsgs:
                item['receivedTime'] = int(item['receivedTime'])

            # connect to db
            uri = "mongodb://%s:%s@%s" % (
                quote_plus(db_config.get('db_user')),
                quote_plus(db_config.get('db_pwd')),
                db_config.get('db_url'),
            )
            myclient = pymongo.MongoClient(
                host=uri,
                port=db_config.getint('db_port'),
            )

            # locate db and collection
            db = myclient[db_config.get('db_name')]
            msgs_col = db[db_config.get('collection_name')]
            ret = msgs_col.insert_many(nmsgs)
            assert len(ret.inserted_ids)==len(nmsgs)
        except:
            insert2db_err = True
        else:
            return 'msg2db_ok'

    if (not db_config.getboolean('msg2db')) or insert2db_err:
        try:
            # init message dir
            data_dir = os.path.join(os.path.curdir, 'msg_pool')
            if not os.path.exists(data_dir):
                os.makedirs(data_dir, mode=0o755)

            # save messages into file
            msg_dt = datetime.datetime.strptime(
                msg_list[0]['receivedTime'],
                '%Y%m%d%H%M%S',
            )
            last_monday = msg_dt - datetime.timedelta(days=msg_dt.weekday())
            data_file = os.path.join(
                data_dir,
                'msgs_%s-%02d-%02d.txt'%(
                    last_monday.year,
                    last_monday.month,
                    last_monday.day,
                ),
            )
            with open(data_file, 'a+') as f:
                for msg in msg_list:
                    f.write(str(msg)+'\n')
        except:
            return 'msg2file_err'
        else:
            if insert2db_err:
                return 'msg2db_err'
            else:
                return 'msg2file_ok'


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
    kafka_receiver = KafkaReceiver(envs['kafka'], in_queue)
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
    pool = multiprocessing.Pool(envs.getint('general', 'mp_worker_num'))

    # logging
    json_logger = get_json_logger()

    # generate report
    cache_max_time = envs.getint('general', 'cache_max_time')
    last_time = time.time()
    while True:
        on_duty = False

        # save messages to db or file
        if cache_queue.full() or ((time.time() - last_time) > cache_max_time):
            if not cache_queue.empty():
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
            msg = json.loads(msg)
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
        if not in_queue.empty():
            raw_msg = in_queue.get()
            msg = json.loads(raw_msg)
            #print(msg)
            # check the data validation
            if not msg['data']:
                json_logger.info('"rest":"No data found in %s"'%(str(msg)))
                #print('Not find data in message.')
                continue

            if not msg['reportType']:
                json_logger.info(
                    '"rest": "Get unrelated message - %s"'%(str(msg))
                )
                continue

            # get report type and the data purpose
            if '|' in msg['reportType']:
                report_type, data_purpose = msg['reportType'].split('|')
            else:
                report_type = msg['reportType']
                data_purpose = 'REPORT'

            # if we get a test message
            if report_type=='test':
                json_logger.info('"rest":"Get test message - %s"'%(str(msg)))
                continue

            # normalize message structure
            msg['reportType'] = report_type
            msg['reportProcessStatus'] = data_purpose
            ts = datetime.datetime.strftime(
                datetime.datetime.now(),
                '%Y%m%d%H%M%S',
            )
            msg['receivedTime'] = ts

            # save the message
            if data_purpose=='STORE':
                cache_queue.put(msg)
                continue

            # validate received data
            try:
                msg['data'] = eval(msg['data'])
            except:
                json_logger.error('"rest":"Get invalid data - %s"'%(str(msg)))
                continue

            pool.apply_async(
                generate_report,
                (
                    msg,
                    out_queue,
                    cache_queue,
                ),
                {
                    'bucket': bucket,
                    'base_url': base_url,
                    'dummy_base_url': dummy_base_url,
                },
            )

            on_duty = True

        if not on_duty:
            time.sleep(0.01)

