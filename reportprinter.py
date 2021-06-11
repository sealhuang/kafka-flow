# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
import bson
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

import oss2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from weasyprint import HTML
from cachetools import cached, TTLCache, LRUCache
from cachetools.keys import hashkey

from utils import conn2db


# global var: root_dir
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


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

    def __init__(self, envs, fast_queue, queue):
        """Initialize kafka message receiver process."""
        multiprocessing.Process.__init__(self)

        self.consumer = KafkaConsumer(
            envs.get('rec_topic'),
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
        self.fast_queue = fast_queue

    def run(self):
        """Receive message and put it in the queue."""
        # read message
        for msg in self.consumer:
            line = msg.value.decode('utf-8').strip()
            print(line)
            try:
                _msg = json.loads(line)
                assert 'ticketID' in _msg
                assert _msg.get('version', None)==2
                priority = _msg.pop('priority', 'high')
                if priority=='low':
                    self.queue.put(_msg)
                else:
                    self.fast_queue.put(_msg)
            except:
                print('Receive invalid message')
                print(line)


def get_json_logger(log_level=logging.DEBUG):
    """Logger initialization."""
    # init log dir
    log_dir = os.path.join(ROOT_DIR, 'log')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, mode=0o755)

    logger = logging.getLogger('logger')
    logger.setLevel(log_level)

    # set json format log file
    handler = TimedRotatingCompressedFileHandler(
        os.path.join(log_dir, 'reportprinter.log'),
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

def generate_report(msg, out_queue, cache_queue, bucket, base_url,
                    dummy_base_url):
    """Workflow for generating report."""
    # local vars init
    ticket_id = msg['ticketID']
    report_type = msg['reportType']
    data_purpose = msg['dataObjective']
    
    #print('Generating report for ticket ID %s'%(ticket_id))

    # check whether the callback is required
    callback_flag = True
    if not msg['callback_flag']:
        callback_flag = False
    msg.pop('callback_flag')

    rec_ts = msg['examEndTime']

    # init callback message
    uploaded_msg = {
        'id': ticket_id,
        'report_type': report_type,
        'status': 'ok',
        'callback': callback_flag,
    }

    # init result message
    # updated fields in report result
    result_data = {}
    sel_keys = [
        'ticketID',
        'name',
        'gender',
        'age',
        'birthdate',
        'province',
        'city',
        'district',
        'school',
        'grade',
        'paperVersion',
        'dataObjective',
        'reportType',
    ]
    for k in sel_keys:
        result_data[k] = msg.get(k, '')
    result_data['userID'] = msg['userId']
    result_data['class'] = msg['executiveClass']
    result_data['paperID'] = msg['paperId']
    result_data['examStartTime'] = datetime.datetime.fromtimestamp(
        msg['examStartTime']/1000
    )
    result_data['examEndTime'] = datetime.datetime.fromtimestamp(
        msg['examEndTime']/1000
    )
    result_data['reportRequest'] = True
    result_data['reportStatus'] = 'OK'
    result_data['reportURL'] = ''


    # get report types
    report_gallery = get_report_gallery(
        os.path.join(ROOT_DIR, 'report_gallery.config')
    )

    if report_type not in report_gallery:
        # add callback message to out queue
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ''
        uploaded_msg['stderr'] = 'Not find report type %s'%(report_type)
        out_queue.put(uploaded_msg)
        # add result message to cache
        result_data['reportStatus'] = 'ERR'
        cache_queue.put(result_data)
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

    # save answer sheet as json file
    json_file = os.path.join(base_dir, '%s_data.json'%(ticket_id))
    with open(json_file, 'w') as jf:
        jf.write(json.dumps(msg)+'\n')

        
    # run ipynb file
    ipynb_name = report_cfg['entry']
    ipynb_file = os.path.join(base_dir, ipynb_name)
    html_file = os.path.join(base_dir, 'raw_report_%s.html'%(ticket_id))
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
        env = dict(os.environ, USERID=ticket_id),
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        encoding = 'utf-8',
    )
    # user's image dir
    user_img_dir = os.path.join(img_dir, ticket_id)
    # check nbconvert output status
    if not ret.returncode==0:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = ret.args
        uploaded_msg['stderr'] = ret.stderr
        out_queue.put(uploaded_msg)
        # add message to cache
        result_data['reportStatus'] = 'ERR'
        cache_queue.put(result_data)
        #print('Error in nbconvert stage!')
        # clean img dir
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
        return None

    # if we get computation results successfully, get the file path
    result_file = os.path.join(base_dir, '%s_results.json'%(ticket_id))
    try:
        calc_data = json.load(open(result_file))
        if len(calc_data):
            uploaded_msg['reportData'] = {report_type: dict(calc_data)}
            result_data['reportData'] = calc_data
        else:
            result_file = None
    except:
        result_file = None

    # if the pdf file is needed
    if data_purpose=='REPORT':
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
            article_summary_param = '--include_article_summary=' + \
                                    report_cfg['include_article_summary']
        else:
            article_summary_param = ''


        std_html_file = os.path.join(base_dir, 'std_report_%s.html'%(ticket_id))
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
            out_queue.put(uploaded_msg)
            # add message to cache
            result_data['reportStatus'] = 'ERR'
            cache_queue.put(result_data)
            #print('Error in trans2std stage!')
            # clean
            os.remove(html_file)
            if os.path.exists(std_html_file):
                os.remove(std_html_file)
            if os.path.exists(user_img_dir):
                shutil.rmtree(user_img_dir)
            return None

        # convert html to pdf
        ts = datetime.datetime.strftime(
            datetime.datetime.now(),
            '%Y%m%d%H%M%S',
        )
        pdf_filename = 'report_%s_%s.pdf'%(ticket_id, ts)
        pdf_file = os.path.join(pdf_dir, pdf_filename)
        try:
            HTML(std_html_file).write_pdf(pdf_file)
            assert os.path.exists(pdf_file)
        # check weasyprint output status
        except:
            uploaded_msg['status'] = 'error'
            uploaded_msg['args'] = ''
            uploaded_msg['stderr'] = 'Err in weasyprint process'
            out_queue.put(uploaded_msg)
            # add message to cache
            result_data['reportStatus'] = 'ERR'
            cache_queue.put(result_data)
            #print('Error in weasyprint stage!')
            # clean
            os.remove(html_file)
            if os.path.exists(std_html_file):
                os.remove(std_html_file)
            if os.path.exists(user_img_dir):
                shutil.rmtree(user_img_dir)
            return None

        # upload file
        remote_file = os.path.join(report_cfg['oss_dir'], pdf_filename)
        remote_url = upload_file(bucket, base_url, pdf_file, remote_file)

        # clean
        os.remove(html_file)
        os.remove(std_html_file)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
        if os.path.exists(pdf_file):
            os.remove(pdf_file)

        if remote_url:
            uploaded_msg['status'] = 'ok'
            dummy_remote_url = 'https://'+dummy_base_url+'/'+remote_file
            uploaded_msg['urls'] = {report_type: dummy_remote_url}
            out_queue.put(uploaded_msg)
            # add message to cache
            result_data['reportURL'] = dummy_remote_url
            cache_queue.put(result_data)
        else:
            uploaded_msg['status'] = 'error'
            uploaded_msg['args'] = 'Uploads to oss.'
            uploaded_msg['stderr'] = 'Falied to upload pdf file.'
            out_queue.put(uploaded_msg)
            result_data['reportStatus'] = 'ERR2OSS'
            cache_queue.put(result_data)
            return None

    elif data_purpose=='CALC':
        # clean
        os.remove(html_file)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)

        if result_file:
            uploaded_msg['status'] = 'ok'
            out_queue.put(uploaded_msg)
            cache_queue.put(result_data)
        else:
            uploaded_msg['status'] = 'error'
            uploaded_msg['args'] = 'Calculate attributes.'
            uploaded_msg['stderr'] = 'No results file found'
            out_queue.put(uploaded_msg)
            result_data['reportStatus'] = 'ERR'
            cache_queue.put(result_data)
            return None

    # move raw data and result file
    targ_file = os.path.join(data_dir, '%s_%s.json'%(ticket_id, rec_ts))
    shutil.move(json_file, targ_file)
    if result_file:
        targ_file = os.path.join(data_dir, '%s_results_%s.json'%(ticket_id, rec_ts))
        shutil.move(result_file, targ_file)
    elif os.path.exists(os.path.join(base_dir, '%s_results.json'%(ticket_id))):
        os.remove(os.path.join(base_dir, '%s_results.json'%(ticket_id)))

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

def save_msgs(msg_list, ans_col, results_col):
    """Save message."""
    insert2db_err = False

    #print(msg_list)

    try:
        # update answer sheet
        update_cmd = []
        for item in msg_list:
            update_cmd.append(UpdateOne(
                {'ticketID': item['ticketID']},
                {'$set': {
                    'reportStatus': item['reportStatus'],
                    'dataObjective': item['dataObjective'],
                    'reportType': item['reportType'],
                }},
                upsert=False,
            ))
        ret = ans_col.bulk_write(update_cmd)
        assert ret.matched_count==len(msg_list)

        # update or insert new result
        upsert_cmd = []
        for item in msg_list:
            _tmp = dict(item)
            _tmp.pop('ticketID')
            if 'reportData' not in _tmp:
                _tmp['reportData'] = {}
            upsert_cmd.append(UpdateOne(
                {'ticketID': item['ticketID']},
                {'$set': _tmp},
                upsert=True,
            ))
        ret = results_col.bulk_write(upsert_cmd)
        assert (ret.matched_count+ret.upserted_count)==len(msg_list)

    except:
        insert2db_err = True

    else:
        return 'msg2db_ok'

    if insert2db_err:
        try:
            # init message dir
            data_dir = os.path.join(ROOT_DIR, 'msg_pool')
            if not os.path.exists(data_dir):
                os.makedirs(data_dir, mode=0o755)

            # save messages into file
            msg_dt = datetime.datetime.strptime(
                msg_list[0]['examEndTime'],
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
            return 'msg2file_ok'

def normalize_ret_dict(d):
    """Normalize return message."""
    assert isinstance(d, dict)

    for k in d:
        if isinstance(d[k], dict):
            normalize_ret_dict(d[k])
        elif isinstance(d[k], int):
            d[k] = str(d[k])
        elif isinstance(d[k], float):
            d[k] = str(d[k])
        elif isinstance(d[k], list):
            d[k] = '|'.join([str(ele) for ele in d[k]])
        elif isinstance(d[k], tuple):
            d[k] = '|'.join([str(ele) for ele in d[k]])

def _get_full_tags(idx, all_tags, full_tags):
    """Get context for each tag recursively."""
    if idx in all_tags:
        full_tags.append(all_tags[idx][0])
        parent_idx = all_tags[idx][1]
        return _get_full_tags(parent_idx, all_tags, full_tags)
    else:
        return full_tags

@cached(cache=TTLCache(maxsize=512, ttl=900), key=lambda ttl, db: hashkey(ttl))
#@cached(cache=LRUCache(maxsize=512), key=lambda ttl, db: hashkey(ttl))
def get_question_infos(ttl, db):
    """Get question info from db indexed by question title."""
    # get questions' domain tags first
    tag_col = db['questionTag']
    all_tags = {}
    for item in tag_col.find({'parent': {'$exists': True}}):
        if not item['parent'] is None:
            all_tags[str(item['_id'])] = (item['title'], item['parent'])
    # get all domain tags
    domain_tags = {}
    for idx in all_tags:
        full_tags = _get_full_tags(idx, all_tags, [])
        if full_tags[-1]=='维度':
            full_tags.reverse()
            domain_tags[idx] = '-'.join(full_tags)

    # get question info
    question_col = db['Question']
    # handle composite questions
    title_parts = ttl.split('-')
    title = title_parts[0]
    subpaths = title_parts[1:]
    raw_info = question_col.find_one({'title': title})
    try:
        qinfo = {}
        # get domain tags
        qinfo['domain_'+ttl] = []
        for item in raw_info['tags']:
            if (item.get('subPath', None) is None) or \
                ('-'.join(subpaths) in item['subPath']):
                if str(item['_id']) in domain_tags:
                    qinfo['domain_'+ttl].append(domain_tags[str(item['_id'])])

        # get other properties
        qinfo['attr_'+ttl] = {}
        if not raw_info['difficulty']==0.0:
            qinfo['attr_'+ttl]['difficulty'] = raw_info['difficulty']
        _qunit = raw_info['compositeQunit']
        for sp in subpaths:
            _qunit = _qunit['subQunits'][int(sp)-1]
        if ('extFeatures' in _qunit) and isinstance(_qunit['extFeatures'], dict):
            for k in _qunit['extFeatures']:
                qinfo['attr_'+ttl][k] = _qunit['extFeatures'][k]['value']

        return qinfo 

    except:
        print('Err while fetching info of %s'%(ttl))
        return None


def queue_writer(q):
    """For test."""
    for i in range(10):
        time.sleep(random.random() * 10)
        flag = random.randint(1, 51)
        if flag>28:
            msg = {
                'ticketID': '000'+str(i),
                'priority': 'low',
                'version': 2,
            }
        else:
            msg = {
                'ticketID': '00'+str(i),
                'version': 2,
            }
        q.put(json.dumps(msg))


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
    ans_coll = pool_db[db_config.get('answer_sheet_collection')]
    res_coll = pool_db[db_config.get('result_collection')]
 
    # XXX for test: get question infos
    #questions = [
    #    'visualSpatial_mentalRotation_3dRotation_T1_24',
    #    'visualSpatial_mentalRotation_3dRotation_T1_23'
    #]
    #for qtitle in questions:
    #    print('*'*10)
    #    print(qtitle)
    #    st = time.time()
    #    qinfo = get_question_infos(qtitle, dbclient[db_config.get('question_db')])
    #    print(time.time()-st)
    #    if isinstance(qinfo, dict):
    #        print(qinfo)

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
        bootstrap_servers = envs.get('kafka', 'bootstrap_servers').split(','),
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
        retries = 5,
    )
    #-- initialize kafka message receiver process
    kafka_receiver = KafkaReceiver(envs['kafka'], fast_in_queue, in_queue)
    kafka_receiver.start()

    # XXX for test: Create a message writer
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

    # XXX for test: Send report request
    #in_queue.put(json.dumps({
    #    'ticketID': '60814eace781a7665c8c88e6',
    #    'version': 2,
    #}))

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
            save_ret = save_msgs(msg_list, ans_coll, res_coll)
            if save_ret=='msg2db_ok':
                json_logger.info('"rest":"Save msgs to db successfully"')
            elif save_ret=='msg2file_ok':
                json_logger.info('"rest":"Error while saving msgs to db, save results to file successfully"')
            elif save_ret=='msg2db_err':
                json_logger.error('"rest":"Error while saving msgs to db"')
            elif save_ret=='msg2file_err':
                json_logger.error('"rest":"Error while saving msgs to file"')

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
                msg = fast_in_queue.get()
            else:
                msg = in_queue.get()
            #print(msg)

            # get answer sheet from db
            ans_item = ans_coll.find_one({
                'ticketID': msg['ticketID']
            })
            if not isinstance(ans_item, dict):
                json_logger.error(
                    '"rest":"Not get answer sheet (ticketID %s)"' %
                        (msg['ticketID'])
                )
                continue

            # delete useless fields
            ans_item.pop('_id', None)
            ans_item.pop('_class', None)

            # modify report type and data objective if provided
            if msg.get('reportType', None):
                ans_item['reportType'] = msg['reportType']
            if msg.get('dataObjective', None):
                ans_item['dataObjective'] = msg['dataObjective']

            # get report type and the data objectives
            if not ans_item.get('reportType', ''):
                json_logger.info(
                    '"rest": "Get unrelated message - %s"'%(str(ans_item))
                )
                continue

            # XXX for test: if we get a test message
            if ans_item['reportType']=='test':
                json_logger.info(
                    '"rest":"Get test message - %s"'%(str(ans_item))
                )
                continue

            # if the objective of the message is `STORE`
            if ans_item['dataObjective']=='STORE':
                continue

            # validate answer sheet
            if ('data' not in ans_item) or \
                (not isinstance(ans_item['data'], dict)):
                json_logger.error(
                    '"rest":"No data found in answer sheet (%s)"' % 
                        (msg['ticketID'])
                )
                continue

            # get question infos for each question
            question_list = [
                q[4:] for q in ans_item['data'] if q.startswith('rsp_')
            ]
            for qtitle in question_list:
                qinfo = get_question_infos(
                    qtitle,
                    dbclient[db_config.get('question_db')],
                )
                if isinstance(qinfo, dict):
                    ans_item['data'].update(qinfo)

            # if no callback required
            ans_item['callback_flag'] = True
            if msg.get('callback', 'Y')=='N':
                ans_item['callback_flag'] = False

            pool.apply_async(
                generate_report,
                (
                    ans_item,
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

