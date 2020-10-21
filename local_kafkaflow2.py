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

import oss2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError


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

def generate_report(user_id, report_type,
                    bucket=None, base_url=None, dummy_base_url=None):
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
        print('Error! Not find report type named %s'%(report_type))
        return uploaded_msg

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
    json_file = os.path.join(base_dir, '%s_data.json'%(user_id))

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
        #print('Error in nbconvert stage!')
        # clean img dir
        user_img_dir = os.path.join(img_dir, user_id)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
        return uploaded_msg

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
        #print('Error in trans2std stage!')
        return uploaded_msg

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
        #print('Error in weasyprint stage!')
        # clean img dir
        user_img_dir = os.path.join(img_dir, user_id)
        if os.path.exists(user_img_dir):
            shutil.rmtree(user_img_dir)
        return uploaded_msg

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
    else:
        uploaded_msg['status'] = 'error'
        uploaded_msg['args'] = 'Uploads to oss.'
        uploaded_msg['stderr'] = 'Falied to upload pdf file.'
    return uploaded_msg

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

    # connect to aliyun oss
    bucket = conn2aliyun(envs['aliyun'])
    base_url = '.'.join([
        envs['aliyun']['oss_bucket_name'],
        envs['aliyun']['oss_endpoint_name'],
    ])
    dummy_base_url = envs['aliyun']['dummy_oss_url']

    # generate report
    user_id = '5f34e34fabebfe6f71525975'
    report_type = 'mathDiagnosisK8_v1'
    msg = generate_report(
        user_id,
        report_type,
        bucket=bucket,
        base_url=base_url,
        dummy_base_url=dummy_base_url,
    )


    if msg['status']=='ok':
        try:
            future = kafka_sender.send(envs['kafka']['send_topic'], msg)
            record_metadata = future.get(timeout=30)
            assert future.succeeded()
        except KafkaTimeoutError as kte:
            print('"Timeout while sending kafka message - %s"'%(str(msg)))
        except KafkaError as ke:
            print('"KafkaError while sending kafka message - %s"'%(str(msg)))
        except:
            print('"Exception while sending kafka message - %s"'%(str(msg)))
        else:
            print('"Generate report successfully - %s"'%(str(msg)))

    else:
        print(msg)

