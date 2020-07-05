# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import json
from itertools import islice

import oss2
from kafka import KafkaConsumer, KafkaProducer


def get_envs(env_file='.env'):
    with open(env_file, 'r') as f:
        env_content = f.readlines()

    env_content = [line.strip().split('=') for line in env_content
                    if '=' in line]
    envs = dict(env_content)
    return envs

def upload_file(bucket, base_url, upload_files):
    """Upload files to aliyun oss.
    Arguments:
        bucket: oss bucket instance.
        base_url: url of oss bucket.
        upload_files: a dict of uploading files
    """
    # kafka message of upload files successfully
    uploaded_msg = {}
    for k in upload_files:
        rsp = bucket.put_object_from_file(
            upload_files[k][1],
            upload_files[k][0],
        )
        # if upload successfully
        # XXX: should check file content using etag field (md5)
        if rsp.status==200:
            uploaded_msg[k] = 'https://'+base_url+'/'+upload_files[k][1]
        else:
            print('%s error while uploading file %s' %
                    (rsp.status, upload_files[k][0]))
    
    return uploaded_msg

def delete_files(bucket, oss_list):
    for oss_file in oss_list:
        bucket.delete_object(oss_file)


if __name__ == '__main__':
    # get access key
    envs = get_envs()

    # aliyun oss auth
    auth = oss2.Auth(envs['ACCESS_KEY_ID'], envs['ACCESS_KEY_SECRET'])
    # get oss bucket
    bucket = oss2.Bucket(
        auth,
        envs['OSS_ENDPOINT_NAME'],
        envs['OSS_BUCKET_NAME'],
    )

    # upload file
    upload_files = {
        'reading': [
            os.path.join(os.path.curdir, 'leveledReadingPersonalMid.pdf'),
            os.path.join('erdaoqu', 'reading.pdf'),
        ],
        'intel': [
            os.path.join(os.path.curdir, 'ztmt.pdf'),
            os.path.join('erdaoqu', 'intel.pdf'),
        ],
    }
    base_url = '.'.join([envs['OSS_BUCKET_NAME'], envs['OSS_ENDPOINT_NAME']])
    uploaded_msg = upload_file(bucket, base_url, upload_files)

    # if uploaded_msg is not empty, send message
    producer = KafkaProducer(
        bootstrap_servers = envs['KAFKA_BOOTSTRAP_SERVERS'],
        value_serializer = lambda v: json.dumps(v).encode('utf-8'),
    )
    if uploaded_msg:
        # XXX: add user id to uploaded_msg
        producer.send(envs['KAFKA_SEND_TOPIC'], uploaded_msg)

    # remove file
    #oss_list = [upload_files[k][1] for k in upload_files]
    #delete_files(bucket, oss_list)
    
    # list files
    #for b in islice(oss2.ObjectIterator(bucket), 10):
    #    print(b.key)

