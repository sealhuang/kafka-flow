# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
from itertools import islice
from configparser import ConfigParser

import oss2


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


if __name__ == '__main__':
    # read configs
    envs = ConfigParser()
    envs.read('./env.config')

    # connect to aliyun oss
    bucket = conn2aliyun(envs['aliyun'])
    base_url = '.'.join([
        envs['aliyun']['oss_bucket_name'],
        envs['aliyun']['oss_endpoint_name'],
    ])

    # upload file
    upload_file(bucket, base_url, 'test.pdf', 'test/test.pdf')

    # remove file
    #oss_list = [upload_files[k][1] for k in upload_files]
    #delete_files(bucket, oss_list)
    
    # remove selected files
    #sel_files = []
    #for obj in oss2.ObjectIterator(bucket):
    #    # list dir
    #    if obj.is_prefix():
    #        print('-'*10)
    #        print(obj.key)
    #    # list files
    #    else:
    #        print(obj.key)
    #        if 'mathDiagnosisK8_v1/' in obj.key:
    #            sel_files.append(obj.key)

    #print(sel_files)
    #for item in sel_files:
    #    bucket.delete_object(item)

