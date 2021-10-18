#!/bin/env python3
'''
ChangeLogs
- 2021.08.11:
  - adding compression argument and adjusting suffix "tgz"
  - compression feature is added by "Kirill Davydychev", Thanks Kirill
- 2021.08.10:
  - check source directory exist
  - handling argument with argparse
- 2021.08.09:
  - error handling, when tar can't archive a file with 'permission denied' error'
- 2021.08.05:
  - this utility will copy files from local filesystem to SnowballEdge in parallel  
  - snowball_uploader alternative
- 2021.08.03:
  - support multiprocessing(spawn) 
  - fixing windows path delimeter (\)
  - support compatibility of file name between MAC and Windows 
- 2021.08.02:
  - adding logger
  - fixing error on python3.8, multiprocessing.set_start_method("fork")
    - https://github.com/pytest-dev/pytest-flask/issues/104
- 2021.08.01: adding uploader feature
- 2021.07.24: 
- 2021.07.23: applying multiprocessing.queue + process instead of pool
- 2021.07.21: modified getObject function
  - for parallel processing, multiprocessing.Pool used
  - used bucket.all instead of paginator
- 2021.07.20: first created
'''

#requirement
## python 3.7+ (os.name)
## boto3
## preferred os: linux (Windows works as well, but performance is slower)

import os
import boto3
import botocore
import multiprocessing
from os import path, makedirs
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import logging
import time
import unicodedata
import random
import string
import math
import io
import tarfile
import traceback
import argparse

## treating arguments
parser = argparse.ArgumentParser()
parser.add_argument('--bucket_name', help='your bucket name e) your-bucket', action='store', required=True) 
parser.add_argument('--src_dir', help='source directory e) /data/dir1/', action='store', required=True) 
#parser.add_argument('--region', help='aws_region e) ap-northeast-2', action='store') 
parser.add_argument('--endpoint', help='snowball endpoint e) http://10.10.10.10:8080 or https://s3.ap-northeast-2.amazonaws.com', action='store', default='https://s3.ap-northeast-2.amazonaws.com', required=True) 
parser.add_argument('--profile_name', help='aws_profile_name e) sbe1', action='store', default='default') 
parser.add_argument('--prefix_root', help='prefix root e) dir1/', action='store', default='') 
parser.add_argument('--max_process', help='NUM e) 5', action='store', default=5, type=int) 
parser.add_argument('--max_tarfile_size', help='NUM bytes e) $((1*(1024**3))) #1GB for < total 50GB, 10GB for >total 50GB', action='store', default=10*(1024**3), type=int)
parser.add_argument('--max_part_size', help='NUM bytes e) $((100*(1024**2))) #100MB', action='store', default=100*(1024**2), type=int)
parser.add_argument('--compression', help='specify gz to enable', action='store', default='')
args = parser.parse_args()

prefix_list = args.src_dir  ## Don't forget to add last slash '/'
prefix_root = args.prefix_root ## Don't forget to add last slash '/'
##Common Variables
bucket_name = args.bucket_name
profile_name = args.profile_name
endpoint = args.endpoint
max_process = args.max_process
max_tarfile_size = args.max_tarfile_size # 10GiB, 100GiB is max limit of snowball
max_part_size = args.max_part_size  # 100MB, 500MiB is max limit of snowball
compression = args.compression # default for no compression, "gz" to enable
log_level = logging.INFO ## DEBUG, INFO, WARNING, ERROR
# end of user variables ## you don't need to modify below codes.
##### Optional variables
## begin of snowball_uploader variables
s3_client_class = 'STANDARD' ## value is fixed, snowball only transferred to STANDARD class
min_part_size = 5 * 1024 ** 2 # 16MiB for S3, 5MiB for SnowballEdge
max_part_count = int(math.ceil(max_tarfile_size / max_part_size))
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
# CMD variables
cmd='upload_dir' ## supported_cmd: 'download|del_obj_version|restore_obj_version'
# create log directory
try:
    os.makedirs('log')
except: pass
errorlog_file = 'log/error-%s.log' % current_time
successlog_file = 'log/success-%s.log' % current_time
filelist_file = 'log/filelist-%s.log' % current_time
quit_flag = 'DONE'
# End of Variables

if os.name == 'posix':
    multiprocessing.set_start_method("fork")

# S3 session
#s3_client = boto3.client('s3')
session = boto3.Session(profile_name=profile_name)
s3_client = session.client('s3', endpoint_url=endpoint)

# defining function
## setup logger
def setup_logger(logger_name, log_file, level=logging.INFO):
    l = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(message)s')
    fileHandler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    fileHandler.setFormatter(formatter)
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    l.setLevel(level)
    l.addHandler(fileHandler)
    #l.addHandler(streamHandler)

## define logger
setup_logger('error', errorlog_file, level=log_level)
setup_logger('success', successlog_file, level=log_level)
setup_logger('filelist', filelist_file, level=log_level)
error_log = logging.getLogger('error')
success_log = logging.getLogger('success')
filelist_log = logging.getLogger('filelist')

## code from snowball_uploader
def create_mpu(key_name):
    mpu = s3_client.create_multipart_upload(Bucket=bucket_name, Key=key_name, StorageClass=s3_client_class, Metadata={"snowball-auto-extract": "true"})
    mpu_id = mpu["UploadId"]
    return mpu_id

def upload_mpu(key_name, mpu_id, data, index, parts):
    part = s3_client.upload_part(Body=data, Bucket=bucket_name, Key=key_name, UploadId=mpu_id, PartNumber=index)
    parts.append({"PartNumber": index, "ETag": part["ETag"]})
    success_log.debug('parts list: %s' % str(parts))
    return parts

def complete_mpu(key_name, mpu_id, parts):
    result = s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=mpu_id,
        MultipartUpload={"Parts": parts})
    return result

def adjusting_parts_order(mpu_parts):
    return sorted(mpu_parts, key=lambda item: item['PartNumber'])

def buf_fifo(buf):
    tmp_buf = io.BytesIO()            # added for FIFO operation
    tmp_buf.write(buf.read())    # added for FIFO operation
    #print ('3. before fifo, recv_buf_size: %s' % len(buf.getvalue()))
    #print('3.before fifo, recv_buf_pos : %s' % buf.tell())
    buf.seek(0,0)
    buf.truncate(0)
    tmp_buf.seek(0,0)
    buf.write(tmp_buf.read())
    return buf

def copy_to_snowball(tar_name, org_files_list):
    delimeter = ' ,'
    tar_file_size = 0
    recv_buf = io.BytesIO()
    mpu_id = create_mpu(tar_name)
    parts_index = 1
    parts = []
    collected_files_no = 0
    with tarfile.open(fileobj=recv_buf, mode='w:'+compression) as tar:
    #with tarfile.open(fileobj=recv_buf, mode='w:'+compression, compresslevel=1) as tar:
        for file_name, obj_name, file_size in org_files_list:
            if os.path.isfile(file_name):
                try:
                    tar.add(file_name, arcname=obj_name)
                    collected_files_no += 1
                    #success_log.debug('1. recv_buf_size: %s' % len(recv_buf.getvalue()))
                    filelist_log.debug(file_name + delimeter + obj_name + delimeter + str(file_size)) #kyongki
                    recv_buf_size = recv_buf.tell()
                    #success_log.debug('1. recv_buf_pos: %s' % recv_buf.tell())
                    if recv_buf_size > max_part_size:
                        print('multi part uploading:  %s / %s , size: %s bytes' % (parts_index, max_part_count, recv_buf_size))
                        chunk_count = int(recv_buf_size / max_part_size)
                        tar_file_size = tar_file_size + recv_buf_size
                        #print('%s is accumulating, size: %s byte' % (tar_name, tar_file_size))
                        for buf_index in range(chunk_count):
                            start_pos = buf_index * max_part_size
                            recv_buf.seek(start_pos,0)
                            mpu_parts = upload_mpu(tar_name, mpu_id, recv_buf.read(max_part_size), parts_index, parts)
                            parts_index += 1
                        ####################
                        buf_fifo(recv_buf)
                        recv_buf_size = recv_buf.tell()
                        #print('3.after fifo, recv_buf_pos : %s' % recv_buf.tell())
                        #print ('3. after fifo, recv_buf_size: %s' % len(recv_buf.getvalue()))
                    else:
                        pass
                        #print('accumulating files...')
                except IOError:
                    error_log.info("%s is ignored" % file_name) 
            else:
                error_log.info(file_name,' does not exist\n')
                print (file_name + ' is not exist...............................................\n')
    recv_buf.seek(0,0)
    mpu_parts = upload_mpu(tar_name, mpu_id, recv_buf.read(), parts_index, parts)
    parts_index += 1
    mpu_parts = adjusting_parts_order(mpu_parts)
    complete_mpu(tar_name, mpu_id, mpu_parts)
    ### print metadata
    meta_out = s3_client.head_object(Bucket=bucket_name, Key=tar_name)
    print('metadata info: %s\n' % str(meta_out))
    print('%s is uploaded successfully\n' % tar_name)
    success_log.debug('metadata info: %s' % str(meta_out))
    success_log.info('%s uploaded successfully' % tar_name)
    return collected_files_no
## end of code from snowball_uploader

# check source directory exist
def check_srcdir(src_dir):
    if not os.path.isdir(src_dir):
        raise IOError("source directory not found: " + src_dir)

# generate random 6 character
def gen_rand_char():
    char_set = string.ascii_uppercase + string.digits
    return (''.join(random.sample(char_set*6, 6)))
# execute multiprocessing
def run_multip(max_process, exec_func, q):
    p_list = []
    for i in range(max_process):
        p = multiprocessing.Process(target = exec_func, args=(q,))
        p_list.append(p)
        p.daemon = True
        p.start()
    return p_list

def finishq(q, p_list):
    for j in range(max_process):
        q.put(quit_flag)
    for pi in p_list:
        pi.join()

# get files to upload
def upload_get_files(sub_prefix, q):
    num_obj=0
    sum_size = 0
    org_files_list = []
   # get all files from given directory
    for r,d,f in os.walk(sub_prefix):
        for file in f:
            try:
                file_name = os.path.join(r,file)
                # support compatibility of MAC and windows
                file_name = unicodedata.normalize('NFC', file_name)
                if os.name == 'nt':
                    obj_name = prefix_root + file_name.replace(sub_prefix,'',1).replace('\\', '/')
                else:
                    obj_name = prefix_root + file_name.replace(sub_prefix,'',1)
                f_size = os.stat(file_name).st_size                
                file_info = (file_name, obj_name, f_size)
                org_files_list.append(file_info)
                sum_size = sum_size + f_size
                if max_tarfile_size < sum_size:
                    sum_size = 0
                    mp_data = org_files_list
                    org_files_list = []
                    try:
                        # put files into queue in max_tarfile_size
                        q.put(mp_data)
                        success_log.debug('0, sending mp_data size: %s'% len(mp_data))
                        success_log.debug('0, sending mp_data: %s'% mp_data)
                    except Exception as e:
                        error_log.info('exception error: putting %s into queue is failed' % file_name)
                        error_log.info(e)
                num_obj+=1
            except Exception as e:
                error_log.info('exception error: getting %s file info is failed' % file_name)
                error_log.info(e)
            #time.sleep(0.1)
    try:
        # put remained files into queue
        mp_data = org_files_list
        q.put(mp_data)
        success_log.debug('1, sending mp_data size: %s'% len(mp_data))
        success_log.debug('1, sending mp_data: %s'% mp_data)
    except Exception as e:
        error_log.info('exception error: putting %s into queue is failed' % file_name)
        error_log.info(e)
    return num_obj

def upload_file(q):
    while True:
        mp_data = q.get()
        org_files_list = mp_data
        randchar = str(gen_rand_char())
        if compression == '': 
            tar_name = ('snowball-%s-%s.tar' % (current_time, randchar))
        elif compression == 'gz':
            tar_name = ('snowball-%s-%s.tgz' % (current_time, randchar))
        success_log.debug('receving mp_data size: %s'% len(org_files_list))
        success_log.debug('receving mp_data: %s'% org_files_list)
        if mp_data == quit_flag:
            break
        try:
            copy_to_snowball(tar_name, org_files_list)
            #print('%s is uploaded' % tar_name)
        except Exception as e:
            error_log.info('exception error: %s uploading failed' % tar_name)
            error_log.info(e)
            traceback.print_exc()
        #return 0 ## for the dubug, it will pause with error
        
def upload_file_multi(src_dir):
    success_log.info('%s directory is uploading' % src_dir)
    p_list = run_multip(max_process, upload_file, q)
    # get object list and ingest to processes
    num_obj = upload_get_files(src_dir, q)
    # sending quit_flag and join processes
    finishq(q, p_list) 
    success_log.info('%s directory is uploaded' % src_dir)
    return num_obj

def s3_booster_help():
    print("example: python3 s3booster_upload.py")
# start main function
if __name__ == '__main__':

    # define simple queue
    q = multiprocessing.Queue()
    
    #print("starting script...")
    start_time = datetime.now()
    src_dir = prefix_list
    check_srcdir(src_dir)

    if cmd == 'upload_dir':
        total_files = upload_file_multi(src_dir)
    else:
        s3_booster_help

    end_time = datetime.now()
    print('====================================')
    #for d in down_dir:
    #    stored_dir = local_dir + d
    #    print("[Information] Download completed, data stored in %s" % stored_dir)
    print('Duration: {}'.format(end_time - start_time))
    print('Total File numbers: %d' % total_files) #kyongki
    print('S3 Endpoint: %s' % endpoint)
    print('End')
