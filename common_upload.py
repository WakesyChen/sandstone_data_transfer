# -*- coding:utf-8 -*-


import commands
import datetime
import hashlib
import os
import os.path
import socket
import sys
import traceback
import configobj
import boto
import boto.s3
import boto.s3.connection
from config import *
from optparse import OptionParser
from db.mysql_db import CommonMysqlDB
from db.oracle_db import CommonOracleDB
from multiupload import upload_file_multipart
from util import logger
reload(sys) 
sys.setdefaultencoding('gbk')

"""
一、脚本说明------首次上传
   第一次上传某个文件夹下的所有文件到MOS（即S3），并记录每个文件到数据库；
   如果某个文件上传到MOS失败，记录到..._failed_s3; 上传到数据库失败，记录到..._failed_db
"""

# S3配置
BUCKET_PREF = ""
S3_CONN = None
UPLOAD_DIR = DEFAULT_UPLOAD_DIR

# 数据库配置
db_instance = None
db_connection = None
db_type = None
db_conf = DEFAULT_DB_CONF
db_table = None



#============================S3相关操作========================

def init_s3_connection():
    try:
        global AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT, S3_CONN
        S3_CONN = get_s3_connection(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT)
        return True
    except Exception,e:
        logger.critical("init_s3_connection s3 failed: %s" % str(e))
        return False


def get_hostname_and_ipaddr():
    try:
        hostname = socket.gethostname()
        ip_info = socket.gethostbyname_ex(hostname)
        logger.debug("IPinfo: %s" % str(ip_info))
        ipaddr = ip_info[2][0]
        logger.debug("local ip addr: %s"%ipaddr)
        return hostname, ipaddr
    except Exception,e:
        logger.critical("get hostname ip error: %s" % str(e))
        return "PADDING", "PADDING"


def get_s3_connection(access_key, secret_key, host, port):
    S3_CONN = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=host,
        port=port,
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),)
    return S3_CONN


def iterate_over_directory_process(source_path, processor):
    '''
    iterate over source_path,and process files with function: processor
    '''
    # new_path = source_path
    if os.path.isfile(source_path):
        cloud_path = source_path[len(UPLOAD_DIR):]
        processor(cloud_path, source_path)

    elif os.path.isdir(source_path):
        for subfile in os.listdir(source_path):
            new_path=os.path.join(source_path,subfile)
            iterate_over_directory_process(new_path, processor)

 
def upload_file_to_s3(cloud_path, file_path):

    if is_file_uploaded(cloud_path):
        logger.info("File: %s has already been uploaded, go on for next file!", cloud_path)
        return True

    md5id = GetFileMd5(file_path)
    try:
        global BUCKET_PREF
        bucket = S3_CONN.get_bucket(BUCKET_NAME, validate=False)
        object_path = cloud_path.replace("\\", "/")

        if object_path[0] == '/':
            object_path = BUCKET_PREF + object_path
        else:
            object_path = BUCKET_PREF + '/' + object_path

        kobject = bucket.new_key(object_path)
        filesize = os.stat(file_path).st_size
        if filesize >= MULTI_UPLOAD_THRESHOLD_SIZE:
            upload_file_multipart(file_path, object_path, bucket, md5id)
            logger.info("SUCCESS to S3: multipart way, uploading destination object path: %s"%object_path)
        else:
            kobject.set_contents_from_filename(file_path, headers={'CONTENT-MD5' : md5id})
            logger.info("SUCCESS to S3: singlefile way, uploading destination file path: %s"%file_path)
        # 插入记录到数据库
        insert_file_record_to_db(cloud_path,  file_path)

    except Exception,e:
        S3_CONN.close()
        logger.error("FAILURE to S3 : upload cloud_path: %s ,error: %s" % (cloud_path, str(e)))
        collect_failed_files(file_path, UPLOAD_MOS_FAILED_LOG)
        return False

    return True


def GetFileMd5(filename):
    if not os.path.isfile(filename):
        return
    myhash = hashlib.md5()
    f = file(filename,'rb')
    while True:
        b = f.read(8096)
        if not b :
            break
        myhash.update(b)
    f.close()
    return myhash.hexdigest()


def collect_failed_files(file_path, failed_log):
    '''
    :param failed_log: 记录上传失败的文件日志路径
    :param file_path: 上传失败的文件路径
    '''
    if os.path.isfile(file_path):
        try:
            add_failed_file = "echo '{}' >> {}".format(file_path, failed_log)
            ret_code, result = commands.getoutput(add_failed_file)
            assert ret_code == 0, "Writing path [{0}] to [{1}] failed!".format(file_path, failed_log)
        except Exception, error:
            logger.error("***collect_failed_files, error_msg:[%s]" % error)


#==========================数据库相关操作======================

def init_db_connection():
    """根据配置文件，初始化选择数据库，以及对应的方法"""
    global db_type, db_instance, db_connection, db_table
    try:
        db_conf_obj = configobj.ConfigObj(db_conf)
        db_type = db_conf_obj['common']['DB_TYPE']

        if db_type == 'oracle':
            db_instance = CommonOracleDB(db_conf)
            db_table = db_conf_obj['oracle']['DEFAULT_TABLE']
        elif db_type == 'mysql':
            db_instance = CommonMysqlDB(db_conf)
            db_table = db_conf_obj['mysql']['DEFAULT_TABLE']
        else:
            logger.error("init_db_connection failed, invalid DB_TYPE:%s" % db_type)
            return False

        if db_instance.get_connection(): # 创建数据库连接
            return True
    except Exception, error:
        logger.critical("init_db_connection failed, error:[%s]" % error)
    return False


def is_file_uploaded(cloudpath):
    '''查询数据库上传记录，根据结果数目，判断文件是否上传'''
    cols = ["CLOUDPATH"] # 待查询的列
    where = "CLOUDPATH = '%s' " % cloudpath
    query_count = db_instance.select_count(cols=cols, table=db_table, where=where)
    return True if query_count > 0 else False


def add_record_to_db(datainfo, fullpath):
    '''上传到S3成功后，添加记录到数据库中'''
    insert_success = db_instance.insert_data(db_table, datainfo)

    if not insert_success:
        # 没有插入成功，加入失败文件列表
        collect_failed_files(fullpath, UPLOAD_ORACLE_FAILED_LOG)
    return insert_success


def insert_file_record_to_db(cloud_path, fullpath):
    global BUCKET_PREF
    fullpath = fullpath.replace("\\", "/")
    cloud_path = cloud_path.replace("\\", "/")
    hostname, ipaddr = get_hostname_and_ipaddr()

    if cloud_path[0] == '/':
        cloud_path = BUCKET_PREF + cloud_path
    else:
        cloud_path = BUCKET_PREF + '/' + cloud_path
    try:

        datainfo = {}
        datainfo['filename'] = fullpath.split('/')[-1]
        datainfo['fullsourcepath'] = fullpath
        datainfo['cloudpath'] = cloud_path
        datainfo['uploadhostname'] = hostname
        datainfo['uploadip'] = ipaddr
        datainfo['uploadtime'] = str(datetime.datetime.now())
        datainfo['md5id'] = GetFileMd5(fullpath)
        if add_record_to_db(datainfo, fullpath):
            logger.info("SUCCESS: insert file to db: %s" % fullpath)

    except Exception,e:
        logger.error("FAILURE: insert file record error: %s, file: %s" % (str(e), fullpath))
        # 插入失败，保存失败文件路径到指定日志
        collect_failed_files(fullpath, UPLOAD_ORACLE_FAILED_LOG)
        return False
    return True


def ensure_table_created():
    return db_instance.ensure_table_created(db_table)

#==========================公共方法===========================

def get_user_paras():
    try:
        opt = OptionParser()

        opt.add_option('--bucket-name',
                       dest='bucket_name',
                       type=str,
                       default=BUCKET_NAME,
                       help='bucket name')
        opt.add_option('--local-path',
                       dest="local_path",
                       type=str,
                       default=UPLOAD_DIR,
                       help="sync source local path")
        opt.add_option('--pref-path',
                       dest="path_pref",
                       type=str,
                       default="",
                       help="sync to pref file path")
        (options, args) = opt.parse_args()
        is_valid_paras = True
        error_messages = []
        bucket = options.bucket_name
        local_path = options.local_path
        path_pref = options.path_pref
        if not bucket:
            error_messages.append("bucket must be set;")
            is_valid_paras = False
        if not local_path:
            error_messages.append("local path must be set;")
            is_valid_paras = False

        if is_valid_paras:
            user_paras = {"bucket_name": bucket, "local_path": local_path, "path_pref": path_pref}
            return user_paras
        else:
            for error_message in error_messages:
                print(error_message)
                opt.print_help()
            return None
    except Exception, error:
        logger.info("get_user_paras error_msg:{0}".format(error))
        return None


def init_log(*logfiles):
    for log in logfiles:
        commands.getoutput('cat /dev/null > %s' % log)
        logger.info("Empty %s finished!" % log)


if __name__ == "__main__":
    logger.critical("========[Start to Upload]========")
    try:
        user_paras = get_user_paras()
        if not user_paras:
            sys.exit(0)

        global UPLOAD_DIR, BUCKET_NAME
        UPLOAD_DIR = user_paras['local_path']
        BUCKET_NAME = user_paras['bucket_name']

        if not os.path.isdir(UPLOAD_DIR):
            logger.error("Invalid upload path: %s", UPLOAD_DIR)
            sys.exit(2)

        if not init_db_connection():
            sys.exit(3)

        if not ensure_table_created():
            logger.critical("DB does not exist and create failed!")
            sys.exit(4)

        if not init_s3_connection():
            sys.exit(5)

        # 清空之前记录失败文件的日志
        init_log(UPLOAD_MOS_FAILED_LOG, UPLOAD_ORACLE_FAILED_LOG)
        # 开始遍历目录下的文件，执行上传到S3和记录到数据库
        iterate_over_directory_process(UPLOAD_DIR, upload_file_to_s3)

        logger.critical("========[Upload finished!]========")
    except Exception, error:
        logger.critical("Executing script error_msg:{}".format(error))
    finally:
        if S3_CONN:
            S3_CONN.close ()

    logger.critical("========[Exit Uploading]========")
