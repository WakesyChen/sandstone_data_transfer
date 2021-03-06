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
sys.setdefaultencoding('utf-8')

"""
二、脚本说明--------第二次，尝试重传上次上传失败的文件
    读取上次保存的，上传到s3和数据库失败文件的两个日志,重新上传
"""

# S3配置
BUCKET_PREF = ""
S3_CONN = None
UPLOAD_DIR = DEFAULT_UPLOAD_DIR


# 数据库配置
db_instance = None
db_connection = None
db_conf = DEFAULT_DB_CONF
db_type = None

#============================S3相关操作========================

def init_s3_connection():
    try:
        global AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT, S3_CONN
        S3_CONN = get_s3_connection(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT)
        return True
    except Exception,error:
        logger.critical("init_s3_connection s3 failed: %s" % str(error))
        return False


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


def get_hostname_and_ipaddr():
    try:
        hostname = socket.gethostname()
        ip_info = socket.gethostbyname_ex(hostname)
        logger.debug("IPinfo: %s" % str(ip_info))
        ipaddr = ip_info[2][0]
        logger.debug("local ip addr: %s"%ipaddr)
        return hostname, ipaddr
    except Exception,error:
        logger.critical("get hostname ip error: %s" % str(error))
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


def upload_file_to_s3(cloud_path, file_path):
    if is_file_uploaded(file_path):
        logger.info("FOUND upload record in DB, file path: %s", file_path)
        return True

    try:
        md5id = GetFileMd5(file_path)
        if not S3_CONN:
            init_s3_connection()
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
            logger.info("SUCCESS to S3: multipart way,  uploading file path: %s" % file_path)
        else:
            kobject.set_contents_from_filename(file_path, headers={'CONTENT-MD5': md5id})
            logger.info("SUCCESS to S3: singlefile way, uploading file path: %s" % file_path)
        # 插入记录到数据库
        insert_file_record_to_db(cloud_path, file_path)

    except Exception, e:
        S3_CONN.close()
        logger.error("FAILURE to S3: error: %s, uploading file path: %s" % (e, file_path))
        return False

    return True


def reupload_failed_files(*failed_file_logs):

    for failed_log in failed_file_logs:
        # 失败日志文件存在，且不为空,否则跳过
        if not os.path.isfile(failed_log) or (os.path.isfile(failed_log) and os.path.getsize(failed_log) == 0):
            logger.info("FAILURE: Reuploading failed, log_path [%s] does\'t exist or is empty." % failed_log)
            continue
        failed_file = "first file path" # 开启循环
        try:
            logger.info("***Start to read file: [%s]" % failed_log)
            with open(failed_log, "r") as f:
                # 逐条读出，失败记录中的文件路径
                while failed_file:
                    failed_file = f.readline().strip()

                    if not os.path.isfile(failed_file):
                        logger.info("FAILURE: Not correct file path: {0}".format(failed_file))
                        continue
                    cloud_path = failed_file[len(UPLOAD_DIR):]
                    upload_file_to_s3(cloud_path, failed_file)

        except Exception as err :
            logger.error("***Reupload failed, error: %s, file: %s" % (err, failed_file))
    logger.info("***Upload failed_files finished!")

#==========================数据库相关操作======================

def init_db_connection():
    """根据配置文件，初始化选择数据库，以及对应的方法"""
    global db_type, db_instance, db_connection
    try:
        db_conf_obj = configobj.ConfigObj(db_conf)
        db_type = db_conf_obj['common']['DB_TYPE']

        if db_type == 'oracle':
            db_instance = CommonOracleDB(db_conf)
        elif db_type == 'mysql':
            db_instance = CommonMysqlDB(db_conf)
        else:
            logger.error("init_db_connection failed, invalid DB_TYPE:%s" % db_type)
            return False

        if db_instance.get_connection(): # 创建数据库连接
            return True
    except Exception, error:
        logger.critical("init_db_connection failed, error:[%s]" % error)
    return False


def is_file_uploaded(fullsourcepath):
    '''查询数据库上传记录，根据结果数目，判断文件是否上传'''
    cols = ["FULLSOURCEPATH"] # 待查询的列
    where = ''
    query_count, sql_status = db_instance.select_count(cols=cols, where=where, datainfo={'fullsourcepath': fullsourcepath})
    if sql_status == 0 and query_count > 0:
        return True
    else:
        return False


def add_record_to_db(datainfo, fullpath):
    '''上传到S3成功后，添加记录到数据库中'''
    insert_status = db_instance.insert_data(datainfo)
    return insert_status


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
            logger.info("SUCCESS: insert file to oracle db: %s" % fullpath)

    except Exception,e:
        logger.error("FAILURE: insert file record error: %s, file: %s" % (str(e), fullpath))
        return False
    return True


def ensure_table_created():
    return db_instance.ensure_table_created()

#==========================公共方法===========================



if __name__ == "__main__":
    logger.critical("========[Start to ReUpload]========")
    try:

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

        # 开始读取上次上传到s3和db，失败文件的两个日志,重新上传
        reupload_failed_files(UPLOAD_MOS_FAILED_LOG, UPLOAD_DB_FAILED_LOG)

        logger.critical("========[ReUpload finished!]========")
    except Exception, error:
        logger.critical("Executing script error_msg:{}".format(error))
    finally:
        if S3_CONN:
            S3_CONN.close ()
        logger.critical("========[Exit ReUploading]========")
