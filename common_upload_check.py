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


"""
三、脚本说明------最后检查，是否都已上传到MOS
    检查指定路径下的文件，通过查询数据库的记录，打印出没有上传到MOS的文件
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


#==========================S3相关操作==========================

def init_s3_connection():
    try:
        global AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT, S3_CONN
        S3_CONN = get_s3_connection(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT)
        return True
    except Exception, e:
        logger.critical("init_s3_connection failed: %s" % str(e))
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
        collect_files(source_path, ALL_FILE_HANDLER)  # 记录所有遍历到的文件路径，便于分析
        cloud_path = source_path[len(UPLOAD_DIR):]
        processor(cloud_path, source_path)

    elif os.path.isdir(source_path):
        for subfile in os.listdir(source_path):
            new_path=os.path.join(source_path,subfile)
            iterate_over_directory_process(new_path, processor)


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


def check_from_db(fullpath):
    '''查询数据库上传记录，根据结果数目，判断文件是否上传'''
    cols = ["FULLSOURCEPATH"]  # 待查询的列
    where = ''
    query_count, sql_status = db_instance.select_count(cols=cols, where=where, datainfo={'fullsourcepath': fullpath})
    if sql_status == 0 and query_count < 1:
        logger.critical("Not found in db, file: %s " % fullpath)



def collect_files(file_path, file_handler):
    '''
    :param failed_log: 记录上传失败的文件日志路径
    :param file_path: 上传失败的文件路径
    '''
    try:
        file_handler.write(file_path)
        file_handler.write("\n")
    except Exception as err:
        logger.error("Collecting files failed, error: %s , file: %s" % (err, file_path))


def init_log(*logfiles):
    import os
    import time
    import commands
    for logfile in logfiles:
        if os.path.isfile(logfile) and os.path.getsize(logfile) > 0:
            old_log = logfile.replace(CURRENT_INDEX, "old_") + '_' + time.strftime('%Y-%m-%d-%H-%M-%S')
            commands.getoutput('mv %s %s' % (logfile, old_log))
            logger.info("Backup %s finished!" % logfile)

    # 通过write方式写
    global ALL_FILE_HANDLER
    ALL_FILE_HANDLER = open(ALL_FILES_FOUND_LOG, "a")


def ensure_table_created():
    return db_instance.ensure_table_created()


if __name__ == "__main__":
    logger.critical("========[Start to Check]========")
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

        # 备份之前的记录，并开始记录遍历到的所有文件
        init_log(ALL_FILES_FOUND_LOG)
        # 建立索引加速遍历
        db_instance.create_index('default_index', 'FULLSOURCEPATH')
        # 开始遍历
        iterate_over_directory_process(UPLOAD_DIR, check_from_db)
        # 结束后删除索引
        db_instance.drop_index()

        logger.critical("========[Check Finished!]========")
    except Exception, error :
        logger.critical("Executing script error_msg:{}".format(error))
    finally:
        if S3_CONN:
            S3_CONN.close ()
        if ALL_FILE_HANDLER:
            ALL_FILE_HANDLER.close()

    logger.critical("========[Exit Checking]========")
