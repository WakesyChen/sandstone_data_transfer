# -*- coding:utf-8 -*-
import re
import sys
import boto
import socket
import os.path
import datetime
import hashlib
import signal
import os
import cx_Oracle
import boto.s3
import boto.s3.connection
from boto.s3.key import Key
from config import *
from util import logger
from optparse import OptionParser
from multiupload import upload_file_multipart
import traceback
reload(sys) 
sys.setdefaultencoding('gbk')

ORACLE_CONNECTION = None
ORACLE_CURSOR = None
UPLOAD_DIR = DEFAULT_UPLOAD_DIR
BUCKET_PREF = ""
IS_MOVE = False
CONN = None

def init_db_connection():
    try:
        global ORACLE_CONNECTION
        global ORACLE_CURSOR
        ORACLE_CONNECTION = cx_Oracle.connect(ORACLE_USER, ORACLE_PASSWORD, '%s:%s/%s'%(ORACLE_SERVER_IP, ORACLE_SERVER_PORT, ORACLE_DBNAME))
        ORACLE_CURSOR = ORACLE_CONNECTION.cursor()

        return True
    except Exception,e:
        logger.critical("Connect Oracle DB failed: %s"%str(e))
        return False

def init_s3_connection():
    try:
        global AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT, CONN
        CONN = get_connection(AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY_SECRET, HOST, PORT)
        return True
    except Exception,e:
        logger.critical("Connect s3 failed: %s"%str(e))
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


def get_connection(access_key, secret_key, host, port):
    conn = boto.connect_s3(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        host=host,
        port=port,
        is_secure=False,
        calling_format=boto.s3.connection.OrdinaryCallingFormat(),)
    return conn



def reupload_failed_files():

    failed_file_logs = [UPLOAD_MOS_FAILED_LOG, UPLOAD_ORACLE_FAILED_LOG]
    for failed_log in failed_file_logs:
        # 失败日志文件存在，且不为空,否则跳过
        if not os.path.isfile(failed_log) or (os.path.isfile(failed_log) and os.path.getsize(failed_log) == 0):
            logger.info("***Reupload failed: [%s] does\'t exist or is empty." % failed_log)
            continue
        failed_file = "first file path" # 开启循环
        try:
            logger.info("***Start read :[%s]" % failed_log)
            with open(failed_log, "r") as f:
                # 逐条读出，失败记录中的文件路径
                while failed_file:
                    failed_file = f.readline().strip()
                    if os.path.isfile(failed_file):
                        cloud_path = failed_file[len(UPLOAD_DIR):]
                        if not is_file_uploaded(cloud_path):
                            upload_file_to_s3(cloud_path, failed_file)
                    else:
                        logger.info("***Path '{}' is not a correct file path.".format(failed_file))
                        # 读出的不是文件路径，则读取下一条
                        continue
        except:
            logger.error("***Upload [%s] failed, error: %s" % (failed_file, traceback.format_exc()))
    logger.info("***Upload failed_files finished!")


def upload_file_to_s3(cloud_path, file_path):
    if is_file_uploaded(cloud_path):
        logger.info("File: %s has already been uploaded, go on for next file!", cloud_path)
        return True

    md5id = GetFileMd5(file_path)
    try:
        global BUCKET_PREF
        bucket = CONN.get_bucket(BUCKET_NAME, validate=False)
        object_path = cloud_path.replace("\\", "/")

        if object_path[0] == '/':
            object_path = BUCKET_PREF + object_path
        else:
            object_path = BUCKET_PREF + '/' + object_path

        kobject = bucket.new_key(object_path)
        filesize = os.stat(file_path).st_size
        if filesize >= MULTI_UPLOAD_THRESHOLD_SIZE:
            upload_file_multipart(file_path, object_path, bucket, md5id)
            logger.info("SUCCESS: multipart way, uploading destination object path: %s"%object_path)
        else:
            kobject.set_contents_from_filename(file_path, headers={'CONTENT-MD5' : md5id})
            logger.info("SUCCESS: singlefile way, uploading destination object path: %s"%object_path)


        insert_file_record_to_db(cloud_path,  file_path)


    except Exception,e:
        CONN.close()
        logger.error("FAILURE: upload file: %s error: %s" % (cloud_path, str(e)))
        collect_failed_files(file_path, REUPLOAD_FAILED_LOG)
        return False

    return True

def collect_failed_files(file_path, failed_log):
    '''
    :param failed_log: 记录上传失败的文件日志路径
    :param file_path: 上传失败的文件路径
    '''
    import  commands
    if os.path.isfile(failed_log):
        try:
            grep_cmd = "echo '{}' >> {}".format(file_path, failed_log)
            ret_code, result = commands.getoutput(grep_cmd)
            if ret_code != 0:
                logger.error("FAILUER: Write failed file path [{0}] to [{1}] failed!".format(file_path, failed_log))
        except:
            logger.error(traceback.format_exc())



def is_file_uploaded(cloudpath):
    try:
        query_sql = "select * from NAS_FILE_UPLOAD_STATUS  where CLOUDPATH='%s'"%cloudpath
        ORACLE_CURSOR.execute(query_sql)
        row_list = ORACLE_CURSOR.fetchall()
        if len(row_list) == 0:
            logger.debug("Not uploaded, file: %s", cloudpath)
            return False

        logger.debug("UPLOADED file: %s",  cloudpath)
        return True
    except Exception,e:
        logger.critical("Query db error: %s" % str(e))
        return False

def add_record_to_oracle_db(datainfo, fullpath):
    try:
        insert_sql = "INSERT INTO NAS_FILE_UPLOAD_STATUS VALUES(:filename, :fulluploadpath, :cloudpath, :hostname, :uploadip, :uploadtime, :md5id)"
        # insert_sql = "INSERT INTO NAS_FILE_UPLOAD_STATUS VALUES(:filename, :fulluploadpath, :cloudpath, :hostname, :uploadip, :uploadtime, :md5id, :isuploaded, :isdelete)"
        ORACLE_CURSOR.execute (insert_sql, datainfo)
        ORACLE_CONNECTION.commit()
        return True
    except Exception,e:
        logger.critical("Insert db error: %s, datainfo: %s" % (str(e), str(datainfo)))
        # 记录上传失败的文件日志路径
        collect_failed_files(fullpath, REUPLOAD_FAILED_LOG)
        return False

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
        datainfo['fulluploadpath'] = fullpath
        datainfo['cloudpath'] = cloud_path
        datainfo['hostname'] = hostname
        datainfo['uploadip'] = ipaddr
        datainfo['uploadtime'] = str(datetime.datetime.now())
        datainfo['md5id'] = GetFileMd5(fullpath)
        if add_record_to_oracle_db(datainfo, fullpath):
            logger.info("SUCCESS: insert file to oralce db: %s" % fullpath)

    except Exception,e:
        logger.error("FAILURE: insert file record error: %s, file: %s" % (str(e), fullpath))
        # 记录上传失败的文件日志路径
        collect_failed_files(fullpath, REUPLOAD_FAILED_LOG)
        return False

    return True

def ensure_table_created():
    # 文件名  源文件全路径   对象存储路径  上传节点主机名  上传节点IP    MD5
    create_sql = """CREATE TABLE NAS_FILE_UPLOAD_STATUS
                (FILENAME           VARCHAR2(1024),\
                 FULLSOURCEPATH     VARCHAR2(3600),\
                 CLOUDPATH          VARCHAR2(3600) primary key,\
                 UPLOADHOSTNAME     VARCHAR2(256),\
                 UPLOADIP           VARCHAR2(128),\
                 UPLOADTIME         VARCHAR2(128),\
                 MD5ID              VARCHAR2(128)\
                 )"""

    try:
        ORACLE_CURSOR.execute(create_sql)
        ORACLE_CONNECTION.commit()
    except Exception,e:
        if "ORA-00955" not in str(e):
            logger.error("FAILURE: create table: %s, error: %s"%("NAS_FILE_UPLOAD_STATUS", str(e)) )
            return False

    return True


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
    except Exception as ex:
        print("exception :{0}".format(str(ex)))
        return None

def init_log(*logfiles):
    import commands
    for log in logfiles:
        commands.getoutput('cat /dev/null > %s' % log)
        logger.info("Empty %s finished!" % log)


if __name__ == "__main__":
    logger.info("========[Start upload failed files]========")
    try:
        user_paras = get_user_paras()
        if user_paras is None:
            sys.exit(0)

        UPLOAD_DIR = user_paras['local_path']
        BUCKET_NAME = user_paras['bucket_name']

        if not os.path.isdir(UPLOAD_DIR):
            logger.error("Invalid upload path: %s", UPLOAD_DIR)
            sys.exit(2)

        if not init_db_connection():
            sys.exit(3)

        if not ensure_table_created():
            logger.critical("Oracle db not exists and create failed!")
            sys.exit(4)

        if not init_s3_connection():
            sys.exit(5)

        # 清空记录之前二次上传，失败文件的日志
        init_log(REUPLOAD_FAILED_LOG)
        # 开始读取上次上传到s3和oracle，失败文件的两个日志,重新上传
        reupload_failed_files()


    except Exception,e:
        logger.error("Error: %s", e)
    finally:
        if ORACLE_CURSOR:
            ORACLE_CURSOR.close ()
        if ORACLE_CONNECTION:
            ORACLE_CONNECTION.close ()
        if CONN:
            CONN.close ()


    logger.info("========[Exit upload failed files]========")
