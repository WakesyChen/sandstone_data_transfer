# -*- coding:utf-8 -*-
import logging

#===========================MOS相关配置==================================
#EOS用户AK/SK
AWS_ACCESS_KEY_ID = 'NV0SMJ58EBTTFGIMWZ84'
AWS_ACCESS_KEY_SECRET = 'tESHEnM7qA4JBRZm6O5TrU3v73zImznMEVHz7SAA'
#EOS服务IP
HOST = "10.10.7.151"
#EOS服务端口号
PORT = 6624
#EOS上传目标桶名称
BUCKET_NAME = 'test'

UPLOAD_PROCESS_NUM = 5
MULTI_THREAD_NUM = 5
CHUNK_SIZE = 4*1024*1024
MULTI_UPLOAD_THRESHOLD_SIZE = 50*1024*1024


#===========================日志相关配置==========================================

# 日志目录
LOG_DIR = "./log/"
# 日志文件前缀（后缀是日期，格式：upload_files_to_MOS.log2017-06-23-20-14）
LOG_NAME_PREFIX = "upload_files_to_MOS.log_"
# 文件日志级别
FILE_LOG_LEVEL = logging.INFO
# 控制台日志级别
CONSOLE_LOG_LEVEL = logging.ERROR

# 记录上传到MOS失败的日志路径
UPLOAD_MOS_FAILED_LOG = LOG_DIR + LOG_NAME_PREFIX  + "failed_s3"
# 记录记录到ORACLE失败的日志路径
UPLOAD_ORACLE_FAILED_LOG = LOG_DIR + LOG_NAME_PREFIX + "failed_db"
# 重新上传之前失败的文件，仍然上传失败，则记录到第三个文件
REUPLOAD_FAILED_LOG = LOG_DIR + LOG_NAME_PREFIX + "failed_re-upload"

# 本地待上传目录，注意windows的反斜杠需要换位linux格式的正斜杠
DEFAULT_UPLOAD_DIR = "/var/lib/sdsom/venv/lib/python2.7/site-packages/sdsom-core-py2.7.egg/sdsom"
# 默认数据库配置文件
DEFAULT_DB_CONF = './db/db.conf'



