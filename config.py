# -*- coding:utf-8 -*-
import logging
from random import randint
#===========================MOS相关配置==================================

#EOS用户AK/SK
AWS_ACCESS_KEY_ID = 'H36CODNFKUI0OZYEQITL'
AWS_ACCESS_KEY_SECRET = 'lMAeJBolLEDrdEO2cj8tjj8DpaZZ9B99lcLuA8gN'
#RGW服务IP
HOST = "10.10.5.153"
#RGW服务端口号
PORT = 8080
#EOS上传目标桶名称
BUCKET_NAME = 'test'

UPLOAD_PROCESS_NUM = 5
MULTI_THREAD_NUM = 5
CHUNK_SIZE = 4*1024*1024
# 大于50M分片上传
MULTI_UPLOAD_THRESHOLD_SIZE = 50*1024*1024


#===========================日志相关配置=====================================

# 日志目录
LOG_DIR = "./log/"
# 日志文件前缀（后缀是日期，格式：upload_files_to_MOS.log2017-06-23-20-14）
LOG_NAME_PREFIX = "upload_files_to_MOS.log_"
# 文件日志级别
FILE_LOG_LEVEL = logging.INFO
# 控制台日志级别
CONSOLE_LOG_LEVEL = logging.ERROR


# 当前执行脚本后，生成的即时log标识
CURRENT_INDEX = "current_"
#记录所有遍历到的文件路径
ALL_FILES_FOUND_LOG = LOG_DIR + CURRENT_INDEX + "all_file_found"
# 记录上传到MOS失败的文件路径
UPLOAD_MOS_FAILED_LOG = LOG_DIR + CURRENT_INDEX + "failed_s3"
# 记录记录到db失败的文件路径
UPLOAD_DB_FAILED_LOG = LOG_DIR + CURRENT_INDEX + "failed_db"


#==============================其他配置======================================
# 默认数据库配置文件
DEFAULT_DB_CONF = './db/db.conf'

# 本地待上传目录，注意windows的反斜杠需要换位linux格式的正斜杠
DEFAULT_UPLOAD_DIR = "/var/lib/sdsom/venv/lib/python2.7/site-packages/sdsom-core-py2.7.egg/sdsom"








