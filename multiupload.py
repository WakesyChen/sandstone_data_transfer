#!/var/lib/sdsom/venv/bin/python
# -*- coding: utf-8 -*-

import shutil
import math
import string
import io
from io import BytesIO
import os
from os import path
import sys
import boto
import boto.s3.connection
from filechunkio import FileChunkIO
import threading
import Queue

from config import CHUNK_SIZE
from config import MULTI_THREAD_NUM
from config import MULTI_UPLOAD_THRESHOLD_SIZE


class Chunk:
    num = 0
    offset = 0
    len = 0
    def __init__(self, n, o, l):  
        self.num = n
        self.offset = o
        self.len = l


def init_queue(filesize):
    chunkcnt = int(math.ceil(filesize*1.0/CHUNK_SIZE))
    q = Queue.Queue(maxsize = chunkcnt)
    for i in range(0,chunkcnt):
        offset = CHUNK_SIZE*i
        len = min(CHUNK_SIZE, filesize-offset)
        c = Chunk(i+1, offset, len)
        q.put(c)
    return q

def upload_chunk_func(filepath, mp_handler, chunk_queue, id, md5id):
    while (not chunk_queue.empty()):
        chunk = chunk_queue.get()
        #print "Thread id: %s, chunk offset: %s, len: %s" % (id, chunk.offset, chunk.len)
        fp = FileChunkIO(filepath, 'r', offset=chunk.offset, bytes=chunk.len)
        mp_handler.upload_part_from_file(fp, headers={'CONTENT-MD5' : md5id}, part_num=chunk.num)
        fp.close()
        chunk_queue.task_done()

    #print "UPload thread: %s, done !" % id

def upload_file_multipart(filepath, keyname, bucket, md5id, threadcnt=MULTI_THREAD_NUM):
    filesize = os.stat(filepath).st_size
    mp_handler = bucket.initiate_multipart_upload(keyname)
    chunk_queue = init_queue(filesize)
    for i in range(0, threadcnt):
        t = threading.Thread(target=upload_chunk_func, args=(filepath, mp_handler, chunk_queue, i, md5id))
        t.setDaemon(True)
        t.start()

    #block all upload thread until complete upload
    chunk_queue.join()
    mp_handler.complete_upload()


'''
access_key = "P1FR6AD7OYYIXQI3SQ8G"
secret_key = "fNKtVpSd9W6cBUgN4YpjEuDOOIqswMKMSCMK1eQf"
host = "192.168.4.42"
port=3344

filepath = "/root/sandstone-v3.1-installer.tar.gz.release.86"
#filepath = "/root/installbuilder-professional-9.0.2-linux-x64-installer.run"
keyname = "bigfile"
conn = boto.connect_s3(
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key,
    host = host,
    port = port,
    is_secure=False,
    calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )

bucket = conn.get_bucket("test")
upload_file_multipart(filepath, keyname, bucket, MULTI_THREAD_NUM)
'''
