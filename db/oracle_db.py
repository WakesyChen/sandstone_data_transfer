#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import time
from random import randint
import cx_Oracle
from db.base_db import CommonBaseDB as BaseDB
from util import logger


class CommonOracleDB(BaseDB):
    '''Oracle对应的方法'''

    def __init__(self, conf_file):
        '''
        :param conf_file: 数据库的配置文件
        '''
        super(CommonOracleDB, self).__init__(conf_file)
        self.connection = None
        self.cursor = None
        self.table = ''
        self.build_connection()

    def __del__(self):
        # 析构，对象没被调用，关闭连接
        if self.cursor:
            self.cursor.close()
            logger.info("---Close Oracle cursor---")

        if self.connection:
            self.connection.close()
            logger.info("---Close Oracle connection---")

    def count_time(func):
        '''计算某个方法执行的时间
        '''
        def inner(*args, **kwargs):
            time_start = time.time()
            function = func(*args, **kwargs)
            time_end = time.time() - time_start
            logger.critical(">>>>>Excuting oracle [%s] spent %0.5f seconds." % (str(func.__name__), time_end))
            return function

        return inner

    @count_time
    def build_connection(self):
        """创建ORACLE数据库的连接"""
        try:
            if self.db_info:
                self.connection = cx_Oracle.connect(self.db_info['user'], self.db_info['passwd'], "%s:%s/%s" %
                                                    (self.db_info['host'], int(self.db_info['port']),
                                                     self.db_info['db_name']))
                self.table = self.db_info['default_table']
                self.cursor = self.connection.cursor()  # 公共cursor
            else:
                logger.error("Building oracle connection failed! DB config information is not correct!")
        except Exception , error:
            logger.critical("Building oracle connection failed, error_msg: %s" % error)

    def get_connection(self):
        return self.connection

    def ensure_table_created(self):
        '''重写父类建表方法
        :param table_name: 表名
        '''
        # 文件名  源文件全路径   对象存储路径  上传节点主机名  上传节点IP    MD5
        create_sql = """CREATE TABLE  %s
                    (FILENAME           VARCHAR2(1024),
                     FULLSOURCEPATH     VARCHAR2(3600) PRIMARY KEY,
                     CLOUDPATH          VARCHAR2(3600),
                     UPLOADHOSTNAME     VARCHAR2(256),
                     UPLOADIP           VARCHAR2(128),
                     UPLOADTIME         VARCHAR2(128),
                     MD5ID              VARCHAR2(128)
                     )""" % self.table
        try:
            if self.connection:
                cursor = self.cursor if self.cursor else self.connection.cursor()
                cursor.execute(create_sql)
                self.connection.commit()
            else:
                logger.error("Oracle connection hasn\'t been established!")
        except Exception, error:
            if "ORA-00955" not in str(error):
                logger.error("***Create table %s failed,error_msg:%s" % (self.table, error))
                return False
        return True

    def insert_data(self, datainfo):
        '''重写插入数据方法
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''

        insert_sql = "INSERT INTO %s VALUES(:filename, :fullsourcepath, :cloudpath, :uploadhostname, :uploadip, :uploadtime, :md5id)" % self.table
        if self.connection:
            cursor = self.cursor if self.cursor else self.connection.cursor()
            cursor.execute(insert_sql, datainfo)
            self.connection.commit()
            return True
        return False


    def select_count(self, cols=[], where='', datainfo={}):
        '''重写查询数量的方法
        :return: 查询结果数量
        '''
        count, sql_status = 0, 0 # sql_status，0表示查询正常，1表示查询异常
        try:
            # 针对广汽具体实现
            query_sql = "select FULLSOURCEPATH from NAS_FILE_UPLOAD_STATUS  where FULLSOURCEPATH = :fullsourcepath"

            if self.connection:
                cursor = self.connection.cursor()  # 查询的时候不复用，避免数据不正确
                cursor.execute(query_sql, datainfo)
                item = cursor.fetchone()
                if item:
                    count = 1
        except Exception , error:
            sql_status = 1
            logger.error("Failure: Query db error: %s, fullpath: %s" % (error, datainfo.get('fullpath')))
        return count, sql_status

    @count_time
    def select_normal(self, cols=[], where=''):
        '''重写查询出所有满足条件的数据方法
        :param cols: 待查询的列，列表类型，如：['*']或者['name','age']
        :param table: 待查询的表
        :param where: 待查询的条件，如："name = 'Wakesy' and age > 23"
        :return:
        '''
        results = None
        try:
            cols_str = ','.join(cols)
            select_sql = "SELECT %s FROM %s WHERE %s ;" % (cols_str, self.table, where)
            # print "query_sql:", query_sql
            conn = self.connection
            if conn:
                cursor = self.connection.cursor()  # 查询的时候不复用，避免数据不正确
                cursor.execute(select_sql)
                results = cursor.fetchall()
            else:
                logger.error("DB connection hasn\'t been established!")
        except Exception , error:
            logger.error("***Selecting data failed! error_msg:%s" % error)
        return results


    def create_index(self, index_name, target_colonm):
        try:
            sql = "CREATE INDEX %s ON %s(%s)" % (index_name, self.table, target_colonm)
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute(sql)
                logger.info('Create index succeeded!')
            else:
                logger.error("DB connection hasn\'t been established!")
        except Exception as error:
            logger.error('Create index Failed! error:%s' % error)


    def drop_index(self, index_name):
        try:
            sql = "DROP INDEX %s" % index_name
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute(sql)
                logger.info('Drop index succeeded!')
            else:
                logger.error("DB connection hasn\'t been established!")
        except Exception as error:
            logger.error("Drop index Failed! error: %s" % error)


