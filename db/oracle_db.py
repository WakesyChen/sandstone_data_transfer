#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import time
import traceback
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
        """创建MYSQL数据库的连接"""
        try:
            if self.db_info:
                self.connection = cx_Oracle.connect(self.db_info['user'], self.db_info['passwd'], "%s:%s/%s" %
                                                    (self.db_info['host'], int(self.db_info['port']),
                                                     self.db_info['db_name']))
                self.cursor = self.connection.cursor()  # 公共cursor
            else:
                logger.error("Create db connection failed! DB config information is not correct!")
        except Exception , error:
            logger.critical("***Building oracle connection failed,error_msg:[%s]" % error)

    def get_connection(self):
        return self.connection

    def ensure_table_created(self, table):
        '''重写父类建表方法
        :param table_name: 表名
        '''

        # 文件名  源文件全路径   对象存储路径  上传节点主机名  上传节点IP    MD5
        create_sql = """CREATE TABLE  %s
                    (FILENAME           VARCHAR2(1024),
                     FULLSOURCEPATH     VARCHAR2(3600),
                     CLOUDPATH          VARCHAR2(3600) PRIMARY KEY,
                     UPLOADHOSTNAME     VARCHAR2(256),
                     UPLOADIP           VARCHAR2(128),
                     UPLOADTIME         VARCHAR2(128),
                     MD5ID              VARCHAR2(128)
                     )""" % table
        try:
            if self.connection:
                cursor = self.cursor if self.cursor else self.connection.cursor()
                cursor.execute(create_sql)
                self.connection.commit()
            else:
                logger.error("Oracle connection hasn\'t been established!")
        except Exception, error:

            if "ORA-00955" not in str(error):
                logger.error("***Create table [%s] failed,error_msg:[%s]" % (table, error))
                return False
        return True

    def insert_data(self, table, datainfo):
        '''重写插入数据方法
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''
        try:

            insert_sql = "INSERT INTO %s VALUES(:filename, :fullsourcepath, :cloudpath, :uploadhostname, :uploadip, :uploadtime, :md5id)" % table
            # print "insert_sql:", insert_sql
            if self.connection:
                cursor = self.cursor if self.cursor else self.connection.cursor()
                cursor.execute(insert_sql, datainfo)
                self.connection.commit()
                logger.info("Inserted data successfully!")
                return True
            else:
                logger.error("Inserting data Failed! Mysql connection hasn\'t been established!")
        except Exception , error:
            logger.critical("***Inserting data to [%s] Failed:[%s]" % (table, error))
        return False

    def select_count(self, cols=[], table='', where=''):
        '''重写查询数量的方法
        :param cols: 待查询的列，列表类型，如：['*']或者['name','age']
        :param table: 待查询的表
        :param where: 待查询的条件，如："name = 'Wakesy' and age > 23"
        :return: 查询结果数量
        '''
        count = 0
        try:
            cols_str = ','.join(cols)
            query_sql = "SELECT %s FROM %s WHERE %s" % (cols_str, table, where)  # 查询数量，检索出一个字段即可
            # print "query_sql:", query_sql
            if self.connection:
                cursor = self.connection.cursor()  # 查询的时候不复用，避免数据不正确
                cursor.execute(query_sql)
                item = cursor.fetchone()
                if item:
                    count = 1
        except Exception , error:
            logger.critical("***Selecting count failed! error_msg: [{}]".format(error))
        return count

    @count_time
    def select_normal(self, cols=[], table='', where=''):
        '''重写查询出所有满足条件的数据方法
        :param cols: 待查询的列，列表类型，如：['*']或者['name','age']
        :param table: 待查询的表
        :param where: 待查询的条件，如："name = 'Wakesy' and age > 23"
        :return:
        '''
        results = None
        try:
            cols_str = ','.join(cols)
            select_sql = "SELECT %s FROM %s WHERE %s ;" % (cols_str, table, where)
            # print "query_sql:", query_sql
            conn = self.connection
            if conn:
                cursor = self.connection.cursor()  # 查询的时候不复用，避免数据不正确
                cursor.execute(select_sql)
                results = cursor.fetchall()
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except Exception , error:
            logger.error("***Selecting data failed! error_msg:%s" % error)
        return results


if __name__ == '__main__':
    DB_CONF = 'common_db.conf'
    TABLE_NAME = 'mysql_test'
    test_data = {"id": randint(50, 150),
                 "name": "Jonhsy",
                 "age": 23,
                 "level": 12,
                 "description": u"在山的那边海的那边，有一群白精灵，他们活泼又聪明！".encode('utf8')
                 }
    colomns = ['*']
    # where = "name in ('wakesy', 'Coco','Johnsy')"
    where = ""
    mysql_db = CommonOracleDB(DB_CONF)
    mysql_db.create_table(TABLE_NAME)
    mysql_db.insert_data(TABLE_NAME, **test_data)
    mysql_db.select_count(cols=['*'], table=TABLE_NAME, where=where)
    print "-------------------"
    mysql_db.select_normal(cols=['*'], table=TABLE_NAME, where=where)
