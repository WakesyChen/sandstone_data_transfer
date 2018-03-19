#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import time
import MySQLdb
from db.base_db import CommonBaseDB as BaseDB
from util import logger


class CommonMysqlDB(BaseDB):
    def __init__(self, conf_file):
        '''
        :param conf_file: 数据库的配置文件
        '''
        super(CommonMysqlDB, self).__init__(conf_file)
        self.connection = None
        self.cursor = None
        self.table = ''
        self.build_connection()

    def __del__(self):
        # 析构，对象没被调用，关闭连接
        if self.cursor:
            self.cursor.close()
            logger.info("---Close Mysql cursor---")

        if self.connection:
            self.connection.close()
            logger.info("---Close Mysql connection---")

    def count_time(func):
        '''计算某个方法执行的时间'''

        def inner(*args, **kwargs):
            time_start = time.time()
            function = func(*args, **kwargs)
            time_end = time.time() - time_start
            logger.critical(">>>>>Excuting Mysql [%s] spent %0.5f seconds." % (str(func.__name__), time_end))
            return function

        return inner

    def get_values_str(self, datainfo):
        '''针对特定的表字段，按顺序排好对应的值
        参数对应的键：filename, fullsourcepath, cloudpath, uploadhostname, uploadip, uploadtime, md5id
        表对应的字段：FILENAME, FULLSOURCEPATH, CLOUDPATH, UPLOADHOSTNAME, UPLOADIP, UPLOADTIME, MD5ID
        :return: 格式化成的 '"V1","V2","V3"'这样的字符串
        '''
        try:
            cols = ['FILENAME', 'FULLSOURCEPATH', 'CLOUDPATH', 'UPLOADHOSTNAME', 'UPLOADIP', 'UPLOADTIME', 'MD5ID']
            clo_values = [datainfo[col.lower()] for col in cols]
            clo_values_foramt_str = ','.join(['"%s"' % value for value in clo_values])
            return clo_values_foramt_str
        except:
            logger.critical("***Formatting table colomn values failed!")
            return ''

    @count_time
    def build_connection(self):
        """创建MYSQL数据库的连接"""
        try:
            if self.db_info:
                self.connection = MySQLdb.connect(host=self.db_info['host'], port=int(self.db_info['port']),
                                                  user=self.db_info['user'],
                                                  passwd=self.db_info['passwd'], db=self.db_info['db_name'],
                                                  charset='utf8')
                self.table = self.db_info['default_table']
                self.cursor = self.connection.cursor()  # 创建一个公共cursor
        except Exception, error:
            logger.critical("***Building mysql connection failed,error_msg:[%s]" % error)

    def get_connection(self):
        '''对外提供的获取connection方法'''
        return self.connection

    def ensure_table_created(self, table):
        '''重写父类建表方法
        :param table_name: 表名
        ENGINE=InnoDB DEFAULT CHARSET=utf8 不带这条无法插入中文
        '''
        # 文件名  源文件全路径   对象存储路径  上传节点主机名  上传节点IP    MD5
        create_sql = """CREATE TABLE IF NOT EXISTS %s
                    (FILENAME           VARCHAR(500),
                     FULLSOURCEPATH     VARCHAR(700) PRIMARY KEY,
                     CLOUDPATH          VARCHAR(500),
                     UPLOADHOSTNAME     VARCHAR(256),
                     UPLOADIP           VARCHAR(128),
                     UPLOADTIME         VARCHAR(128),
                     MD5ID              VARCHAR(128)
                     )""" % self.table

        try:
            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                cursor.execute(create_sql)
                conn.commit()
                return True
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except Exception, error:
            logger.critical("***Creating table %s failed! ,error:[{}]".format(error))
        return False

    def insert_data(self, datainfo):
        '''重写插入数据方法
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''

        values_str = self.get_values_str(datainfo)
        if not values_str:  # 格式化插入的数据失败，则插入失败
            return False
        insert_sql = "INSERT INTO %s  VALUES (%s) ;" % (self.table, values_str)  # 拼接插入语句
        logger.debug("insert_sql:%s" % insert_sql)

        conn = self.connection
        if conn:
            cursor = self.cursor if self.cursor else conn.cursor()
            cursor.execute(insert_sql)
            conn.commit()
            logger.info("Inserted data successfully!")
            return True
        return False

    def select_count(self, cols=[], where='', datainfo={}):
        '''重写查询数量的方法
        :param cols: 待查询的列，列表类型，如：['*']或者['name','age']
        :param table: 待查询的表
        :param where: 待查询的条件，如："name = 'Wakesy' and age > 23"
        :return: 查询结果数量
        '''
        count, sql_status = 0, 0  # sql_status，0表示查询正常，1表示查询异常
        try:
            cols_str = ','.join(cols)
            select_sql = "SELECT %s FROM %s WHERE %s ;" % (cols_str, self.table, where)
            # print select_sql
            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                count = cursor.execute(select_sql)
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except Exception, error:
            sql_status = 1
            logger.critical("***Select count failed! error_msg: [{}]".format(error))
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
            logger.debug("select_sql:%s" % select_sql)
            conn = self.connection
            if conn:
                cursor = conn.cursor()
                count = cursor.execute(select_sql)
                logger.debug("select count is %s" % count)
                results = cursor.fetchall()
            else:
                logger.critical("***Mysql connection hasn\'t been established!")
        except Exception, error:
                logger.critical("***Select data failed! error_msg: [{}]".format(error))
        finally:
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

