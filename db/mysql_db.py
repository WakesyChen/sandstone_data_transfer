#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import time
import traceback

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
            logger.info("Excuting [%s] spent %0.5f seconds." % (str(func.__name__), time_end))
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
            logger.critical("Formatting table colomn values failed!")
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
                self.cursor = self.connection.cursor()  # 创建一个公共cursor
        except:
            logger.critical(traceback.print_exc())

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
                    (FILENAME           LONGTEXT,
                     FULLSOURCEPATH     LONGTEXT,
                     CLOUDPATH          LONGTEXT,
                     UPLOADHOSTNAME     VARCHAR(256),
                     UPLOADIP           VARCHAR(128),
                     UPLOADTIME         VARCHAR(128),
                     MD5ID              VARCHAR(128)
                     )""" % table

        try:
            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                cursor.execute(create_sql)
                conn.commit()
                return True
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except:
            logger.critical("***Creating table %s failed! ,error:{}".format(traceback.format_exc()))
        return False

    def insert_data(self, table, datainfo):
        '''重写插入数据方法
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''
        try:
            values_str = self.get_values_str(datainfo)
            if not values_str:  # 格式化插入的数据失败，则插入失败
                return False
            insert_sql = "INSERT INTO %s  VALUES (%s) ;" % (table, values_str)  # 拼接插入语句
            logger.info("insert_sql:%s" % insert_sql)

            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                cursor.execute(insert_sql)
                conn.commit()
                logger.info("Inserted data successfully!")
                return True
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except:
            logger.critical(traceback.print_exc())
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
            select_sql = "SELECT %s FROM %s WHERE %s ;" % (cols_str, table, where)
            # print select_sql
            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                count = cursor.execute(select_sql)
            else:
                logger.error("Mysql connection hasn\'t been established!")
        except:
            logger.critical(traceback.print_exc())
        return count

    @count_time
    def select_normal(self, cols=[], table='', where=''):
        '''重写查询出所有满足条件的数据方法
        :param cols: 待查询的列，列表类型，如：['*']或者['name','age']
        :param table: 待查询的表
        :param where: 待查询的条件，如："name = 'Wakesy' and age > 23"
        :return:
        '''
        cursor = None
        results = None
        try:
            cols_str = ','.join(cols)
            select_sql = "SELECT %s FROM %s ;" % (cols_str, table)
            if where:
                select_sql = select_sql.replace(';', 'WHERE ') + where + ";"
            print select_sql
            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor()
                count = cursor.execute(select_sql)
                print "select count is %s" % count
                results = cursor.fetchall()
                for item in results:
                    print item
            else:
                print "Mysql connection hasn\'t been established!"
        except:
            print "***Selecting data failed!"
            print traceback.print_exc()
        finally:
            return results


if __name__ == '__main__':
    from random import randint

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
    mysql_db = CommonMysqlDB(DB_CONF)
    mysql_db.create_table(TABLE_NAME)
    mysql_db.insert_data(TABLE_NAME, **test_data)
    mysql_db.select_count(cols=['*'], table=TABLE_NAME, where=where)
    print "-------------------"
    mysql_db.select_normal(cols=['*'], table=TABLE_NAME, where=where)
