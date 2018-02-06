#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import MySQLdb
import traceback
import time
from random import randint
from common_base_db import CommonBaseDB as BaseDB


class CommonMysqlDB(BaseDB):

    def __init__(self, conf_file):
        '''
        :param conf_file: 数据库的配置文件
        '''
        self.connection = None
        self.cursor = None
        super(CommonMysqlDB, self).__init__(conf_file)
        try:
            self.connection = self.get_connection()
            print 'Init Mysql DB finished!'
        except:
            print traceback.format_exc()


    def __del__(self):
        # 析构，对象没被调用，关闭连接
        if self.cursor:
            self.cursor.close()
            print "---Close cursor---"

        if self.connection:
            self.connection.close()
            print "---Close connection---"


    def count_time(func):
        '''计算某个方法执行的时间
        '''
        def inner(*args, **kwargs):
            time_start = time.time()
            function = func(*args, **kwargs)
            time_end = time.time() - time_start
            print "Excuting [%s] spent %0.5f seconds." % (str(func.__name__), time_end)
            return function
        return inner


    @count_time
    def get_connection(self):
        # 重写mysql connection获取方法
        if self.connection:
            return self.connection
        else:
            if self.db_info:
                mysql_conn = MySQLdb.connect(host=self.db_info['host'], port=int(self.db_info['port']),user=self.db_info['user'],
                                            passwd=self.db_info['passwd'], db=self.db_info['db_name'], charset='utf8')
                return  mysql_conn
            return None




    def create_table(self, table_name):
        '''重写父类建表方法
        :param table_name: 表名
        ENGINE=InnoDB DEFAULT CHARSET=utf8 不带这条无法插入中文
        '''
        create_sql = """CREATE TABLE IF NOT EXISTS %s (id INTEGER(20) PRIMARY KEY NOT NULL,
                                                             name VARCHAR(32) NOT NULL ,
                                                             age  INTEGER(5) ,
                                                             level INTEGER(10),
                                                             description VARCHAR(100)
                                                             )ENGINE=InnoDB DEFAULT CHARSET=utf8""" % table_name

        try:
            conn = self.connection
            if conn:
                self.cursor = conn.cursor()
                self.cursor.execute(create_sql)
                conn.commit()
                print ">>>Created table %s successfuly!" % table_name
            else:
                print "Mysql connection hasn\'t been established!"
        except:
            if self.cursor:
                self.cursor.close()
            print "***Creating table %s failed!" % table_name
            print traceback.format_exc()



    def insert_data(self, table, **kwargs):
        '''重写插入数据方法
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''
        try:
            keys = []
            vals = []
            for key, value in kwargs.items():
                keys.append(key)
                vals.append(('"%s"' % value) if isinstance(key, str) else value)  # 保留字符串或者整型
            keys_str = ','.join(keys)
            vals_str = ','.join(vals)
            insert_sql = "INSERT INTO %s (%s) VALUES (%s) ;" % (table, keys_str, vals_str)  # 拼接插入语句
            print "insert_sql:", insert_sql

            conn = self.connection
            if conn:
                cursor = self.cursor if self.cursor else conn.cursor() # 游标存在就用之前的
                cursor.execute(insert_sql)
                conn.commit()
                print "Inserted data successfully!"
            else:
                print "Mysql connection hasn\'t been established!"
        except :
            print traceback.print_exc()



    def select_count(self, cols=[], table='', where=''):
        '''重写查询满足条件的数据数目方法
        '''
        count = 0
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
            else:
                print "Mysql connection hasn\'t been established!"
        except:
            print "***Selecting data failed!"
            print traceback.print_exc()

        finally:
            return count



    @count_time
    def select_normal(self, cols=[], table='', where=''):
        '''重写查询出所有满足条件的数据方法
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

    DB_CONF = 'common_db.conf'
    TABLE_NAME = 'mysql_test'
    test_data = {"id": randint(50,150),
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









