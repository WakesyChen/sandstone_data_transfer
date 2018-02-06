#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import MySQLdb
import configobj
import traceback
from random import randint
import time
class CommonDB(object):

    DB_TYPE = None  # 数据库类型
    def __init__(self, conf_file):
        '''
        :param conf_file: 数据库的配置文件
        '''
        try:
            global DB_TYPE
            self.db_conf = configobj.ConfigObj(conf_file)
            DB_TYPE = self.db_conf['common']['DB_TYPE']

            if DB_TYPE == 'oracle':
                self.db_info = self.__get_db_info()
                self.connection = self.__get_oracle_connection()
            elif DB_TYPE == 'mysql':
                self.db_info = self.__get_db_info()
                self.connection = self.__get_mysql_connection(self.db_info)
            else:
                raise Exception, "Init database failed! db_type is not valid."

        except:
            self.connection = None
            print traceback.format_exc()


    def __del__(self):
        # 析构，对象没被调用，关闭连接
        if self.connection:
            self.connection.close()
            print "Close connection."




#---------------------------common相关方法--------------------------------

    def __get_db_info(self):

        db_info = {}
        db_info['host'] = self.db_conf[DB_TYPE]['HOST']
        db_info['port'] = self.db_conf[DB_TYPE]['PORT']
        db_info['user'] = self.db_conf[DB_TYPE]['USER']
        db_info['passwd'] = self.db_conf[DB_TYPE]['PASSWORD']
        db_info['db_name'] = self.db_conf[DB_TYPE]['DB_NAME']
        print "db_info:%s" % db_info
        return db_info


    def get_connection(self):
        # 外部模块获取对应的数据库连接
        return self.connection

    def create_table(self, table_name):
        # 对外提供的创建表方法
        if DB_TYPE == 'mysql':
            self.create_table_mysql(table_name)
        elif DB_TYPE == 'oracle':
            self.create_table_oracle(table_name)

    def insert_data(self, table, **kwargs):
        if DB_TYPE == 'mysql':
            self.insert_data_mysql(table, **kwargs)
        elif DB_TYPE == 'oracle':
            self.insert_data_oracle(table, **kwargs)


    def select_normal(self, cols=[], table='', where=''):
        if DB_TYPE == 'mysql':
            self.select_normal_mysql(cols=[], table='', where='')
        elif DB_TYPE == 'oracle':
            self.select_normal_oracle(cols=[], table='', where='')


    def count_time(func):
        '''计算某个方法执行的时间
        :param func: 方法名
        '''
        def inner(*args, **kwargs):
            time_start = time.time()
            function = func(*args, **kwargs)
            time_end = time.time() - time_start
            print "Excuting [%s] spent %0.5f seconds." % (str(func.__name__), time_end)
            return function
        return inner





#----------------------------mysql相关方法--------------------------------

    @count_time
    def __get_mysql_connection(self, db_info):
            db_info = self.db_info
            mysql_connection = MySQLdb.connect(host=db_info['host'], port=int(db_info['port']), user=db_info['user'],
                                               passwd=db_info['passwd'], db=db_info['db_name'], charset='utf8')
            return mysql_connection


    def create_table_mysql(self, table_name):
        '''建表语句不方便统一，各自独立拼sql
        :param table_name: 表名
        ENGINE=InnoDB DEFAULT CHARSET=utf8 这句不带的话无法插入中文
        '''
        create_sql = """CREATE TABLE IF NOT EXISTS %s (id INTEGER(20) PRIMARY KEY NOT NULL,
                                                             name VARCHAR(32) NOT NULL ,
                                                             age  INTEGER(5) ,
                                                             level INTEGER(10),
                                                             description VARCHAR(100)
                                                             )ENGINE=InnoDB DEFAULT CHARSET=utf8""" % table_name
        cursor = None
        try:
            conn = self.connection
            if conn:
                cursor = conn.cursor()
                cursor.execute(create_sql)
                conn.commit()
                print ">>>Created table %s successfuly!" % table_name
            else:
                print "Mysql connection hasn\'t been established!"
        except:
            print "***Creating table %s failed!" % table_name
            print traceback.format_exc()

        finally:
            if cursor:
                cursor.close()

    def insert_data_mysql(self, table, **kwargs):
        '''通用插入语句
        :param table: 待插入的表名
        :param kwargs: 要插入的数据字典，对应字段名和值
        '''
        cursor = None
        conn = None
        try:
            keys = []
            vals = []
            for key, value in kwargs.items():
                keys.append(key)
                # 保留字符串或者整型
                vals.append(('"%s"' % str(value)) if isinstance(key, str) else value)
            keys_str = ','.join(keys)
            vals_str = ','.join(vals)
            # 拼接插入语句
            insert_sql = "INSERT INTO %s (%s) VALUES (%s) ;" % (table, keys_str, vals_str)
            print "insert_sql:", insert_sql
            conn = self.connection
            if conn:
                cursor = conn.cursor()
                cursor.execute(insert_sql)
                conn.commit()
                print ">>>>Inserted data successfully!"
            else:
                print "Mysql connection hasn\'t been established!"
        except :
            if conn:
                conn.rollback()
            print "***Inserting data Failed! Rollback"
            print traceback.print_exc()
        finally:
            # 结束关闭游标
            if cursor:
                cursor.close()


    def select_count_mysql(self, cols=[], table='', where=''):
        '''查询满足条件的数据数目
        '''
        cursor = None
        count = 0
        try:
            cols_str = ','.join(cols)
            select_sql = "SELECT %s FROM %s ;" % (cols_str, table)
            if where:
                select_sql = select_sql.replace(';', 'WHERE ') + where + ";"
            print select_sql
            conn = self.connection
            if conn:
                cursor = conn.cursor()
                count = cursor.execute(select_sql)
                print "select count is %s" % count
            else:
                print "Mysql connection hasn\'t been established!"
        except:
            print "***Selecting data failed!"
            print traceback.print_exc()
        finally:
            if cursor:
                cursor.close()
            return count


    @count_time
    def select_normal_mysql(self, cols=[], table='', where=''):
        '''查询出所有满足条件的数据
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
                cursor = conn.cursor()
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
            if cursor:
                cursor.close()
            return results




#---------------------------oracle相关方法----------------------------------

    def __get_oracle_connection(self, db_info):
        pass

    def create_table_oracle(self, table_name):
        pass

    def insert_data_oracle(table, **kwargs):
        pass


if __name__ == '__main__':

    DB_CONF = 'common_db.conf'
    TABLE_NAME = 'common_test'
    # conn = mysql_db.get_connection()
    test_data = {"id": randint(0,100),
                 "name": "Coco",
                 "age": 23,
                 "level": 17,
                 "description": u"在山的那边海的那边，有一群蓝精灵，他们活泼又聪明！".encode('utf8')
                }
    colomns = ['*']
    where = "name in ('wakesy', 'Coco')"
    mysql_db = BaseDB(DB_CONF)

    # mysql_db.create_table(TABLE_NAME)
    # mysql_db.insert_data(TABLE_NAME, **test_data)
    mysql_db.select_normal(cols=['*'], table=TABLE_NAME, where=where)
    print "-------------------"
    mysql_db.select_count(cols=['*'], table=TABLE_NAME, where=where)









