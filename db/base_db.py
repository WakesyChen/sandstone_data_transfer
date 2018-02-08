#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:Wakesy

import configobj
import traceback
from util import logger
class CommonBaseDB(object):

    def __init__(self, conf_file):
        '''
        :param conf_file: 数据库的配置文件
        '''
        try:
            self.db_conf = configobj.ConfigObj(conf_file)
            self.db_type = self.db_conf['common']['DB_TYPE']
            self.db_info = self.get_db_info()
        except:
            logger.critical(traceback.format_exc())

    def get_db_info(self):
        db_info = {}
        db_info['host'] = self.db_conf[self.db_type]['HOST']
        db_info['port'] = self.db_conf[self.db_type]['PORT']
        db_info['user'] = self.db_conf[self.db_type]['USER']
        db_info['passwd'] = self.db_conf[self.db_type]['PASSWORD']
        db_info['db_name'] = self.db_conf[self.db_type]['DB_NAME']
        logger.info("db_info:%s" % db_info)
        return db_info

# ---------------------------对外提供方法--------------------------------
    def get_connection(self):
        # 获取对应的数据库连接
        pass

    def ensure_table_created(self, table):
        # 创建表方法
        pass

    def insert_data(self, table, datainfo):
        # 插入数据
        pass

    def select_count(self, cols=[], table='', where=''):
        # 查询满足条件的数目
        pass


    def select_normal(self, cols=[], table='', where=''):
        # 查询满足条件的项
        pass








