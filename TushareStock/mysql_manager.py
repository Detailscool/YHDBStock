#!/usr/bin/python
# -*- coding:utf-8 -*-
#  mysql_manager.py
#  Created by HenryLee on 2017/9/10.
#  Copyright © 2017年. All rights reserved.
#  Description :

import MySQLdb
import sys

class mysqlmanager(object):
    _instance = None
    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(mysqlmanager, cls).__new__(cls, *args, **kw)
        return cls._instance

    __host = '127.0.0.1'
    __user = 'root'
    __passwd = 'root'
    __dbname = 'stocks'
    """
    功能：连接数据
    """
    def __init__(self):
        self.db = None
        try:
            self.db = MySQLdb.connect(host=self.__host, user=self.__user, passwd=self.__passwd, db=self.__dbname, charset='utf8', connect_timeout=10)
        except Exception as e:
            print e

    __ddl_str = "CREATE TABLE IF NOT EXISTS `%s` (" \
                "  `index` int(11) NOT NULL AUTO_INCREMENT," \
                "  `date` varchar(32) NOT NULL," \
                "  `open` FLOAT (32) NOT NULL," \
                "  `high` FLOAT(32) NOT NULL," \
                "  `close` FLOAT(32) NOT NULL," \
                "  `low` FLOAT(32) NOT NULL," \
                "  `volume` FLOAT(32) NOT NULL," \
                "  `price_change` FLOAT(32) NOT NULL," \
                "  `p_change` FLOAT(32) NOT NULL," \
                "  `ma5` FLOAT(32) NOT NULL," \
                "  `ma10` FLOAT (32) NOT NULL," \
                "  `ma20` FLOAT(32) NOT NULL," \
                "  `v_ma5` FLOAT(32) NOT NULL," \
                "  `v_ma10` FLOAT(32) NOT NULL," \
                "  `v_ma20` FLOAT(32) NOT NULL," \
                "  `turnover` FLOAT(32) NOT NULL," \
                "  PRIMARY KEY (`index`)" \
                ") ENGINE=InnoDB"

    def create_table(self, stock):
        if stock:
            ddl = self.__ddl_str % stock
            self.mysql_com(ddl)

    """
    功能：执行mysql命令，返回结果
    输入：sql_com, sql命令
    返回：mysql查询结果数组
    """
    def mysql_com(self, sql_com, params=None):
        # 连接数据库
        # if not self.db:
        #     for i in range(3):
        #         islinked = self.__connect_db()
        #         if islinked:
        #             break

        result = []
        if self.db:
            # 执行mysql命令
            # cursor = db.cursor()
            cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
            try:
                if isinstance(params, list):
                    cursor.executemany(sql_com, params)
                elif not params:
                    cursor.execute(sql_com)
                result = cursor.fetchallDict()
                self.db.commit()
                # self.db.close()
            except Exception as e:
                print 'SQL:', sql_com
                print 'Exception:', e
        return result

    def __del__(self):
        self.db.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "please input filename"
        sys.exit()
    stock_table = sys.argv[1]
    mysqlmanager().create_table(stock_table)