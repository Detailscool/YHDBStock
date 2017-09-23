# -*- coding: utf-8 -*-

from MysqlDBManager import MysqlDBManager
from MongoDBManager import MongoDBManager
from DataManager import DataManager

import Test9

from StockPylot import StockPylot

if __name__ == '__main__':
    database_manager = MysqlDBManager()

    while 1:
        work_name = raw_input('你要搞乜:' + '\n1.今年数据' + '\n2.今天数据' + '\n3.查找数据' + '\n').strip()
        if int(work_name) == 1:
            DataManager.enqueue_stock_data(database_manager)
            print 1
        elif int(work_name) == 2:
            print 2
        elif int(work_name) == 3:
            print 3
            stock_code = raw_input('输入股票代码:\n').strip()
            if stock_code:
                DataManager.dequeue_stock(database_manager, stock_code)
        else:
            pass
