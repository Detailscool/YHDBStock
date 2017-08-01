# -*- coding: utf-8 -*-

import sys

from DataManager import DataManager
from MysqlDBManager import MysqlDBManager
from MongoDBManager import MongoDBManager

if __name__ == '__main__':

    # database_manager = MysqlDBManager()
    database_manager = MongoDBManager()

    if len(sys.argv[1:]) > 1:
        print '参数过多'
    else:
        work_name = sys.argv[1:][0] if len(sys.argv[1:]) == 1 else None
        if not work_name:
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
