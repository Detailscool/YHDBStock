# -*- coding: utf-8 -*-

import tushare as ts
from bs4 import BeautifulSoup
import requests
import re
import threading
import Queue
from time import time
from multiprocessing.dummy import Pool as ThreadPool
import os
from StockPylot import StockPylot


class DataManager:

    @classmethod
    def __getHTMLText(cls, url, code="utf-8"):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status
            # r.encoding = r.apparent_encoding
            r.encoding = code  # 编码识别优化
            return r.text
        except:
            return ""

    '''''
    从东方财富网获取股票代码列表
    '''

    @classmethod
    def getStockList(cls, stock_list_url="http://quote.eastmoney.com/stocklist.html"):
        stock_list = []
        html = cls.__getHTMLText(stock_list_url, "GB2312")
        soup = BeautifulSoup(html, "html.parser")
        a = soup.find_all("a")
        for i in a:
            try:
                href = i.attrs["href"]  # 相当于i.get("href")
                stock_list.append(re.findall(r's[zh]\d{6}', href)[0].encode('utf-8'))
            except:
                continue
        return stock_list

    @classmethod
    def __download_stock_data1(cls, stock_list, queue):
        stock_datas = []
        for index, stock_code in enumerate(stock_list):
            data_frame = ts.get_hist_data(stock_code, start='2017-01-01', end='2017-08-01')
            if data_frame is not None:
                for date, stock_data in data_frame.iterrows():
                    # print 'index :\n', type(date), '\n', date
                    # print 'row : \n', type(stock_data), '\n', stock_data
                    # database.enqueue_stock(stock_code=stock_code, date=date.encode('utf8'), stock_data=stock_data)
                    stock_datas.append((stock_code, date, stock_data))
            else:
                print stock_code + '无效数据 --- ', threading.currentThread().name
        queue.put(stock_datas)

    @classmethod
    def __download_stock1(cls):
        start = time()

        stock_list = cls.getStockList()

        sz_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sz']
        sh_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sh']

        stock_list = sz_stock_list + sh_stock_list + ['sh', 'sz', 'hs300', 'sz50', 'zxb', 'cyb']

        print 'stock_list leng:', len(stock_list)

        # Test
        stock_list = stock_list[:6]

        queue = Queue.Queue()

        thread_count = 6
        per_count = len(stock_list) // thread_count
        print 'per_count :', per_count

        pool = []
        for i in range(0, thread_count):
            if i != thread_count - 1:
                t = threading.Thread(target=cls.__download_stock_data1, name='GetStockThread %s ' % i,
                                     args=(stock_list[(per_count * i): (per_count * (i + 1))], queue))
            else:
                t = threading.Thread(target=cls.__download_stock_data1, name='GetStockThread %s ' % i,
                                     args=(stock_list[(per_count * i):], queue))
            pool.append(t)

        for t in pool:
            t.start()

        for t in pool:
            t.join()

        result = []
        for _ in pool:
            result.extend(queue.get())

        print 'done --- ', len(result)
        print '下载耗时：', time() - start

        return result

    @classmethod
    def enqueue_stock_data(cls, database_manager):
        start = time()
        # if hasattr(database_manager, 'stocks'):
        #     database_manager.stocks = stock_list
        result = cls.__download_stock1()
        for stock_code, date, stock_data in result:
            database_manager.enqueue_stock(stock_code=stock_code, date=date.encode('utf8'), stock_data=stock_data)
        print '插入数据库耗时：', time() - start

    @classmethod
    def dequeue_stock(cls, database_manager, stock_code):
        start = time()
        data = database_manager.dequeue_stock(stock_code=stock_code)
        if data:
            print '耗时：', time() - start
            # StockPylot.line_plot(data)
            StockPylot.bar_plot(data)
        else:
            print '输入有误'

    data_path = './Stocks/'

    def download_stock_data2(self, stock_code):
        try:
            data_frame = ts.get_hist_data(stock_code, start='2017-01-01', end='2018-11-05')
            if data_frame is not None:
                # print dir(data_frame)
                if not os.path.exists(self.data_path):
                    os.mkdir(self.data_path)
                # 1
                # data_frame.to_csv(os.path.join(self.data_path, 't_%s.csv' % stock_code), mode='wb', encoding='utf-8')
                data_frame.to_csv(os.path.join(self.data_path, 't_%s' % stock_code), mode='wb', sep='\t', header=False,
                                  index_label=False, encoding='utf-8')

                # 2
                # data_str = data_frame.to_string(header=False, index=False)
                # f = open(os.path.join(cls.data_path, 't_%s' % stock_code), 'wb')
                # f.write(data_str)
                # f.close()
                pass
            else:
                print stock_code + ' undone --- ', threading.currentThread().name
        except Exception as e:
            print e

    def download_stock_2(self):
        stock_list = self.__class__.getStockList()
        stock_list = [a[-6:] for a in stock_list if len(a) >= 6]
        stock_list += ['sh', 'sz', 'hs300', 'sz50', 'zxb', 'cyb']

        pool = ThreadPool(6)
        pool.map(self.download_stock_data2, stock_list)
        pool.close()
        pool.join()


if __name__ == '__main__':
    DataManager().download_stock_2()
