# -*- coding: utf-8 -*-

import tushare as ts
from bs4 import BeautifulSoup
import requests
import re
import threading
import Queue
from time import time
import pandas as pd

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
    def __getStockList(cls, stock_list_url):
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
    def __download_stock_data(cls, stock_list, queue):
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
    def enqueue_stock_data(cls, database_manager):
        start = time()

        stock_list_url = "http://quote.eastmoney.com/stocklist.html"
        stock_list = cls.__getStockList(stock_list_url)

        sz_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sz']
        sh_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sh']

        stock_list = sz_stock_list + sh_stock_list

        print 'stock_list leng:', len(stock_list)

        # Test
        stock_list = stock_list[:6]

        thread_count = 6
        per_count = len(stock_list) // thread_count
        print 'per_count :', per_count

        queue = Queue.Queue()

        pool = []
        for i in range(0, thread_count):
            if i != thread_count - 1:
                t = threading.Thread(target=cls.__download_stock_data, name='GetStockThread %s ' % i,
                                     args=[stock_list[(per_count * i): (per_count * (i + 1))], queue])
            else:
                t = threading.Thread(target=cls.__download_stock_data, name='GetStockThread %s ' % i,
                                     args=[stock_list[(per_count * i): -1], queue])
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
        start = time()

        print '打开数据库耗时：', time() - start
        start = time()

        if hasattr(database_manager, 'stocks'):
            database_manager.stocks = stock_list

        print 'result type :', type(result[0])#, '\n', result
        # pd.DataFrame(result).to_csv('result.csv', header=None)

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