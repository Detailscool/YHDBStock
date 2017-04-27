# -*- coding: utf-8 -*-

import tushare as ts
from time import time
from mysqlmanager import StockDatabaseManager
from bs4 import BeautifulSoup
import requests
import re
import sys
import threading
import Queue

def getHTMLText(url, code="utf-8"):
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
def getStockList(stock_list_url):
    stock_list = []
    html = getHTMLText(stock_list_url, "GB2312")
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find_all("a")
    for i in a:
        try:
            href = i.attrs["href"]  # 相当于i.get("href")
            stock_list.append(re.findall(r's[zh]\d{6}', href)[0].encode('utf-8'))
        except:
            continue
    return stock_list

def download_stock_data(stock_list, queue):
    stock_datas = []
    for index, stock_code in enumerate(stock_list):
        data_frame = ts.get_hist_data(stock_code, start='2017-01-01', end='2017-04-22')
        if data_frame is not None:
            for date, stock_data in data_frame.iterrows():
                # print 'index :\n', type(date), '\n', date
                # print 'row : \n', type(stock_data), '\n', stock_data
                # database.enqueue_stock(stock_code=stock_code, date=date.encode('utf8'), stock_data=stock_data)
                stock_datas.append((stock_code, date, stock_data))
        else:
            print stock_code + '无效数据 --- ', threading.currentThread().name
    queue.put(stock_datas)

def enqueue_stock_data():
    start = time()

    stock_list_url = "http://quote.eastmoney.com/stocklist.html"
    stock_list = getStockList(stock_list_url)

    sz_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sz']
    sh_stock_list = [a[2:].strip() for a in stock_list if a[:2] == 'sh']

    stock_list = sz_stock_list + sh_stock_list

    print 'stock_list leng:', len(stock_list)

    thread_count = 6
    per_count = len(stock_list)//thread_count
    print 'per_count :', per_count

    queue = Queue.Queue()

    pool = []
    for i in range(0, thread_count):
        t = threading.Thread(target=download_stock_data, name='GetStockThread %s ' % (i), args=[stock_list[(per_count * i): (per_count * (i+1))], queue])
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

    # database = StockDatabaseManager(5, stocks=stock_list)
    # for stock_code, date, stock_data in result:
    #     database.enqueue_stock(stock_code=stock_code, date=date.encode('utf8'), stock_data=stock_data)
    # print '插入数据库耗时：', time() - start

def dequeue_stock(stock_code):
    # start = time()
    # data = database.dequeue_stock(stock_code=stock_code)
    # print '耗时：', time() - start
    # print data, '\n', len(data), '条数据'
    pass

if __name__ == '__main__':
    if len(sys.argv[1:]) > 1:
        print '参数过多'
    else:
        work_name = sys.argv[1:][0] if len(sys.argv[1:]) == 1 else None
        if not work_name:
            while 1:
                work_name = raw_input('你要搞乜:' + '\n1.今年数据' + '\n2.今天数据' + '\n3.查找数据' + '\n').strip()
                if int(work_name) == 1:
                    enqueue_stock_data()
                    print 1
                elif int(work_name) == 2:
                    print 2
                elif int(work_name) == 3:
                    print 3
                    stock_code = raw_input('输入股票代码:\n').strip()
                    if stock_code:
                        dequeue_stock(stock_code)
                    # print '耗时：', time() - start
                else:
                    pass
