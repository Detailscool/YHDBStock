# -*- coding: utf-8 -*-

import tushare as ts
from time import time
from mysqlmanager import StockDatabaseManager
from bs4 import BeautifulSoup
import requests
import re

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


if __name__ == '__main__':
    start = time()

    database = StockDatabaseManager(5)

    stock_list_url = "http://quote.eastmoney.com/stocklist.html"
    stock_list = getStockList(stock_list_url)

    sz_stock_list = [a[2:] for a in stock_list if a[:2] == 'sz']
    sh_stock_list = [a[2:] for a in stock_list if a[:2] == 'sh']

    stock_list = sz_stock_list + sh_stock_list

    for index, stock_code in enumerate(stock_list):
        data_frame = ts.get_hist_data(stock_code, start='2017-01-01', end='2017-04-22')
        if data_frame is not None:
            for date, stock_data in data_frame.iterrows():
                # print 'index :\n', type(date), '\n', date
                # print 'row : \n', type(stock_data), '\n', stock_data
                database.enqueueUrl(stock_code=stock_code, date=date.encode('utf8'), stock_data=stock_data)

    print '耗时：', time() - start