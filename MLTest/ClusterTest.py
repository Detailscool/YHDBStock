#!/usr/bin/python
# -*- coding:utf-8 -*-
#  ClusterTest.py
#  Created by HenryLee on 2017/9/10.
#  Copyright © 2017年. All rights reserved.
#  Description :

from datetime import datetime
import numpy as np
from sklearn.cluster import AffinityPropagation
from matplotlib import finance
from TushareStock.mysql_manager import mysqlmanager
from TushareStock import DataManager2
import pandas as pd
from pprint import pprint
from scipy.stats import mode
from matplotlib import pyplot as plt


pd.set_option('display.width', 500)

stock_list = DataManager2.getStockList()[-200:]
sqls = ["SELECT code, close, date From t_" + stock[-6:] + " ORDER BY `date` ASC" for stock in stock_list if len(stock)>6]
a_results = [mysqlmanager().mysql_com(sql) for sql in sqls]
results = []
pop_list = []
for i, result in enumerate(a_results):
    if result:
        results.append(result)
    else:
        pop_list.append(i)

a = list(set(range(len(stock_list))).difference(set(pop_list)))
stock_list = np.array(stock_list)[a]

close_list = []
pop_list = []
current_dates = None
days = 100  # 最近交易
for index, result in enumerate(results):
    ll = list(result)
    data = pd.DataFrame(ll)
    data.set_index(np.array(data.date), inplace=True)
    data.drop(['date', 'code'], axis=1, inplace=True)
    # print type(data.close)
    # print dir(data.close)
    # dates = data.date.get_values().tolist()
    # dict[int(ll[0]['code'].encode('utf-8'))] = data[-20:]
    close = data.close[-days:].get_values().tolist()
    if len(close) < days:
        # stock_list.pop(index)
        pop_list.append(index)
        # print ll[0]['code'].encode('utf-8'),' - ', index, ' - ', len(close)
    else:
        close_list.append(close)

a = list(set(range(len(stock_list))).difference(set(pop_list)))
stock_list = stock_list[a]

# pprint(dict)
# print dict
closes = np.array(close_list, dtype=np.float)
log_returns = np.diff(np.log(closes))  # 对数收益率
print log_returns.shape

log_returns_norms = np.sum(log_returns**2, axis=1)
a = log_returns_norms[:, np.newaxis]
b = log_returns_norms[np.newaxis, :]
S = -a - b + 2*np.dot(log_returns, log_returns.T)
aff_pro = AffinityPropagation().fit(S)
labels = aff_pro.labels_


mode = mode(labels)
print '类型数共 ：',len(set(labels)), '  最多：', mode[0] ,' 有 %s 个' % mode[1]

for i, label in enumerate(labels):
    print '%s -> Cluster %s' % (stock_list[i], label)

print stock_list[labels==mode[0]]

plt.hist(labels, range(len(set(labels))))
plt.show()