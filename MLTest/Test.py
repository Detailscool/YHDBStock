#!/usr/bin/python
# -*- coding:utf-8 -*-
#  Test.py
#  Created by HenryLee on 2017/9/10.
#  Copyright © 2017年. All rights reserved.
#  Description :

import numpy as np

close_list = [[1, 2, 2], [2, 1, 1], [1, 1, 1]]

jsfl = np.sum(close_list, axis=0)
fa = np.sum(close_list, axis=1)

closes = np.array(close_list, dtype=np.float)
closes = np.log(closes)
log_returns = np.diff(closes)
print log_returns.shape

log_returns_norms = np.sum(log_returns**2, axis=1)
a = log_returns_norms[:, np.newaxis]
b = log_returns_norms[np.newaxis, :]
dot = 2*np.dot(log_returns, log_returns.T)
c = dot - a
S = c - b

pass