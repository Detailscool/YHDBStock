# -*- coding: utf-8 -*-

import tushare as ts
import threading
from multiprocessing.dummy import Pool as ThreadPool
import os


class DataManager:

    data_path = './Stocks/'

    def download_stock_data2(self, stock_code):
        if len(stock_code) == 6:
            data_frame = ts.get_hist_data(stock_code, start='2018-01-01', end='2020-10-30')
            if data_frame is not None:
                # print dir(data_frame)
                if not os.path.exists(self.data_path):
                    os.mkdir(self.data_path)
                # 1
                # data_frame.to_csv(os.path.join(self.data_path, 't_%s.csv' % stock_code), mode='wb', encoding='utf-8')
                data_frame.to_csv(os.path.join(self.data_path, 't_%s' % stock_code), sep='\t', header=False, index_label=False)
                print(stock_code+'  done')

                # 2
                # data_str = data_frame.to_string(header=False, index=False)
                # f = open(os.path.join(cls.data_path, 't_%s' % stock_code), 'wb')
                # f.write(data_str)
                # f.close()
                # pass
            else:
                print(stock_code + '无效数据 --- ', threading.currentThread().name)

    def download_stock_2(self):
        import QUANTAXIS as QA
        data = QA.QAFetch.QATdx.QA_fetch_get_stock_list ()
        stock_list = data.code.tolist()

        pool = ThreadPool(6)
        pool.map(self.download_stock_data2, stock_list)
        pool.close()
        pool.join()


if __name__ == '__main__':
    DataManager().download_stock_2()
    pass