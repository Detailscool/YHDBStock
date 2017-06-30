from pymongo import MongoClient
import datetime

class MongoDBManager:

    SERVER_IP = 'localhost'

    def __init__(self, client=None, expires=datetime.timedelta(days=30)):
        self.client = MongoClient()
        self.db = self.client.spider

    def dequeue_stock(self, stock_code):
        record = self.db.stock.find_one(filter={'t_'+stock_code: stock_code})
        if record:
            return record
        else:
            return None

    def enqueue_stock(self, stock_code, date, stock_data):
        stock = {'code': stock_code, 'date': date, 'open': stock_data.open, 'high': stock_data.high, 'close': stock_data.close, 'low': stock_data.low, 'volume': stock_data.volume, 'price_change': stock_data.price_change, 'p_change': stock_data.p_change, 'ma5': stock_data.ma5, 'ma10': stock_data.ma10, 'ma20': stock_data.ma20, 'v_ma5': stock_data.v_ma5, 'v_ma10': stock_data.v_ma10, 'v_ma20': stock_data.v_ma20}
        self.db.stock.insert({'t_'+stock_code: stock_code}, {'stock': stock})