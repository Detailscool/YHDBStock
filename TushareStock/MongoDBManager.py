from pymongo import MongoClient
import datetime

class MongoDBManager:

    SERVER_IP = 'localhost'

    def __init__(self, client=None, expires=datetime.timedelta(days=30)):
        self.client = MongoClient()
        self.db = self.client.Stock

    def dequeue_stock(self, stock_code):
        record = self.db.stock.find_one(filter={'t_'+stock_code: stock_code})
        if record:
            return record
        else:
            return None

    def enqueue_stock(self, stock_code, date, stock_data):
        # print 'stock_code :', stock_code, ' date :', date
        stock = dict(stock_data)
        stock['date'] = date
        db = self.db['t_'+stock_code]
        db.insert_one(stock)