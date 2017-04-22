import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling
import httplib
import hashlib
import time 

class StockDatabaseManager:

    DB_NAME = 'stock_database'

    SERVER_IP = '127.0.0.1'

    TABLES = {}
    TABLES['stocks'] = (
        "CREATE TABLE IF NOT EXISTS `stocks` ("
        "  `index` int(11) NOT NULL AUTO_INCREMENT,"
        "  `code` varchar(512) NOT NULL,"
        "  `date` varchar(32) NOT NULL,"
        "  `open` FLOAT (32) NOT NULL,"
        "  `high` FLOAT(32) NOT NULL,"
        "  `close` FLOAT(32) NOT NULL,"
        "  `low` FLOAT(32) NOT NULL,"
        "  `volume` FLOAT(32) NOT NULL,"
        "  `price_change` FLOAT(32) NOT NULL,"
        "  `p_change` FLOAT(32) NOT NULL,"
        "  `ma5` FLOAT(32) NOT NULL,"
        "  `ma10` FLOAT (32) NOT NULL,"
        "  `ma20` FLOAT(32) NOT NULL,"
        "  `v_ma5` FLOAT(32) NOT NULL,"
        "  `v_ma10` FLOAT(32) NOT NULL,"
        "  `v_ma20` FLOAT(32) NOT NULL,"
        "  PRIMARY KEY (`index`)"
        ") ENGINE=InnoDB")


    def __init__(self, max_num_thread):
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user='root')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print 'Create Error ' + err.msg
            exit(1)

        cursor = cnx.cursor()

        try:
            cnx.database = self.DB_NAME
            self.create_tables(cursor)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.create_database(cursor)
                cnx.database = self.DB_NAME
                self.create_tables(cursor)
            else:
                print(err)
                exit(1)
        finally:
            cursor.close()
            cnx.close()

        dbconfig = {
            "database": self.DB_NAME,
            "user":     "root",
            "host":     self.SERVER_IP,
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name = "mypool",
                                                          pool_size = max_num_thread,
                                                          **dbconfig)


    def create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, cursor):
        for name, ddl in self.TABLES.iteritems():
            try:
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print 'create tables error ALREADY EXISTS'
                else:
                    print 'create tables error ' + err.msg
            else:
                print 'Tables created'


    def enqueueUrl(self, stock_code, date, stock_data):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            add_strock = ("INSERT INTO stocks (code, date, open, high, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_strock = (stock_code, date, float(stock_data.open), float(stock_data.high), float(stock_data.close), float(stock_data.low), float(stock_data.volume), float(stock_data.price_change), float(stock_data.p_change), float(stock_data.ma5), float(stock_data.ma10), float(stock_data.ma20), float(stock_data.v_ma5), float(stock_data.v_ma10), float(stock_data.v_ma20))
            cursor.execute(add_strock, data_strock)
            con.commit()
            # print stock_code + 'insert success'
        except mysql.connector.Error as err:
            print 'enqueueUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()


    # def dequeueUrl(self):
    #     con = self.cnxpool.get_connection()
    #     cursor = con.cursor(dictionary=True)
    #     try:
    #         query = ("SELECT `index`, `code` FROM stocks WHERE status='new' ORDER BY `index` ASC LIMIT 1 FOR UPDATE")
    #         cursor.execute(query)
    #         if cursor.rowcount is 0:
    #             return None
    #         row = cursor.fetchone()
    #         update_query = ("UPDATE stocks SET `status`='downloading' WHERE `index`=%d") % (row['index'])
    #         cursor.execute(update_query)
    #         con.commit()
    #         return row
    #     except mysql.connector.Error as err:
    #         # print 'dequeueUrl() ' + err.msg
    #         return None
    #     finally:
    #         cursor.close()
    #         con.close()
    #
    # def finishUrl(self, index):
    #     con = self.cnxpool.get_connection()
    #     cursor = con.cursor()
    #     try:
    #         # we don't need to update done_time using time.strftime('%Y-%m-%d %H:%M:%S') as it's auto updated
    #         update_query = ("UPDATE stocks SET `status`='done', `done_time`=%s WHERE `index`=%d") % (time.strftime('%Y-%m-%d %H:%M:%S'), index)
    #         cursor.execute(update_query)
    #         con.commit()
    #     except mysql.connector.Error as err:
    #         # print 'finishUrl() ' + err.msg
    #         return
    #     finally:
    #         cursor.close()
    #         con.close()
