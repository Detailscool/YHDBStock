import mysql.connector
from mysql.connector import errorcode
from mysql.connector import pooling

class MysqlDBManager:

    _instance = None
    def __new__(cls, *args, **kw):
        if not cls._instance:
            cls._instance = super(MysqlDBManager, cls).__new__(cls, *args, **kw)
        return cls._instance

    __DB_NAME = 'stock_database'

    __SERVER_IP = '127.0.0.1'

    __TABLES = {}
    __TABLES['stocks'] = (
        "CREATE TABLE IF NOT EXISTS `t_%s` ("
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

    def __init__(self, max_num_thread=5, stocks=None):
        try:
            cnx = mysql.connector.connect(host=self.__SERVER_IP, user='root', passwd='root')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print 'Create Error ' + err.msg
            exit(1)

        self.__cursor = cnx.cursor()

        try:
            cnx.database = self.__DB_NAME
            if max_num_thread:
                self.__create_connectionPool(max_num_thread)
            if stocks:
                self.stocks = stocks
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                self.__create_database(self.__cursor)
                cnx.database = self.__DB_NAME
                self.__create_connectionPool(max_num_thread)
                self.stocks = stocks
            else:
                print(err)
                exit(1)
        finally:
            self.__cursor.close()
            cnx.close()

    def __setattr__(self, key, value):
        if key == 'stock' and value:
             self.__create_tables(self.__cursor, value)


    def __create_connectionPool(self, max_num_thread):
        dbconfig = {
            "database": self.__DB_NAME,
            "user": "root",
            "host": self.__SERVER_IP,
            "passwd": "root",
        }
        self.cnxpool = mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool",
                                                                   pool_size=max_num_thread,
                                                                   **dbconfig)

    def __create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.__DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def __create_tables(self, cursor, stocks):
        if stocks is not None and stocks:
            for i, stock in enumerate(stocks):
                for name, ddl in self.__TABLES.iteritems():
                    try:
                        cursor.execute(ddl % stock)
                    except mysql.connector.Error as err:
                        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                            print 'create tables error ALREADY EXISTS'
                        else:
                            print 'create tables error ' + err.msg
                    else:
                        print 'Tables created - ', i


    def enqueue_stock(self, stock_code, date, stock_data):
        con = self.cnxpool.get_connection()
        cursor = con.cursor()
        try:
            string = "INSERT INTO t_%s (code, date, open, high, close, low, volume, price_change, p_change, ma5, ma10, ma20, v_ma5, v_ma10, v_ma20) " % (stock_code)
            add_stock = (string + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_stock = (stock_code, date, float(stock_data.open), float(stock_data.high), float(stock_data.close), float(stock_data.low), float(stock_data.volume), float(stock_data.price_change), float(stock_data.p_change), float(stock_data.ma5), float(stock_data.ma10), float(stock_data.ma20), float(stock_data.v_ma5), float(stock_data.v_ma10), float(stock_data.v_ma20))
            cursor.execute(add_stock, data_stock)
            con.commit()
            # print stock_code + 'insert success'
        except mysql.connector.Error as err:
            print 'enqueueUrl() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()


    def dequeue_stock(self, stock_code):
        con = self.cnxpool.get_connection()
        cursor = con.cursor(dictionary=True)
        try:
            query = ("SELECT * FROM t_%s ORDER BY `date` ASC") % (stock_code)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            row = cursor.fetchall()
            con.commit()
            return row
        except mysql.connector.Error as err:
            print 'dequeueUrl() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()
