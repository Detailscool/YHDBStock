# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, WeekdayLocator, DayLocator, MONDAY, date2num
from matplotlib.finance import candlestick_ohlc
from datetime import datetime
import pandas as pd
import matplotlib.ticker as tk
import datetime as dt

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class StockPylot:
    @classmethod
    def line_plot(cls, data):
        data = pd.DataFrame(data)
        print len(data), '条数据\n', data

        dates = data.date
        opens = data.open
        closes = data.close

        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.plot_date(dates, opens, 'b-')
        ax.plot_date(dates, closes, 'g-')

        # format the ticks
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
        ax.autoscale_view()

        # format the coords message box
        def price(x):
            return '¥%1.2f' % x

        ax.fmt_xdata = DateFormatter('%Y-%m-%d')
        ax.fmt_ydata = price
        ax.grid(True)

        fig.autofmt_xdate()
        plt.title(data.code[0])
        plt.show()

    @classmethod
    def bar_plot(cls, data):
        width = 0.6
        data = pd.DataFrame(data)
        print len(data), '条数据\n', data

        plt.figure(figsize=(len(data)*1.5, 9), facecolor='w')

        def to_datetime(date):
            date_str = date.encode('utf-8')
            return datetime.strptime(date_str, "%Y-%m-%d")

        dates = map(date2num, map(to_datetime, data.date))
        ohlc = zip(dates, data.open, data.high, data.low, data.close)

        # print 'ohlc:\n', ohlc

        ax_candle = plt.subplot2grid((10, 8), (0, 0), rowspan=6, colspan=10)
        candlestick_ohlc(ax_candle, ohlc, width=width, colorup='r', colordown='g')
        plt.grid(True)

        ax_bar = plt.subplot2grid((10, 8), (6, 0), rowspan=2, colspan=10)
        ax_bar.bar(dates, data.volume/1000, width=width, align='center')
        plt.grid(True)

        # def t_formatter(_i, pos):
        #     i = int(_i)
        #     if i > len(data)-1:
        #         i = len(data) - 1
        #     elif i < 0:
        #         i = 0
        #     if data.date[i] == 0:
        #         return ""
        #     d = dt.date.fromtimestamp(data.date[i])
        #     return d.strftime('$%Y-%m-%d$')
        #
        # ax_candle.xaxis.set_major_locator(DayLocator())
        # ax_candle.xaxis.set_major_formatter(WeekdayLocator(MONDAY))
        ax_candle.xaxis.set_minor_locator(DayLocator())
        # ax_bar.xaxis.set_major_locator(DayLocator())
        # ax_bar.xaxis.set_major_formatter(WeekdayLocator(MONDAY))
        ax_bar.xaxis.set_minor_locator(DayLocator())

        ax_candle.set_title(data.code[0])
        ax_candle.set_ylabel('Price')
        ax_bar.set_ylabel('Volum(A thousand)')

        ax_candle.xaxis_date()
        ax_candle.autoscale_view()
        ax_bar.xaxis_date()
        ax_bar.autoscale_view()

        x_min, x_max = ax_candle.get_xlim()
        gap = x_max - x_min
        print 'x_max:', x_max, '-', 'x_min', x_min
        ax_candle.set_xlim((x_min-0.02*gap, x_max+0.02*gap))
        ax_bar.set_xlim((x_min-0.02*gap, x_max+0.02*gap))

        plt.setp(plt.gca().get_xticklabels(), rotation=90, horizontalalignment='right')
        plt.setp(ax_candle.yaxis.get_ticklabels()[0], visible=False)
        plt.subplots_adjust(bottom=0.2, top=0.9, hspace=0)

        plt.show()

