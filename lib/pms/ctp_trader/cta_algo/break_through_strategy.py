# -*- coding:utf-8 -*-
# ***********************************************************#
#     File: Break through strategy
#   Author: Myron
# ***********************************************************#
import numpy as np
import datetime as dt
from cta_base import *
from cta_template import CtaTemplate
from data.market_server import MarketServer


class Position(object):

    def __init__(self):
        self.short_position = 0
        self.long_position = 1

    @property
    def total_position(self):
        return self.short_position + self.long_position

    def update_position(self, trade_string=None, trade_amount=0):
        if trade_string == 'buy':
            self.long_position += trade_amount
        elif trade_string == 'sell':
            self.long_position -= trade_amount
        elif trade_string == 'short':
            self.short_position += trade_amount
        elif trade_string == 'cover':
            self.short_position -= trade_amount

    def __repr__(self):
        return str(self.__dict__)


class BreakThroughSignal(object):

    history_data = dict()
    high_low_points = dict()

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(BreakThroughSignal, cls).__new__(cls, *args)
            cls._instance._information = dict()
            cls._instance._signals = dict()
        return cls._instance

    @staticmethod
    def update_history_data(history_data, refresh_rate, offset=5):
        BreakThroughSignal.history_data = history_data
        for symbol, data in BreakThroughSignal.history_data.iteritems():
            fragment_data = [data.iloc[key*refresh_rate:(key+1)*refresh_rate]
                             for key in range(data.shape[0]/refresh_rate)]
            bar_data = [BreakThroughSignal._generate_bar_data(fragment)
                        for fragment in fragment_data][:-1]
            np_close = np.array(bar_data)[:,1].tolist()
            BreakThroughSignal.high_low_points.setdefault(symbol, [0, 0])
            if np_close.index(max(np_close)) < len(np_close)*4/5:
                BreakThroughSignal.high_low_points[symbol][0] = max(np_close) + offset
            if np_close.index(min(np_close)) < len(np_close)*4/5:
                BreakThroughSignal.high_low_points[symbol][1] = min(np_close) - offset

    @property
    def signals(self):
        for symbol, threshold in self.high_low_points.iteritems():
            high, low = threshold
            history_data = self.history_data[symbol]
            close_price = history_data.iat[-1, history_data.columns.get_loc('closePrice')]
            self._information['signals'] = {
                'price': close_price,
                'high_point': high,
                'low_point': low
            }
            if high and close_price > high:
                self._signals[symbol] = 'long'
            elif low and close_price < low:
                self._signals[symbol] = 'short'
            else:
                self._signals[symbol] = 'hold'
        return self._signals, self._information

    @staticmethod
    def _generate_bar_data(fragment):
        '''
        Generate the bar data using data fragment from history data.

        :param fragment: pandas.DataFrame.
        :return: list of float. [open, close, high, low, volume]
        '''
        open_price = fragment.iat[0, fragment.columns.get_loc('openPrice')]
        close_price = fragment.iat[-1, fragment.columns.get_loc('closePrice')]
        high_price = max(fragment.highPrice)
        low_price = min(fragment.lowPrice)
        volume = sum(fragment.volume)
        return [open_price, close_price, high_price, low_price, volume]


class BreakThroughStrategy(CtaTemplate):

    # **************** strategy initialization ****************** #
    bars_number = 40
    refresh_rate = 15
    symbols = ['bu1709']
    position = Position()
    signal = BreakThroughSignal()
    MarketServer.init_server(symbols=symbols, max_cache_period=3)

    def __int__(self, cta_engine, setting):
        super(BreakThroughStrategy, self).__init__(cta_engine=cta_engine, setting=setting)

    def onInit(self):
        self.writeCtaLog(u'The strategy "{}" is initialized! \n'.format(self.__name__))
        self.putEvent()

    def onStart(self):
        self.writeCtaLog(u'The strategy "{}" is started! \n'.format(self.__name__))
        self.putEvent()

    def onStop(self):
        self.writeCtaLog(u'The strategy "{}" is stopped! \n'.format(self.__name__))
        self.putEvent()

    _bar = EMPTY_STRING
    _bar_minute = EMPTY_STRING

    def onTick(self, tick=CtaTickData()):
        if tick.datetime.minute != self._bar_minute:
            if self._bar:
                self.onBar(self._bar)
            else:
                self.writeCtaLog(content='No available '
                                         'bar data in {}'.format(tick.datetime))
            bar = CtaBarData()
            bar.vtSymbol = tick.vtSymbol
            bar.symbol = tick.symbol
            bar.exchange = tick.exchange
            bar.open = tick.lastPrice
            bar.high = tick.lastPrice
            bar.low = tick.lastPrice
            bar.close = tick.lastPrice
            bar.date = tick.date
            bar.time = tick.datetime.strftime("%Y-%m-%d %H:%M:%S")
            bar.datetime = tick.datetime
            bar.volume = tick.volume
            self._bar = bar
            self._bar_minute = tick.datetime.minute
        else:
            self._bar.high = max(self._bar.high, tick.lastPrice)
            self._bar.low = min(self._bar.low, tick.lastPrice)
            self._bar.close = tick.lastPrice
            self._bar.volume += tick.volume

    def onBar(self, bar=CtaBarData()):
        self.writeCtaLog(content='Information in {}'
                                 ''.format(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        self.log_object_info(bar, keys=['datetime', 'open', 'high', 'low', 'close'],
                             prefix='bar at ')
        self.writeCtaLog(content=''.join(['current position: ', self.position.__repr__()]))
        self.writeCtaLog(content=''.join(['current position: ', str(self.pos)]))
        if dt.datetime.now().hour not in [9, 10, 11, 13, 14, 21, 22]:
            return
        current_minute = dt.datetime.now().minute
        while current_minute == self._bar_minute:
            import time
            time.sleep(0.5)
            current_minute = dt.datetime.now().minute
        if not current_minute % self.refresh_rate:
            history_data = MarketServer.get_symbol_history(symbols=self.symbols,
                                                           time_range=self.refresh_rate * self.bars_number)
            self.signal.update_history_data(history_data=history_data, refresh_rate=self.refresh_rate, offset=2)
            signals, information = self.signal.signals
            self.writeCtaLog(content=''.join(['current high low thres: ', str(information)]))
            self.writeCtaLog(content=''.join(['current trading signal: ', str(signals)]))
            symbol = self.symbols[0]
            signal = signals.get(symbol, None)
            if signal:
                current_long_position = self.position.long_position
                current_short_position = self.position.short_position
                if signal == 'long':
                    if current_short_position:
                        self.cover(price=self._bar.close, volume=1)
                        self.position.update_position('cover', 1)
                    if not current_long_position:
                        self.buy(price=self._bar.close, volume=1)
                        self.position.update_position('buy', 1)
                elif signal == 'short':
                    if current_long_position:
                        self.sell(price=self._bar.close, volume=1)
                        self.position.update_position('sell', 1)
                    if not current_short_position:
                        self.short(price=self._bar.close, volume=1)
                        self.position.update_position('short', 1)
        self.ctaEngine.writeCtaLog('\n')

    def onOrder(self, order):
        self.writeCtaLog(content=str(order))
        pass

    def onTrade(self, trade):
        self.writeCtaLog(content=str(trade))
        pass
