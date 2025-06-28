from abc import ABC, abstractmethod
from Brokerage import Brokerage
from DB import Event
from HistoricalData import HistoricalData
import datetime

class Strategy(ABC):
    @abstractmethod
    def run(self, timestamp: datetime.datetime, events: list[Event], brokerage: Brokerage, historical_data: HistoricalData):
        pass

class BlankStrategy(Strategy):
    def run(self, timestamp: datetime.datetime, events: list[Event], brokerage: Brokerage, historical_data: HistoricalData):
        pass
    
class SAndP500Strategy(Strategy):
    def run(self, timestamp: datetime.datetime, events: list[Event], brokerage: Brokerage, historical_data: HistoricalData):
        if len(brokerage.get_positions()) > 0:
            return
        
        ivv_price = brokerage.get_ticker_price('IVV')
        cash = brokerage.get_cash()

        quantity = cash // ivv_price[1]
        
        brokerage.place_buy_trade('IVV', quantity)
        