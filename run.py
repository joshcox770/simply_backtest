import datetime
from Brokerage import SimpleBrokerage
from Backtester import Backtester
from Strategy import DividendStrategy, SAndP500Strategy
from HistoricalData import SQLHistoricalData
from DB import SqliteDB

historical_data = SQLHistoricalData(SqliteDB("./event.sqlite"))

market_backtester = Backtester(SAndP500Strategy(), SimpleBrokerage(1000000, historical_data), historical_data)
market_end_equity = market_backtester.run(datetime.date(2024, 9, 1), datetime.date(2024, 12, 31))


print(market_end_equity)
