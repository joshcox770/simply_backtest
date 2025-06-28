from abc import ABC, abstractmethod
import datetime
from DB import SqliteDB, Event

class HistoricalData(ABC):
    @abstractmethod
    def __init__(self, starting_timestamp: datetime.datetime):
        pass

    @abstractmethod
    def update_timestamp(self, timestamp: datetime.datetime):
        pass

    @abstractmethod
    def get_events(self, ticker: str, begin: datetime.datetime, end: datetime.datetime) -> list[Event]:
        pass

    @abstractmethod
    def get_current_price(self, ticker: str) -> int:
        pass
    

class SQLHistoricalData(HistoricalData):
    _current_timestamp: datetime.datetime
    _db: SqliteDB

    def __init__(self, db: SqliteDB):
        self._current_timestamp = datetime.datetime.now()
        self._db = db

    def get_events(self, begin: datetime.datetime, end: datetime.datetime, ticker: str | None = None):
        if begin > self._current_timestamp:
            begin = self._current_timestamp
        if end > self._current_timestamp:
            end = self._current_timestamp

        return self._db.get_events(begin=[begin, end], ticker=ticker)

    def get_events_unrestricted(self, begin: datetime.datetime, end: datetime.datetime):
        return self._db.get_events(begin=[begin, end])

    def update_timestamp(self, timestamp: datetime.datetime):
        self._current_timestamp = timestamp

    def get_timestamp(self) -> datetime.datetime:
        return self._current_timestamp

    def get_current_price(self, ticker: str) -> int | None:
        latest_ohlcv = self._db.get_latest_event(ticker, 'OHLCV', self._current_timestamp)

        if latest_ohlcv is None:
            return None

        return latest_ohlcv.open
    
