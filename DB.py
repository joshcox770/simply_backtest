from abc import ABC, abstractmethod
from datetime import datetime, date
from enum import Enum
from sqlite3 import Connection, Cursor
import sqlite3
import time

EVENT_TYPE_OHLCV = "OHLCV"
EVENT_TYPE_DIVIDEND_ANNOUNCEMENT = "DIVIDEND_ANNOUNCEMENT"
EVENT_TYPE_EX_DIVIDEND = "EX_DIVIDEND"
EVENT_TYPE_DIVIDEND_PAYMENT = "DIVIDEND_PAYMENT"
EVENT_TYPE_EARNINGS = "EARNINGS"

EVENT_SELECT_ALL = """
            SELECT 
                e.id, e.type, e.begin, e.end, e.ticker, e.exchange,
                o.open, o.high, o.low, o.close, o.volume,
                da.amount as da_amount, da.ex_dividend_id, da.dividend_payment_id,
                ed.amount as ed_amount, ed.dividend_payment_id as ed_payment_id,
                dp.amount as dp_amount,
                ear.eps as eps, ear.eps_estimate as eps_estimate, ear.number_of_estimates as number_of_estimates, ear.fiscal_quarter_ending as fiscal_quarter_ending,
                ex_dividend.begin as ex_dividend_begin,
                dividend_payment.begin as dividend_payment_begin
            FROM event e
            LEFT JOIN ohlcv o ON e.id = o.event_id
            LEFT JOIN dividend_announcement da ON e.id = da.event_id
            LEFT JOIN ex_dividend ed ON e.id = ed.event_id
            LEFT JOIN dividend_payment dp ON e.id = dp.event_id
            LEFT JOIN earnings ear ON e.id = ear.event_id
            LEFT JOIN event ex_dividend ON da.ex_dividend_id = ex_dividend.id
            LEFT JOIN event dividend_payment ON da.dividend_payment_id = dividend_payment.id
        """

class Event(ABC):
    begin: datetime
    end: datetime
    ticker: str
    exchange: str

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str):
        self.begin = begin
        self.end = end
        self.ticker = ticker
        self.exchange = exchange

class OHLCV(Event):
    open: datetime
    high: datetime
    low: int
    close: int
    volume: int

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str, open: int, high: int, low: int, close: int, volume: int):
        super().__init__(begin, end, ticker, exchange)
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume

class DividendAnnouncement(Event):
    amount: int
    exDividend: datetime.date
    payment: datetime.date

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str, amount: int, exDividend: datetime.date, payment: datetime.date):
        super().__init__(begin, end, ticker, exchange)
        self.amount = amount
        self.exDividend = exDividend
        self.payment = payment

class ExDividend(Event):
    amount: int
    exDividend: datetime.date
    payment_date: datetime.date

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str, amount: int, exDividend: datetime.date, payment_date: datetime.date):
        super().__init__(begin, end, ticker, exchange)
        self.amount = amount
        self.exDividend = exDividend
        self.payment_date = payment_date

class DividendPayment(Event):
    amount: int

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str, amount: int):
        super().__init__(begin, end, ticker, exchange)
        self.amount = amount

class Earnings(Event):
    eps: float
    eps_estimate: float | None
    number_of_estimates: int
    fiscal_quarter_ending: datetime.date

    def __init__(self, begin: datetime, end: datetime, ticker: str, exchange: str, eps: float, eps_estimate: float, number_of_estimates: int, fiscal_quarter_ending: str):
        super().__init__(begin, end, ticker, exchange)
        self.eps = eps
        self.eps_estimate = eps_estimate
        self.number_of_estimates = number_of_estimates
        self.fiscal_quarter_ending = fiscal_quarter_ending

class DB(ABC):
    @abstractmethod
    def get_events(self, ticker: str | None = None, begin: tuple[datetime, datetime] | None = None, end: tuple[datetime, datetime] | None = None):
        pass

    @abstractmethod
    def get_latest_event(self, ticker: str, type: str | None = None):
        pass

class SqliteDB(DB):
    db_connection: Connection

    def __init__(self, path: str):
        self.db_connection = sqlite3.connect(path)

    def get_events(self, ticker: str | None = None, event_type: str | None = None, begin: tuple[datetime, datetime] | None = None, end: tuple[datetime, datetime] | None = None) -> list[Event]:
        cursor = self.db_connection.cursor()

        select = EVENT_SELECT_ALL
        where: list[str] = []
        params: list[str] = []

        if ticker is not None:
            where.append("e.ticker = ?")
            params.append(ticker)

        if event_type is not None:
            where.append("e.type = ?")
            params.append(event_type)

        if begin is not None:
            where.append("e.begin >= ? AND e.begin <= ?")
            params.append(begin[0])
            params.append(begin[1])

        if end is not None:
            where.append("e.end >= ? AND e.end <= ?")
            params.append(end[0])
            params.append(end[1])

        query = self._construct_query(select, where, "ORDER BY e.begin ASC")
        
        cursor.execute(query, params)

        results = cursor.fetchall()
        events = list(map(lambda db_row: self.sql_to_event_from_joined_row(db_row), results))

        return events

    def get_latest_event(self, ticker: str | None = None, type: str | None = None, current_timestamp: datetime | None = None) -> Event | None:
        cursor = self.db_connection.cursor()

        select = EVENT_SELECT_ALL
        where: list[str] = []
        params: list[str] = []

        if ticker is not None:
            where.append("e.ticker = ?")
            params.append(ticker)

        if type is not None:
            where.append("e.type = ?")
            params.append(type)

        if current_timestamp is not None:
            where.append("e.begin <= ?")
            params.append(current_timestamp)
        
        cursor.execute(self._construct_query(select, where, "ORDER BY e.begin DESC LIMIT 1"), params)

        result = cursor.fetchone()
        if result is None:
            return None

        return self.sql_to_event_from_joined_row(result)

    def sql_to_event(self, cursor: Cursor, db_row: tuple[int, str, datetime, datetime, str, str]) -> Event:
        match db_row[1]:
            case 'OHLCV':
                cursor.execute("SELECT open, high, low, close, volume FROM ohlcv WHERE event_id = ?", [db_row[0]])
                ohlcv_row = cursor.fetchone()
                return OHLCV(db_row[2], db_row[3], db_row[4], db_row[5], ohlcv_row[0], ohlcv_row[1], ohlcv_row[2], ohlcv_row[3], ohlcv_row[4])
            case 'DIVIDEND_ANNOUNCEMENT':
                cursor.execute("SELECT amount, ex_dividend_id, dividend_payment_id FROM dividend_announcement WHERE event_id = ?", [db_row[0]])
                dividend_announcement_row = cursor.fetchone()

                cursor.execute("SELECT begin FROM event WHERE id = ?", [dividend_announcement_row[1]])
                ex_dividend_begin = cursor.fetchone()[0]

                cursor.execute("SELECT begin FROM event WHERE id = ?", [dividend_announcement_row[2]])
                dividend_payment_begin = cursor.fetchone()[0]

                return DividendAnnouncement(db_row[2], db_row[3], db_row[4], db_row[5], dividend_announcement_row[0], ex_dividend_begin, dividend_payment_begin)
            case 'EX_DIVIDEND':
                cursor.execute("SELECT amount, dividend_payment_id FROM ex_dividend WHERE event_id = ?", [db_row[0]])
                ex_dividend_row = cursor.fetchone()

                cursor.execute("SELECT begin FROM event WHERE id = ?", [ex_dividend_row[1]])
                ex_dividend_begin = cursor.fetchone()[0]
                
                return ExDividend(db_row[2], db_row[3], db_row[4], db_row[5], ex_dividend_row[0], ex_dividend_begin)
            case 'DIVIDEND_PAYMENT':
                cursor.execute("SELECT amount FROM dividend_payment WHERE event_id = ?", [db_row[0]])
                dividend_payment_row = cursor.fetchone()
                
                return DividendPayment(db_row[2], db_row[3], db_row[4], db_row[5], dividend_payment_row[0])

    def sql_to_event_from_joined_row(self, db_row: tuple) -> Event:
        # Unpack the joined row
        (id, type, begin, end, ticker, exchange,
         open, high, low, close, volume,
         da_amount, ex_dividend_id, dividend_payment_id,
         ed_amount, ed_payment_id,
         dp_amount,
         eps, eps_estimate, number_of_estimates, fiscal_quarter_ending,
         ex_dividend_begin,
         dividend_payment_begin) = db_row

        # Convert string dates to datetime objects if they exist
        if ex_dividend_begin is not None:
            ex_dividend_begin = datetime.fromisoformat(ex_dividend_begin)
        if dividend_payment_begin is not None:
            dividend_payment_begin = datetime.fromisoformat(dividend_payment_begin)

        match type:
            case 'OHLCV':
                return OHLCV(begin, end, ticker, exchange, open, high, low, close, volume)
            case 'DIVIDEND_ANNOUNCEMENT':
                return DividendAnnouncement(begin, end, ticker, exchange, da_amount, ex_dividend_begin, dividend_payment_begin)
            case 'EX_DIVIDEND':
                # we only store the id of the payment event on ex_dividend so to get the upcoming payment date we need to query on that
                cursor = self.db_connection.cursor()
                cursor.execute("SELECT begin FROM event WHERE id = ?", [ed_payment_id])
                dividend_payment_begin = cursor.fetchone()[0]
                if dividend_payment_begin is not None:
                    dividend_payment_begin = datetime.fromisoformat(dividend_payment_begin)

                return ExDividend(begin, end, ticker, exchange, ed_amount, ex_dividend_begin, dividend_payment_begin)
            case 'DIVIDEND_PAYMENT':
                return DividendPayment(begin, end, ticker, exchange, dp_amount)
            case 'EARNINGS':
                return Earnings(begin, end, ticker, exchange, eps, eps_estimate, number_of_estimates, fiscal_quarter_ending)

    def _construct_query(self, select: str, where: list[str], order_by: str) -> str:
        query = f"{select} { 'WHERE' if len(where) > 0 else '' } { ' AND '.join(where) } {order_by}"

        return query
