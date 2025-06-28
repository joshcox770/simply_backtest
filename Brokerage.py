from abc import ABC, abstractmethod
import datetime
from DB import DividendAnnouncement, ExDividend, DividendPayment, Event, OHLCV
from HistoricalData import HistoricalData

class Position:
    def __init__(self, symbol: str, quantity: int, buy_price: int):
        self.symbol = symbol
        self.quantity = quantity
        self.buy_price = buy_price

class PNL:
    def __init__(self, ticker: str, quantity: int, unit_buy_price: int, unit_sell_price: int):
        self.ticker = ticker
        self.quantity = quantity
        self.unit_buy_price = unit_buy_price
        self.unit_sell_price = unit_sell_price

class PendingDividend:
    def __init__(self, ticker: str, amount: int, payment_date: datetime.date):
        self.ticker = ticker
        self.amount = amount
        self.payment_date = payment_date

class Brokerage(ABC):
    _historical_data: HistoricalData
    _positions: list[Position]
    _pnls: list[PNL]

    def __init__(self, historical_data: HistoricalData):
        self._historical_data = historical_data

    @abstractmethod
    def get_cash(self) -> int:
        pass

    @abstractmethod
    def get_positions(self) -> list[Position]:
        pass

    @abstractmethod
    def place_buy_trade(self, symbol: str, quantity: int) -> Position | None:
        pass

    @abstractmethod
    def place_sell_trade(self, symbol: str, quantity: int) -> list[PNL] | None:
        pass

    @abstractmethod
    def get_pnls(self) -> list[PNL]:
        pass

    @abstractmethod
    def get_ticker_price(self, ticker: str) -> tuple[int, int]:
        pass

    @abstractmethod
    def deposit_cash(self, amount: int):
        pass

    @abstractmethod
    def handle_events(self):
        pass

    @abstractmethod
    def handle_end_of_day(self, date: datetime.date):
        pass

class SimpleBrokerage(Brokerage):
    _cash: int
    _positions: list[Position]
    _pending_dividends: list[PendingDividend]
    _historical_data: HistoricalData
    
    def __init__(self, cash: int, historical_data: HistoricalData):
        self._cash = cash
        self._positions = []
        self._pending_dividends = []
        self._historical_data = historical_data

    def get_cash(self):
        return self._cash

    def get_positions(self):
        return self._positions
    
    def place_buy_trade(self, symbol: str, quantity: int):
        # make sure that at the current price they have enough money
        current_price = self.get_ticker_price(symbol)
        cash_needed = current_price[1] * quantity
        if cash_needed > self.get_cash():
            return None
        
        self._cash -= cash_needed

        position = Position(symbol, quantity, current_price)

        self._positions.append(position)
        return position

    def place_sell_trade(self, symbol: str, quantity: int):
        # make sure that they have the position
        reduced_positions = self._reduce_position(symbol, quantity)
        if reduced_positions is None:
            return None
        
        current_price = self.get_ticker_price(symbol)
        cash_gained = current_price[0] * quantity
        self._cash += cash_gained
        
        pnls = list(map(lambda x: PNL(symbol, x[1], x[0], current_price), reduced_positions))
        self._pnls.extend(pnls)
        return pnls
    
    def get_pnls(self):
        return self._pnls
    
    def get_ticker_price(self, ticker: str) -> tuple[int, int]:
        current_price = self._historical_data.get_current_price(ticker)
        if current_price is None:
            print(f"No price found for {ticker}")
            return None
        
        bid_ask_spread = self._get_bid_ask_spread(ticker)
        return (current_price - bid_ask_spread/2, current_price + bid_ask_spread/2)
    
    def deposit_cash(self, amount: int):
        self._cash += amount

    def handle_events(self, events: list[Event]):
        positions_by_ticker = {}
        for position in self._positions:
            if position.symbol not in positions_by_ticker:
                positions_by_ticker[position.symbol] = []
            positions_by_ticker[position.symbol].append(position)
        
        for event in events:
            if isinstance(event, ExDividend):
                if event.ticker in positions_by_ticker:
                    total_quantity = sum(position.quantity for position in positions_by_ticker[event.ticker])
                    print(f"You are entitled to {event.amount} per share of {event.ticker} for {total_quantity} shares on {event.payment_date}")
                    self._add_pending_dividend(event.ticker, event.amount * total_quantity, event.payment_date)
    
    def handle_end_of_day(self, date: datetime.date):
        self._handle_pending_dividends_for_day(date)
    
    def get_brokerage_value(self):
        return self.get_cash() + sum(position.quantity * self.get_ticker_price(position.symbol)[0] for position in self._positions)

    def _find_positions_with_symbol(self, symbol: str) -> list[Position]:
        positions = []
        for position in self._positions:
            if position.symbol == symbol:
                positions.append(position)
        return positions
    
    def _add_position(self, position: Position):
        self._positions.append(position)
            
    def _reduce_position(self, symbol: str, quantity: int):
        positions = self._find_positions_with_symbol(symbol)
        if len(positions) == 0:
            return None
        
        total_quantity = sum(position.quantity for position in positions)
        if total_quantity > quantity:
            position_prices_removed: list[tuple[int, int]] = []

            for position in positions:
                if position.quantity > quantity:
                    position.quantity -= quantity
                    position_prices_removed.append((position.buy_price, quantity))
                    return position_prices_removed
                else:
                    quantity -= position.quantity
                    positions.remove(position)
                    position_prices_removed.append((position.buy_price, position.quantity))

            return position_prices_removed
        else:
            return None
        
    def _get_bid_ask_spread(self, ticker: str) -> int:
        # Get recent OHLCV data for the ticker
        current_time = self._historical_data.get_timestamp()
        if not isinstance(current_time, datetime.datetime):
            return 0  # Invalid timestamp type
            
        lookback_period = datetime.timedelta(days=30)  # Use 30 days of data
        start_time = current_time - lookback_period
        
        events = self._historical_data.get_events(ticker, start_time, current_time)
        ohlcv_events = [event for event in events if isinstance(event, OHLCV)]
        
        if len(ohlcv_events) < 2:
            return 0  # Not enough data to calculate spread
            
        # Calculate price changes using closing prices
        price_changes = []
        for i in range(1, len(ohlcv_events)):
            price_change = ohlcv_events[i].close - ohlcv_events[i-1].close
            price_changes.append(price_change)
            
        # Calculate serial covariance
        if len(price_changes) < 2:
            return 0
            
        mean_change = sum(price_changes) / len(price_changes)
        covariance = sum((price_changes[i] - mean_change) * (price_changes[i-1] - mean_change) 
                        for i in range(1, len(price_changes))) / (len(price_changes) - 1)
        
        # Apply Roll's formula: Spread = 2 * sqrt(-covariance)
        if covariance >= 0:
            return 0  # Invalid covariance, return 0 spread
            
        spread = int(2 * (-covariance) ** 0.5)
        return spread
    
    def _add_pending_dividend(self, ticker: str, total_amount: int, payment_date: datetime.date):
        self._pending_dividends.append(PendingDividend(ticker, total_amount, payment_date))

    def _handle_pending_dividends_for_day(self, day: datetime.date):
        for pending_dividend in self._pending_dividends:
            if day >= pending_dividend.payment_date.date():
                self._cash += pending_dividend.amount
                self._pending_dividends.remove(pending_dividend)