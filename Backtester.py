import datetime
import time
from Brokerage import Brokerage
from DB import Event
from HistoricalData import HistoricalData
from Strategy import Strategy

class Backtester:
    _brokerage: Brokerage
    _strategy: Strategy
    _historical_data: HistoricalData

    def __init__(self, strategy: Strategy, brokerage: Brokerage, historical_data: HistoricalData):
        self._strategy = strategy
        self._brokerage = brokerage
        self._historical_data = historical_data

    def run(self, start_date: datetime.date, end_date: datetime.date):
        # proceed one day at a time
        start_datetime = datetime.datetime.combine(start_date, datetime.time(0, 0, 0))
        self._historical_data.update_timestamp(start_datetime)
        
        current_date = start_date
        current_date_end = self._get_end_of_day(current_date)

        while current_date_end <= self._get_end_of_day(end_date):
            day_events = self._historical_data.get_events_unrestricted(current_date, current_date_end)

            for events in self._group_events_by_begin_time(day_events):
                self._historical_data.update_timestamp(events[0].end)
                self._brokerage.handle_events(events)
                self._strategy.run(events[0].end, events, self._brokerage, self._historical_data)

            self._brokerage.handle_end_of_day(current_date)

            current_date = current_date + datetime.timedelta(days=1)
            current_date_end = self._get_end_of_day(current_date)
            print(f"Processed day {current_date}: market value {self._brokerage.get_brokerage_value()}")

        return self._brokerage.get_brokerage_value()
    

        

    def _get_end_of_day(self, date: datetime.date) -> datetime.datetime:
        return datetime.datetime.combine(date, datetime.time(23, 59, 59, 999999))
    
    def _group_events_by_begin_time(self, events: list[Event]) -> list[list[Event]]:
        events_by_begin = {}
        for event in events:
            if event.begin not in events_by_begin:
                events_by_begin[event.begin] = []
            events_by_begin[event.begin].append(event)

        return [events_by_begin[end_time] for end_time in sorted(events_by_begin)]
