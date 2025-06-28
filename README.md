# simply_backtest

simply_backtest is a minimalist, event-driven market backtester.

Your strategy is executed every time an event occurs, giving you full flexibility in how your backtester logic is implemented. It has access to the full history of past events that your strategy can query to inform decisions.

The only currently supported data format is an SQLite3 database with the following schema:

```
Table event {
  id INTEGER [pk, increment]
  type TEXT [not null]
  begin DATETIME [not null]
  end DATETIME [not null]
  ticker TEXT [not null]
  exchange TEXT [not null]
}

Table ohlcv {
  event_id INTEGER [pk, ref: > event.id]
  open INTEGER [not null]
  high INTEGER [not null]
  low INTEGER [not null]
  close INTEGER [not null]
  volume INTEGER [not null]
}

Table dividend_announcement {
  event_id INTEGER [pk, ref: > event.id]
  ex_dividend_id INTEGER [ref: > ex_dividend.event_id]
  dividend_payment_id INTEGER [ref: > dividend_payment.event_id]
  amount INTEGER [not null]
}

Table ex_dividend {
  event_id INTEGER [pk, ref: > event.id]
  dividend_announcement_id INTEGER [ref: > dividend_announcement.event_id]
  dividend_payment_id INTEGER [ref: > dividend_payment.event_id]
  amount INTEGER [not null]
}

Table dividend_payment {
  event_id INTEGER [pk, ref: > event.id]
  dividend_announcement_id INTEGER [ref: > dividend_announcement.event_id]
  ex_dividend_id INTEGER [ref: > ex_dividend.event_id]
  amount INTEGER [not null]
}

Table earnings {
  event_id INTEGER [pk, ref: > event.id]
  eps INTEGER [not null]
  eps_estimate INTEGER
  number_of_estimates INTEGER [not null]
  fiscal_quarter_ending DATE [not null]
}
```

All data is recorded as an event which then has many other subtypes declared with further data. Good luck collecting data!
