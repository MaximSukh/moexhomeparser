# moexhomeparser
 simple MOEX parser for home use

## Description

The `MOEX` class provides methods to interact with the Moscow Exchange (MOEX) ISS API. It allows users to retrieve various financial data related to securities, bonds, market data, historical yields, dividends, and more.

## MOEX Class Methods

- `__init__(self, ticker)`: Initializes the MOEX class with a given ticker.
- `get_board(self)`: Returns a list of stock exchange boards for a ticker.
- `get_market(self)`: Returns the market for a ticker.
- `get_engine(self)`: Returns the engine for a ticker.
- `get_description(self, param=None)`: Returns a dataframe with the description of a ticker. If a parameter is provided, it returns the specific parameter value.
- `_parse_market_data(self)`: Parses market data for a given ticker and returns it as a pandas Series.
- `_parse_history_yields(self, date_from=None, date_to=None)`: Parses historical yield data for a given ticker within a specified date range and returns it as a pandas DataFrame.
- `_parse_history_results(self, date_from=None, date_to=None)`: Parses historical trading data for a given ticker within a specified date range and returns it as a pandas DataFrame.
- `get_offers(self)`: Returns a schedule of offers for a bond.
- `get_offer_date(self)`: Returns the next offer date of a bond in DateTime format.
- `get_price(self, date=None, type='last')`: Returns the last clean price for a ticker.
- `get_ticker_currency(self)`: Returns the currency name the ticker is traded in.
- `get_dividends(self)`: Returns a list of known dividends for a ticker.
- `get_trade_currency(self)`: Returns the trade currency for a ticker.
- `get_coupons(self)`: Returns a schedule of coupon payments for a bond.
- `get_amortization(self)`: Returns a schedule of redemption payments for a bond.
- `get_bond_schedule(self, till_offer=False, last_coupon=False)`: Returns an unprocessed schedule of payments for a bond, including coupons, amortization payments, and offers.
- `find_ticker(self, type=None)`: Finds and returns ticker information from the MOEX ISS API.
- `get_candles(self, date_from=None, date_to=None, interval=24)`: Retrieves candle data for a given ticker within a specified date range and interval.
- `get_indices_groups(name=None)`: Returns a list of indice groups.
- `get_indice_tickers(ticker)`: Returns a list of tickers in an indice and their weights.
- `get_zcurve_params(date=None)`: Returns zero-coupon yield curve parameters for a given date.
- `get_zcurve_params_history()`: Fetches and processes the zero-coupon yield curve parameters history from the Moscow Exchange.
- `get_zcurve_prices(date=None)`: Fetches and returns zero-coupon yield curve prices for a given date.
- `calculate_zyield(p, t)`: Calculates the yield based on given parameters.
- `get_zyield_for_maturity(t, date=None)`: Calculates the zero-coupon yield for a given maturity.