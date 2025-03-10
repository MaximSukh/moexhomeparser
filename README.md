# moexhomeparser
 simple MOEX parser for home use

## Description

The `MOEX` class provides methods to interact with the Moscow Exchange (MOEX) ISS API. It allows users to retrieve various financial data related to securities, bonds, market data, historical yields, dividends, and more.

## Installation

To install the `moexhomeparser` package, you can use pip:

```sh
pip install moexhomeparser
```

## Usage

Here's a basic example of how to use the `MOEX` class:

```python
from moexhomeparser import MOEX

# Initialize the MOEX class with a ticker
moex = MOEX('GAZP')

# Get market data
market_data = moex.get_market()
print(market_data)

# # Get historical yields
# historical_yields = moex._parse_history_yields(date_from='2022-01-01', date_to='2022-12-31')
# print(historical_yields)
```

## Requirements

- Python 3.6+
- pandas
- requests

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please read the [CONTRIBUTING](CONTRIBUTING.md) guidelines first.

## Acknowledgments

- The Moscow Exchange for providing the ISS API.
- The developers and contributors of the `moexhomeparser` package.
