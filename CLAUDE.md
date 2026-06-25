# NSELib

Python library for fetching publicly available data from NSE India. Used by `tb-collector`.

## Commands

```bash
venv/bin/pytest     # tests
pip install -e .    # install as editable
```

## Data Modules

| Module | Contents |
|--------|----------|
| `capital_market` | Equity prices, corporate actions, bulk deals |
| `derivatives` | Option chains, futures OI, PCR |
| `cash_market` | Intraday data |
| `indices` | Index constituents, index OHLCV |
| `debt` | Bond data |

## Usage

```python
from nselib import capital_market, derivatives

# Equity history — use dd-mm-YYYY date format
df = capital_market.price_volume_and_deliverable_position_data(
    symbol="RELIANCE", from_date="01-01-2026", to_date="30-06-2026"
)

# Or use period shorthand
df = capital_market.price_volume_and_deliverable_position_data(
    symbol="RELIANCE", period="1Y"
)

# Option chain
chain = derivatives.nse_live_option_chain(symbol="NIFTY")
```

## Conventions

- **All HTTP**: encapsulated in `CapitalMarketHelper` — handles NSE session/cookie management. Never call requests/httpx directly.
- **Dates**: `from_date` + `to_date` as `dd-mm-YYYY`, or `period` as `'1M'`, `'3M'`, `'1Y'`
- **Pagination**: historical endpoints auto-paginate — do not manually chunk date ranges
- **Rate limiting**: NSE blocks aggressive scrapers. Add delays between batch calls in callers.
- **None handling**: NSE returns empty/None on market holidays — always handle `NoneType` responses

## Git

Separate sub-repo. Commit locally → push → `ssh algoserver "cd /home/abhi-trade/nx-trade/libs/nselib && git pull"`.
