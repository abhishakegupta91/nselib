# nselib

A Python library to fetch publicly available data from NSE India.

## Test and Build Commands
```bash
# Run tests
pytest
```

## Critical Pointers
- **Data Modules**: `capital_market`, `derivatives`, `cash_market`, `indices`, `debt`.
- **Paging**: Implements smart pagination for historical endpoints.
- **Dates**: Accepts either `from_date` + `to_date` (dd-mm-YYYY) or `period` ('1M', '1Y').
- **Internal Helper**: Network requests are encapsulated in `CapitalMarketHelper`.

For the full API reference and examples, refer to `README.md`.
