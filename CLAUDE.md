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

## 🤖 Agent Guidelines (`nselib-agent`)
- **Execution**: Coding only. Nothing runs locally; all runs on algoserver.
- **Git**: Separate repo. Commit locally (2-3+ lines descriptive message), push, pull on algoserver. No scp.
- **Design**: NSE scraping / derivatives downloader. Encapsulate calls in `CapitalMarketHelper`. Handle pagination safely.

