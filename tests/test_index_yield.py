from unittest.mock import MagicMock, patch
import pandas as pd
from nselib import capital_market


def test_index_yield_data(mocker):
    mock_json = {
        "data": [
            {
                "EOD_INDEX_NAME": "NIFTY 50",
                "EOD_TIMESTAMP": "10-AUG-2026",
                "EOD_P_E": "21.45",
                "EOD_P_B": "3.85",
                "EOD_DIV_YIELD": "1.25",
            }
        ]
    }

    mocker.patch(
        "nselib.capital_market.network.CapitalMarketHelper._fetch_json",
        return_value=mock_json,
    )

    df = capital_market.index_yield_data(index="NIFTY 50", period="1D")

    assert not df.empty
    assert "INDEX_NAME" in df.columns or "IndexName" in df.columns or "EOD_INDEX_NAME" in df.columns
