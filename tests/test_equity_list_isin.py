import unittest
from unittest.mock import patch

import pandas as pd

from nselib.capital_market import api


def _make_master_df(isin_column_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "SYMBOL": ["RELIANCE"],
            "NAME OF COMPANY": ["Reliance Industries Limited"],
            " SERIES": ["EQ"],
            " DATE OF LISTING": ["1995-11-29"],
            " PAID UP VALUE": [10],
            " MARKET LOT": [1],
            isin_column_name: ["INE002A01018"],
            " FACE VALUE": [10],
        }
    )


class TestEquityListIsin(unittest.TestCase):
    def test_isin_number_with_leading_space_is_normalized(self):
        """NSE's real CSV header has a leading space on this column."""
        with patch.object(api.cm_helper, "get_equity_list", return_value=_make_master_df(" ISIN NUMBER")):
            df = api.equity_list()
        self.assertIn("ISIN NUMBER", df.columns)
        self.assertEqual(df["ISIN NUMBER"].iloc[0], "INE002A01018")

    def test_isin_number_without_leading_space_is_kept_as_is(self):
        with patch.object(api.cm_helper, "get_equity_list", return_value=_make_master_df("ISIN NUMBER")):
            df = api.equity_list()
        self.assertIn("ISIN NUMBER", df.columns)
        self.assertEqual(df["ISIN NUMBER"].iloc[0], "INE002A01018")

    def test_missing_isin_column_degrades_gracefully(self):
        master_df = _make_master_df(" ISIN NUMBER").drop(columns=[" ISIN NUMBER"])
        with patch.object(api.cm_helper, "get_equity_list", return_value=master_df):
            df = api.equity_list()
        self.assertNotIn("ISIN NUMBER", df.columns)
        self.assertIn("SYMBOL", df.columns)


if __name__ == "__main__":
    unittest.main()
