import logging
import zipfile
from io import BytesIO
from typing import Optional, Tuple

import pandas as pd
import requests

from ..errors import NSEdataNotFound
from ..request_maker import default_header, nse_urlfetch

logger = logging.getLogger(__name__)

def cleaning_column_name(columns: pd.Index) -> list:
    return [name.replace(" ", "") for name in columns]

class CapitalMarketHelper:
    """
    A helper class for making network requests to NSE Capital Market endpoints.
    """
    ORIGIN_URL_STAGING = "https://nsewebsite-staging.nseindia.com"
    BASE_URL = "https://www.nseindia.com"
    BASE_API_URL = "https://www.nseindia.com/api"

    def _fetch_csv(self, endpoint: str, origin_url: str, error_msg: str, params: Optional[dict] = None) -> pd.DataFrame:
        url = f"{self.BASE_API_URL}{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"
            
        req = nse_urlfetch(url, origin_url=origin_url)
        if req.status_code == 200:
            df = pd.read_csv(BytesIO(req.content))
            if not df.empty:
                df.columns = cleaning_column_name(df.columns)
            return df
        raise NSEdataNotFound(f"{error_msg} :: HTTP {req.status_code}")

    def _fetch_json(self, endpoint: str, origin_url: str, error_msg: str, params: Optional[dict] = None) -> dict:
        url = f"{self.BASE_API_URL}{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"
            
        req = nse_urlfetch(url, origin_url=origin_url)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound(f"{error_msg} :: HTTP {req.status_code}")

    def get_price_volume_and_deliverable_position_data(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/generateSecurityWiseHistoricalData"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {
            "from": from_date,
            "to": to_date,
            "symbol": symbol,
            "type": "priceVolumeDeliverable",
            "series": "ALL",
            "csv": "true"
        }
        return self._fetch_csv(endpoint, origin_url, "Resource not available for Price Volume Deliverable Data", params=params)

    def get_price_volume_data(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/generateSecurityWiseHistoricalData"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {
            "from": from_date,
            "to": to_date,
            "symbol": symbol,
            "type": "priceVolume",
            "series": "ALL",
            "csv": "true"
        }
        return self._fetch_csv(endpoint, origin_url, "Resource not available for Price Volume Data", params=params)

    def get_deliverable_position_data(self, symbol: str, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/generateSecurityWiseHistoricalData"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {
            "from": from_date,
            "to": to_date,
            "symbol": symbol,
            "type": "deliverable",
            "series": "ALL",
            "csv": "true"
        }
        return self._fetch_csv(endpoint, origin_url, "Resource not available for deliverable_position_data", params=params)

    def get_india_vix_data(self, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/vixhistory"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {
            "from": from_date,
            "to": to_date,
            "csv": "true"
        }
        data_json = self._fetch_json(endpoint, origin_url, "Resource not available", params=params)
        df = pd.DataFrame(data_json.get("data", []))
        if not df.empty:
            df.columns = cleaning_column_name(df.columns)
            from .constants import INDIA_VIX_DATA_COLUMN
            return df[INDIA_VIX_DATA_COLUMN]
        return df

    def get_index_data(self, index: str, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/vixhistory"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {
            "from": from_date,
            "to": to_date,
            "index": index,
            "csv": "true"
        }
        data_json = self._fetch_json(endpoint, origin_url, "Resource not available", params=params)
        df = pd.DataFrame(data_json.get("data", []))
        if not df.empty:
            df.columns = cleaning_column_name(df.columns)
            df = df.drop(columns=["_id", "EOD_INDEX_NAME"], errors="ignore")
        return df

    def get_bulk_deal_data(self, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/bulk-deals"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {"from": from_date, "to": to_date, "csv": "true"}
        return self._fetch_csv(endpoint, origin_url, "Resource not available for bulk_deal_data", params=params)

    def get_block_deals_data(self, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/block-deals"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {"from": from_date, "to": to_date, "csv": "true"}
        return self._fetch_csv(endpoint, origin_url, "Resource not available for block_deals_data", params=params)

    def get_short_selling_data(self, from_date: str, to_date: str) -> pd.DataFrame:
        endpoint = "/historicalOR/short-selling"
        origin_url = f"{self.ORIGIN_URL_STAGING}/report-detail/eq_security"
        params = {"from": from_date, "to": to_date, "csv": "true"}
        return self._fetch_csv(endpoint, origin_url, "Resource not available for short_selling_data", params=params)

    def get_bhav_copy_with_delivery(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code == 200:
            return pd.read_csv(BytesIO(req.content))
        raise FileNotFoundError("Data not found, change the trade_date...")

    def get_bhav_copy_equities(self, use_date_str: str, fallback_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/content/cm/BhavCopy_NSE_CM_0_0_0_{use_date_str}_F_0000.csv.zip"
        req = nse_urlfetch(url)
        if req.status_code == 200:
            zip_bhav = zipfile.ZipFile(BytesIO(req.content), "r")
            for file_name in zip_bhav.filelist:
                if file_name:
                    return pd.read_csv(zip_bhav.open(file_name))
        elif req.status_code == 403:
            raise FileNotFoundError(f"Data not found, change the trade_date...")
        return pd.DataFrame()

    def get_bhav_copy_indices(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/content/indices/ind_close_all_{use_date.upper()}.csv"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"No data available for : {use_date}")
        return pd.read_csv(BytesIO(req.content))

    def get_bhav_copy_sme(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/archives/sme/bhavcopy/sme{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code == 200:
            return pd.read_csv(BytesIO(req.content))
        raise FileNotFoundError("Data not found, change the trade_date...")

    def get_equity_list(self) -> pd.DataFrame:
        url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
        req = nse_urlfetch(url, origin_url=self.ORIGIN_URL_STAGING)
        if req.status_code != 200:
            raise FileNotFoundError("No equity list available")
        return pd.read_csv(BytesIO(req.content))

    def get_fno_equity_list(self) -> dict:
        url = f"{self.BASE_API_URL}/underlying-information"
        origin = f"{self.BASE_URL}/products-services/equity-derivatives-list-underlyings-information"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code != 200:
            raise NSEdataNotFound("Resource not available for fno_equity_list")
        return req.json()

    def get_fno_index_list(self) -> dict:
        url = f"{self.BASE_API_URL}/underlying-information"
        origin = f"{self.BASE_URL}/products-services/equity-derivatives-list-underlyings-information"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code != 200:
            raise NSEdataNotFound("Resource not available for fno_equity_index_list")
        return req.json()

    def get_nifty_equity_list(self, url: str) -> pd.DataFrame:
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"No data available at {url}")
        return pd.read_csv(BytesIO(req.content))

    def get_market_watch_all_indices(self) -> dict:
        url = f"{self.BASE_API_URL}/allIndices"
        req = nse_urlfetch(url, origin_url=self.ORIGIN_URL_STAGING)
        return req.json()

    def get_fii_dii_trading_activity(self) -> dict:
        url = f"{self.BASE_API_URL}/fiidiiTradeReact"
        req = nse_urlfetch(url)
        return req.json()

    def get_daily_volatility(self, payload: str, trade_date: str) -> pd.DataFrame:
        origin_url = f"{self.BASE_URL}/all-reports"
        report_urls = [
            f"https://nsearchives.nseindia.com/archives/nsccl/volt/{payload}",
            f"https://archives.nseindia.com/archives/nsccl/volt/{payload}",
            f"https://www.nseindia.com/api/reports?archives=%5B%7B%22name%22%3A%22CM%20-%20Daily%20Volatility%22%2C%22type%22%3A%22archives%22%2C%22category%22%3A%22capital-market%22%2C%22section%22%3A%22equity%22%7D%5D&date={trade_date}&type=equity&mode=single",
        ]
        r_session = requests.session()
        r_session.trust_env = False
        nse_live = r_session.get(origin_url, headers=default_header)
        cookies = nse_live.cookies
        
        last_status = None
        for url in report_urls:
            report = r_session.get(url, headers=default_header, cookies=cookies)
            last_status = report.status_code
            if report.status_code == 200:
                return pd.read_csv(BytesIO(report.content), skipinitialspace=True)
        raise FileNotFoundError(f"No CM daily volatility data available: status={last_status}")

    def get_category_turnover_cash(self, use_date: str) -> bytes:
        url = f"https://archives.nseindia.com/archives/equities/cat/cat_turnover_{use_date}.xls"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"No data available for : {use_date}")
        return req.content

    def get_var_data(self, url: str) -> pd.DataFrame:
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"Data not found at {url}")
        return pd.read_csv(BytesIO(req.content), names=VAR_COLUMNS) if "VAR_COLUMNS" in globals() else pd.read_csv(BytesIO(req.content), header=None)

    def get_sme_band_complete(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/archives/sme/band/sme_sec_band_{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"Data not found for {use_date}")
        return pd.read_csv(BytesIO(req.content))

    def get_week_52_high_low_report(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/archives/cm/52wk/CM_52_wk_High_low_{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"Data not found for {use_date}")
        return pd.read_csv(BytesIO(req.content))

    def get_corporate_bond_trade_report(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/archives/cb/bhavcopy/cb{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"Data not found for {use_date}")
        return pd.read_csv(BytesIO(req.content))

    def get_pe_ratio(self, use_date: str) -> pd.DataFrame:
        url = f"https://nsearchives.nseindia.com/archives/equities/pe/nifty_pe_{use_date}.csv"
        req = nse_urlfetch(url)
        if req.status_code != 200:
            raise FileNotFoundError(f"Data not found for {use_date}")
        return pd.read_csv(BytesIO(req.content))

    def get_corporate_actions_for_equity(self, payload: str) -> list:
        url = f"{self.BASE_API_URL}/corporate-actions?index=equities&{payload}"
        origin = f"{self.BASE_URL}/market-data/corporate-actions"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_event_calendar_for_equity(self, payload: str) -> list:
        url = f"{self.BASE_API_URL}/event-calendar?{payload}"
        origin = f"{self.BASE_URL}/market-data/event-calendar"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_financial_results_master(self, from_date: str, to_date: str, period: str, fo_sec: bool, fin_period: str) -> Tuple[pd.DataFrame, dict, dict, list]:
        if fo_sec:
            payload = f"from_date={from_date}&to_date={to_date}&fo_sec=true"
        else:
            payload = f"from_date={from_date}&to_date={to_date}"
            
        url = f"{self.BASE_API_URL}/financial-results?index=equities&{payload}"
        origin = f"{self.BASE_URL}/market-data/financial-results"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code != 200:
            raise NSEdataNotFound("Resource not available")
            
        data_list = req.json()
        master_data_df = pd.DataFrame(data_list)
        if "period" in master_data_df.columns:
            master_data_df = master_data_df[master_data_df["period"] == fin_period].copy()

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/",
        }
        ns = {
            "xbrli": "http://www.xbrl.org/2003/instance",
            "in-bse-fin": "http://www.bseindia.com/xbrl/fin/2020-03-31/in-bse-fin",
        }
        keys_to_extract = [
            "ScripCode", "Symbol", "MSEISymbol", "NameOfTheCompany", "ClassOfSecurity",
            "DateOfStartOfFinancialYear", "DateOfEndOfFinancialYear", "RevenueFromOperations",
            "OtherIncome", "Income", "CostOfMaterialsConsumed", "PurchasesOfStockInTrade",
            "EmployeeBenefitExpense", "FinanceCosts", "DepreciationDepletionAndAmortisationExpense",
            "OtherExpenses", "Expenses", "ProfitBeforeTax", "CurrentTax", "DeferredTax",
            "TaxExpense", "ProfitLossForPeriod", "BasicEarningsLossPerShareFromContinuingOperations",
        ]
        return master_data_df, headers, ns, keys_to_extract

    def get_top_gainers_or_losers(self, to_get: str) -> dict:
        url = f"{self.BASE_API_URL}/live-analysis-variations?index={to_get}"
        origin = f"{self.BASE_URL}/market-data/top-gainers-losers"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_most_active_equities(self, fetch_by: str) -> dict:
        url = f"{self.BASE_API_URL}/live-analysis-most-active-securities?index={fetch_by}"
        origin = f"{self.BASE_URL}/market-data/most-active-equities"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_total_traded_stocks(self) -> dict:
        url = f"{self.BASE_API_URL}/live-analysis-stocksTraded"
        origin = f"{self.BASE_URL}/market-data/stocks-traded"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_business_growth_cm_segment_yearly(self) -> dict:
        url = f"{self.BASE_API_URL}/business-growth-cm-segment"
        origin = f"{self.BASE_URL}/market-data/business-growth"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_business_growth_cm_segment_monthly(self, from_year: str, to_year: str) -> dict:
        url = f"{self.BASE_API_URL}/business-growth-cm-segment?from_year={from_year}&to_year={to_year}"
        origin = f"{self.BASE_URL}/market-data/business-growth"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")

    def get_business_growth_cm_segment_daily(self, month: str, year: str) -> dict:
        url = f"{self.BASE_API_URL}/business-growth-cm-segment?month={month}&year={year}"
        origin = f"{self.BASE_URL}/market-data/business-growth"
        req = nse_urlfetch(url, origin_url=origin)
        if req.status_code == 200:
            return req.json()
        raise NSEdataNotFound("Resource not available")
