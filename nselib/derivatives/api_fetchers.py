"""Low-level API fetchers for NSE derivatives data.

This module contains the raw HTTP calls to NSE endpoints. Higher-level
validation, pagination, and formatting live in ``derivative_data.py``.
"""

import logging

import pandas as pd

from nselib.errors import NSEdataNotFound
from nselib.libutil import cleaning_column_name, cleaning_nse_symbol
from nselib.request_maker import nse_urlfetch
from .constants import FUTURE_PRICE_VOLUME_DATA_COLUMN, INDICES

logger = logging.getLogger(__name__)


def fetch_future_price_volume(
    symbol: str, instrument: str, from_date: str, to_date: str
) -> pd.DataFrame:
    """Fetch historical price and volume data for a single futures date range.

    Args:
        symbol: The ticker symbol (e.g., ``'SBIN'``, ``'NIFTY'``).
        instrument: ``'FUTIDX'`` for index futures, ``'FUTSTK'`` for stock futures.
        from_date: Start date in ``'DD-MM-YYYY'`` format.
        to_date: End date in ``'DD-MM-YYYY'`` format.

    Returns:
        A DataFrame with OHLCV and other transaction details.

    Raises:
        ValueError: If the NSE API rejects the parameters.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> df = api_fetchers.fetch_future_price_volume('SBIN', 'FUTSTK', '01-01-2025', '31-01-2025')
    """
    logger.debug(
        "Fetching future price volume: symbol=%s, instrument=%s, from=%s, to=%s",
        symbol, instrument, from_date, to_date,
    )
    origin_url = "https://www.nseindia.com/report-detail/fo_eq_security"
    url = (
        "https://www.nseindia.com/api/historicalOR/foCPV?"
        f"from={from_date}&to={to_date}&instrumentType={instrument}"
        f"&symbol={symbol}&csv=true"
    )
    try:
        data: dict = nse_urlfetch(url, origin_url=origin_url).json()
    except Exception as e:
        logger.error("Failed to fetch future price volume data: %s", e, exc_info=True)
        raise ValueError(f"Invalid parameters: NSE error: {e}")

    data_df = pd.DataFrame(data["data"])
    data_df.columns = cleaning_column_name(data_df.columns)
    logger.debug("Retrieved %d future price volume records.", len(data_df))
    return data_df


def fetch_option_price_volume(
    symbol: str, instrument: str, option_type: str, from_date: str, to_date: str
) -> pd.DataFrame:
    """Fetch historical price and volume data for a single options date range.

    Args:
        symbol: The ticker symbol (e.g., ``'RELIANCE'``, ``'BANKNIFTY'``).
        instrument: ``'OPTIDX'`` for index options, ``'OPTSTK'`` for stock options.
        option_type: ``'CE'`` for Call European, ``'PE'`` for Put European.
        from_date: Start date in ``'DD-MM-YYYY'`` format.
        to_date: End date in ``'DD-MM-YYYY'`` format.

    Returns:
        A DataFrame with OHLCV data for the requested option.

    Raises:
        ValueError: If the parameters yield no data or the NSE API errors.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> df = api_fetchers.fetch_option_price_volume('NIFTY', 'OPTIDX', 'CE', '01-01-2025', '31-01-2025')
    """
    logger.debug(
        "Fetching option price volume: symbol=%s, instrument=%s, type=%s, from=%s, to=%s",
        symbol, instrument, option_type, from_date, to_date,
    )
    origin_url = "https://www.nseindia.com/report-detail/fo_eq_security"
    url = (
        "https://www.nseindia.com/api/historicalOR/foCPV?"
        f"from={from_date}&to={to_date}&instrumentType={instrument}"
        f"&symbol={symbol}&optionType={option_type}&csv=true"
    )
    try:
        data_dict = nse_urlfetch(url, origin_url=origin_url).json()
    except Exception as e:
        logger.error("Failed to fetch option price volume data: %s", e, exc_info=True)
        raise ValueError(f"Invalid parameters: NSE error: {e}")

    data_df = pd.DataFrame(data_dict["data"])
    if data_df.empty:
        logger.warning("No option price volume data returned for symbol=%s", symbol)
        raise ValueError("Invalid parameters, please change the parameters")

    data_df.columns = cleaning_column_name(data_df.columns)
    logger.debug("Retrieved %d option price volume records.", len(data_df))
    return data_df[FUTURE_PRICE_VOLUME_DATA_COLUMN]


def fetch_option_chain(symbol: str, expiry_date: str):
    """Fetch the complete live option chain for a given symbol and expiry.

    Args:
        symbol: The underlying symbol (e.g., ``'TCS'``, ``'NIFTY'``).
            Automatically determines if it's an index or equity.
        expiry_date: Expiration date in ``'DD-MMM-YYYY'`` format (e.g., ``'25-Dec-2025'``).

    Returns:
        requests.Response: The HTTP response containing the option chain JSON.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> resp = api_fetchers.fetch_option_chain('TCS', '27-Mar-2026')
    """
    logger.debug("Fetching option chain: symbol=%s, expiry=%s", symbol, expiry_date)
    symbol = cleaning_nse_symbol(symbol)
    origin_url = "https://www.nseindia.com/option-chain"

    asset_type = "Indices" if any(idx in symbol for idx in INDICES) else "Equity"
    logger.debug("Symbol '%s' identified as %s.", symbol, asset_type)

    base = f"https://www.nseindia.com/api/option-chain-v3?type={asset_type}&symbol={symbol}"
    url = f"{base}&expiry={expiry_date}" if expiry_date else base
    response = nse_urlfetch(url, origin_url=origin_url)
    logger.debug("Successfully retrieved option chain data.")
    return response


def fetch_business_growth_fo(api_path: str) -> dict:
    """Fetch business growth data for the F&O segment from NSE.

    Args:
        api_path: The relative API path (e.g.,
            ``'/api/historicalOR/fo/tbg/yearly'``).

    Returns:
        The parsed JSON response from the NSE API.

    Raises:
        NSEdataNotFound: If the NSE API is unreachable or returns an error.
    """
    logger.debug("Fetching business growth F&O data from path: %s", api_path)
    origin_url = "https://www.nseindia.com/market-data/business-growth-fo-segment"
    url = f"https://www.nseindia.com{api_path}"
    try:
        data_json = nse_urlfetch(url, origin_url=origin_url).json()
    except Exception as e:
        logger.error("Failed to fetch business growth F&O data: %s", e, exc_info=True)
        raise NSEdataNotFound(f"Resource not available: {e}")
    logger.debug("Successfully retrieved business growth F&O segment data.")
    return data_json


def fetch_business_growth_fo_yearly() -> dict:
    """Fetch yearly business growth data for the NSE F&O segment.

    Returns:
        The JSON response containing yearly F&O business growth statistics.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> data = api_fetchers.fetch_business_growth_fo_yearly()
    """
    logger.debug("Fetching yearly business growth F&O data.")
    return fetch_business_growth_fo("/api/historicalOR/fo/tbg/yearly")


def fetch_business_growth_fo_monthly(from_year: str, to_year: str) -> dict:
    """Fetch monthly business growth data for a given financial year.

    Args:
        from_year: Starting year (e.g., ``'2025'``).
        to_year: Ending year (e.g., ``'2026'``).

    Returns:
        The JSON response containing monthly F&O business growth statistics.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> data = api_fetchers.fetch_business_growth_fo_monthly('2025', '2026')
    """
    logger.debug("Fetching monthly business growth F&O data for FY %s-%s.", from_year, to_year)
    return fetch_business_growth_fo(
        f"/api/historicalOR/fo/tbg/monthly?from={from_year}&to={to_year}"
    )


def fetch_business_growth_fo_daily(month: str, year: str) -> dict:
    """Fetch daily business growth data for a given month and year.

    Args:
        month: 3-letter abbreviated month name (e.g., ``'Mar'``).
        year: Full year (e.g., ``'2026'``).

    Returns:
        The JSON response containing daily F&O business growth statistics.

    Example:
        >>> from nselib.derivatives import api_fetchers
        >>> data = api_fetchers.fetch_business_growth_fo_daily('Mar', '2026')
    """
    logger.debug("Fetching daily business growth F&O data for %s %s.", month, year)
    return fetch_business_growth_fo(
        f"/api/historicalOR/fo/tbg/daily?month={month}&year={year}"
    )
