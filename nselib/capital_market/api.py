import datetime as dt
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from io import BytesIO
from typing import Callable, Optional

import numpy as np
import pandas as pd
import requests

from .constants import (
    BLOCK_DEALS_DATA_COLUMNS,
    BULK_DEAL_DATA_COLUMNS,
    DELIVERABLE_DATA_COLUMNS,
    INDIA_VIX_DATA_COLUMN,
    PRICE_VOLUME_AND_DELIVERABLE_POSITION_DATA_COLUMNS,
    PRICE_VOLUME_DATA_COLUMNS,
    SHORT_SELL_DATA_COLUMNS,
    VAR_COLUMNS,
)
from .network import CapitalMarketHelper
from ..libutil import (
    cleaning_nse_symbol,
    derive_from_and_to_date,
    validate_date_param,
    validate_param_from_list,
)
from ..utils.enums import DateFormatEnum

cm_helper = CapitalMarketHelper()
logger = logging.getLogger(__name__)


def _paginated_fetch(
    from_date: str,
    to_date: str,
    fetch_fn: Callable[..., pd.DataFrame],
    columns: list,
    chunk_days: int = 365,
    **fetch_kwargs,
) -> pd.DataFrame:
    fmt = DateFormatEnum.DD_MM_YYYY.value
    start = datetime.strptime(from_date, fmt)
    end = datetime.strptime(to_date, fmt)
    frames = []

    while start <= end:
        chunk_end = min(start + dt.timedelta(days=chunk_days - 1), end)
        df = fetch_fn(
            from_date=start.strftime(fmt),
            to_date=chunk_end.strftime(fmt),
            **fetch_kwargs,
        )
        if not df.empty:
            frames.append(df)
        start = chunk_end + dt.timedelta(days=1)

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame(columns=columns)


def price_volume_and_deliverable_position_data(
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """
    get Security wise price volume & Deliverable position data set. use get_nse_symbols() to get all symbols

    :param symbol: symbol eg: 'SBIN'
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date}
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.price_volume_and_deliverable_position_data(symbol='SBIN', period='1M')
    """
    validate_date_param(from_date, to_date, period)
    symbol = cleaning_nse_symbol(symbol=symbol)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )

    nse_df = _paginated_fetch(
        from_date,
        to_date,
        cm_helper.get_price_volume_and_deliverable_position_data,
        PRICE_VOLUME_AND_DELIVERABLE_POSITION_DATA_COLUMNS,
        symbol=symbol,
    )

    if not nse_df.empty:
        nse_df = nse_df.fillna("-").dropna(axis=1, how="all")
        nse_df["TotalTradedQuantity"] = pd.to_numeric(
            nse_df["TotalTradedQuantity"].astype(str).str.replace(",", ""),
            errors="coerce",
        )
        nse_df["TurnoverInRs"] = pd.to_numeric(
            nse_df["TurnoverInRs"].astype(str).str.replace(",", ""), errors="coerce"
        )
        nse_df["No.ofTrades"] = pd.to_numeric(
            nse_df["No.ofTrades"].astype(str).str.replace(",", ""), errors="coerce"
        )
        nse_df["DeliverableQty"] = pd.to_numeric(
            nse_df["DeliverableQty"].astype(str).str.replace(",", ""), errors="coerce"
        )
    return nse_df


def price_volume_data(
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get Security wise price volume data set.

    :param symbol: symbol eg: 'SBIN'
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date}
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.price_volume_data('SBIN', '17-03-2022', '17-06-2023', period='1M')
    """
    logger.debug("Fetching data for price_volume_data")
    validate_date_param(from_date, to_date, period)
    symbol = cleaning_nse_symbol(symbol=symbol)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date,
        to_date,
        cm_helper.get_price_volume_data,
        PRICE_VOLUME_DATA_COLUMNS,
        symbol=symbol,
    )


def deliverable_position_data(
    symbol: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get Security wise deliverable position data set.

    :param symbol: symbol eg: 'SBIN'
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.deliverable_position_data('SBIN', '17-03-2022', '17-06-2023', period='1M')
    """
    logger.debug("Fetching data for deliverable_position_data")
    validate_date_param(from_date, to_date, period)
    symbol = cleaning_nse_symbol(symbol=symbol)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date,
        to_date,
        cm_helper.get_deliverable_position_data,
        DELIVERABLE_DATA_COLUMNS,
        symbol=symbol,
    )


def india_vix_data(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get india vix spot data  set for the specific time period.

    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.india_vix_data('17-03-2022', '17-06-2023', period='1M')
    """

    logger.debug("Fetching data for india_vix_data")
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date, to_date, cm_helper.get_india_vix_data, INDIA_VIX_DATA_COLUMN
    )


def index_data(
    index: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get historical index data set for the specific time period.

    apply the index name as per the nse india site

    :param index: 'NIFTY 50'/'NIFTY BANK'
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date}
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.index_data( 'NIFTY 50', '17-03-2022', '17-06-2023', '1M')
    """
    logger.debug("Fetching data for index_data")
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date, to_date, cm_helper.get_index_data, [], index=index
    )


def bulk_deal_data(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get bulk deal data set.

    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.bulk_deal_data('17-03-2022', '17-06-2023', period='1M')
    """
    logger.debug("Fetching data for bulk_deal_data")
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date, to_date, cm_helper.get_bulk_deal_data, BULK_DEAL_DATA_COLUMNS
    )


def block_deals_data(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get block deals data set.

    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date}
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.block_deals_data('17-03-2022', '17-06-2023', period='1M')
    """
    logger.debug("Fetching data for block_deals_data")
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date, to_date, cm_helper.get_block_deals_data, BLOCK_DEALS_DATA_COLUMNS
    )


def short_selling_data(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
):
    """Get short selling data set.

    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date}
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.short_selling_data('17-03-2022', '17-06-2023', period='1M')
    """
    logger.debug("Fetching data for short_selling_data")
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )
    return _paginated_fetch(
        from_date, to_date, cm_helper.get_short_selling_data, SHORT_SELL_DATA_COLUMNS
    )


def bhav_copy_with_delivery(trade_date: str):
    """Get the NSE bhav copy with delivery data as per the traded date.

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.bhav_copy_with_delivery('17-03-2022')
    """
    logger.debug("Fetching data for bhav_copy_with_delivery")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    bhav_df = cm_helper.get_bhav_copy_with_delivery(use_date)
    bhav_df.columns = [name.replace(" ", "") for name in bhav_df.columns]
    bhav_df["SERIES"] = bhav_df["SERIES"].str.replace(" ", "")
    bhav_df["DATE1"] = bhav_df["DATE1"].str.replace(" ", "")
    return bhav_df


def bhav_copy_equities(trade_date: str):
    """Get new CM-UDiFF Common Bhavcopy Final as per the traded date provided.

    :param trade_date:
    :return: pandas dataframe

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.bhav_copy_equities('17-03-2022')
    """
    logger.debug("Fetching data for bhav_copy_equities")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    return cm_helper.get_bhav_copy_equities(
        trade_date.strftime("%Y%m%d"),
        trade_date.strftime(DateFormatEnum.DD_MM_YYYY.value),
    )


def bhav_copy_indices(trade_date: str):
    """Get nse bhav copy as per the traded date provided.

    :param trade_date: eg:'20-06-2023'
    :return: pandas dataframe

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.bhav_copy_indices('17-03-2022')
    """
    logger.debug("Fetching data for bhav_copy_indices")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    return cm_helper.get_bhav_copy_indices(trade_date.strftime("%d%m%Y"))


def bhav_copy_sme(trade_date: str):
    """Get the NSE bhav copy for SME data as per the traded date.

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.bhav_copy_sme('17-03-2022')
    """
    logger.debug("Fetching data for bhav_copy_sme")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    bhav_df = cm_helper.get_bhav_copy_sme(
        trade_date.strftime(DateFormatEnum.DDMMYY.value)
    )
    bhav_df.columns = [name.replace(" ", "") for name in bhav_df.columns]
    return bhav_df


def equity_list():
    """Get list of all equity available to trade in NSE.

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.equity_list()
    """
    logger.debug("Fetching data for equity_list")
    data_df = cm_helper.get_equity_list()
    return data_df[
        ["SYMBOL", "NAME OF COMPANY", " SERIES", " DATE OF LISTING", " FACE VALUE"]
    ]


def fno_equity_list():
    """Get a dataframe of all listed derivative equity list with the recent lot size to trade

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.fno_equity_list()
    """
    logger.debug("Fetching data for fno_equity_list")
    data_dict = cm_helper.get_fno_equity_list()
    return pd.DataFrame(data_dict["data"]["UnderlyingList"])


def fno_index_list():
    """Get a dataframe of all listed derivative index list with the recent lot size to trade

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.fno_index_list()
    """
    logger.debug("Fetching data for fno_index_list")
    data_dict = cm_helper.get_fno_index_list()
    return pd.DataFrame(data_dict["data"]["IndexList"])


def nifty50_equity_list():
    """List of all equities under NIFTY 50 index

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.nifty50_equity_list()
    """
    logger.debug("Fetching data for nifty50_equity_list")
    url = "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv"
    data_df = cm_helper.get_nifty_equity_list(url)
    return data_df[["Company Name", "Industry", "Symbol"]]


def niftynext50_equity_list():
    """List of all equities under NIFTY NEXT 50 index

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.niftynext50_equity_list()
    """
    logger.debug("Fetching data for niftynext50_equity_list")
    url = "https://archives.nseindia.com/content/indices/ind_niftynext50list.csv"
    data_df = cm_helper.get_nifty_equity_list(url)
    return data_df[["Company Name", "Industry", "Symbol"]]


def niftymidcap150_equity_list():
    """List of all equities under NIFTY MIDCAP 150 index

    :return: pandas data frame

    Example:
    >>> from nselib import capital_marke    t
    >>> df = capital_market.niftymidcap150_equity_list()
    """
    logger.debug("Fetching data for niftymidcap150_equity_list")
    url = "https://archives.nseindia.com/content/indices/ind_niftymidcap150list.csv"
    data_df = cm_helper.get_nifty_equity_list(url)
    return data_df[["Company Name", "Industry", "Symbol"]]


def niftysmallcap250_equity_list():
    """List of all equities under NIFTY SMALLCAP 250 index

    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.niftysmallcap250_equity_list()
    """
    logger.debug("Fetching data for niftysmallcap250_equity_list")
    url = "https://archives.nseindia.com/content/indices/ind_niftysmallcap250list.csv"
    data_df = cm_helper.get_nifty_equity_list(url)
    return data_df[["Company Name", "Industry", "Symbol"]]


def market_watch_all_indices():
    """Market Watch - Indices of the day in data frame

    :return: pd.DataFrame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.market_watch_all_indices()
    """
    logger.debug("Fetching data for market_watch_all_indices")
    data_json = cm_helper.get_market_watch_all_indices()
    data_df = pd.DataFrame(data_json["data"])
    return data_df[
        [
            "key",
            "index",
            "indexSymbol",
            "last",
            "variation",
            "percentChange",
            "open",
            "high",
            "low",
            "previousClose",
            "yearHigh",
            "yearLow",
            "pe",
            "pb",
            "dy",
            "declines",
            "advances",
            "unchanged",
            "perChange365d",
            "perChange30d",
            "previousDay",
            "oneWeekAgoVal",
            "oneMonthAgoVal",
            "oneYearAgoVal",
        ]
    ]


def fii_dii_trading_activity():
    """FII and DII trading activity of the day in data frame

    :return: pd.DataFrame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.fii_dii_trading_activity()
    """
    logger.debug("Fetching data for fii_dii_trading_activity")
    data_json = cm_helper.get_fii_dii_trading_activity()
    return pd.DataFrame(data_json)


def daily_volatility(trade_date: str):
    """Get CM daily volatility report as per the traded date provided

    :param trade_date: eg:'20-06-2023'
    :return: pandas dataframe

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.daily_volatility('17-03-2022')
    """
    logger.debug("Fetching data for daily_volatility")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    payload = f"CMVOLT_{trade_date.strftime('%d%m%Y')}.CSV"
    data_df = cm_helper.get_daily_volatility(payload, trade_date.strftime("%d-%b-%Y"))
    data_df = data_df.dropna(how="all")
    data_df.columns = [column_name.strip() for column_name in data_df.columns]
    return data_df


def category_turnover_cash(trade_date: str):
    """Get NSE cash market category-wise turnover data as per the traded date provided

    :param trade_date: eg:'07-04-2026'
    :return: pandas dataframe

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.category_turnover_cash('17-03-2022')
    """
    logger.debug("Fetching data for category_turnover_cash")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYY.value)

    content = cm_helper.get_category_turnover_cash(use_date)
    raw = pd.read_excel(BytesIO(content), header=None, engine="xlrd")
    blocks = []
    row_count = len(raw.index)
    row_number = 0
    while row_number < row_count:
        first = str(raw.iloc[row_number, 0]).strip() if raw.shape[1] > 0 else ""
        second = str(raw.iloc[row_number, 1]).strip() if raw.shape[1] > 1 else ""
        if first.lower() == "trade date" and second.lower() in {
            "category",
            "client categories",
        }:
            headers = [
                str(value).strip() for value in raw.iloc[row_number, :4].tolist()
            ]
            start_row = row_number + 1
            end_row = start_row
            while end_row < row_count:
                first_value = raw.iloc[end_row, 0] if raw.shape[1] > 0 else None
                second_value = raw.iloc[end_row, 1] if raw.shape[1] > 1 else None
                first_text = str(first_value).strip() if first_value is not None else ""
                second_text = (
                    str(second_value).strip() if second_value is not None else ""
                )
                if (
                    first_value is None
                    or (isinstance(first_value, float) and pd.isna(first_value))
                    or first_text.lower().startswith("note")
                    or first_text.lower().startswith("notes")
                    or first_text.lower() == "trade date"
                    or second_text.lower() in {"category", "client categories"}
                ):
                    break
                end_row += 1
            block = raw.iloc[start_row:end_row, :4].copy()
            block.columns = headers
            blocks.append(block)
            row_number = end_row
            continue
        row_number += 1

    if not blocks:
        raise FileNotFoundError(
            f" Category turnover cash data not found for : {trade_date}"
        )

    data_df = pd.concat(blocks, ignore_index=True)
    data_df = data_df.rename(
        columns={
            "Trade Date": "Trade Date",
            "Category": "Category",
            "Client Categories": "Category",
            "Buy Value in Rs.": "Buy Value in Rs.Crores",
            "Buy Value in Rs.Crores": "Buy Value in Rs.Crores",
            "Sell Value in Rs.": "Sell Value in Rs.Crores",
            "Sell Value in Rs.Crores": "Sell Value in Rs.Crores",
        }
    )
    data_df = data_df[
        [
            column
            for column in [
                "Trade Date",
                "Category",
                "Buy Value in Rs.Crores",
                "Sell Value in Rs.Crores",
            ]
            if column in data_df.columns
        ]
    ].copy()
    data_df["Trade Date"] = pd.to_datetime(data_df["Trade Date"], errors="coerce")
    data_df["Category"] = data_df["Category"].astype(str).str.strip()
    data_df = data_df[data_df["Category"].ne("")].copy()
    for column_name in ["Buy Value in Rs.Crores", "Sell Value in Rs.Crores"]:
        data_df[column_name] = pd.to_numeric(data_df[column_name], errors="coerce")
    data_df = data_df.dropna(subset=["Trade Date", "Category"]).reset_index(drop=True)
    data_df["Net Value in Rs.Crores"] = (
        data_df["Buy Value in Rs.Crores"] - data_df["Sell Value in Rs.Crores"]
    )
    data_df["Trade Date"] = data_df["Trade Date"].dt.strftime("%d-%b-%Y")
    return data_df


def var_begin_day(trade_date: str):
    """Get the VaR Begin Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.var_begin_day('17-03-2022')
    """
    logger.debug("Fetching data for var_begin_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_1.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def var_1st_intra_day(trade_date: str):
    """Get the VaR 1st Intra Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.var_1st_intra_day('17-03-2022')
    """
    logger.debug("Fetching data for var_1st_intra_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_2.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def var_2nd_intra_day(trade_date: str):
    """Get the VaR 2nd Intra Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.var_2nd_intra_day('17-03-2022')
    """
    logger.debug("Fetching data for var_2nd_intra_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_3.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def var_3rd_intra_day(trade_date: str):
    """Get the VaR 3rd Intra Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.var_3rd_intra_day('17-03-2022')
    """
    logger.debug("Fetching data for var_3rd_intra_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_4.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def var_4th_intra_day(trade_date: str):
    """Get the VaR 4th Intra Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.var_4th_intra_day('17-03-2022')
    """
    logger.debug("Fetching data for var_4th_intra_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_5.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def var_end_of_day(trade_date: str):
    """Get the VaR End of Day data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
        >>> df = capital_market.var_end_of_day('17-03-2022')
    """
    logger.debug("Fetching data for var_end_of_day")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    url = f"https://nsearchives.nseindia.com/archives/nsccl/var/C_VAR1_{use_date}_6.DAT"
    var_df = cm_helper.get_var_data(url)
    var_df.columns = VAR_COLUMNS
    return var_df


def sme_bhav_copy(trade_date: str):
    """Get the SME bhav copy data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.sme_bhav_copy('17-03-2022')
    """
    logger.debug("Fetching data for sme_bhav_copy")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYY.value)
    bhav_df = cm_helper.get_bhav_copy_sme(use_date)
    bhav_df.columns = [name.replace(" ", "") for name in bhav_df.columns]
    return bhav_df


def sme_band_complete(trade_date: str):
    """Get the SME Band Complete data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
        >>> df = capital_market.sme_band_complete('17-03-2022')
    """
    logger.debug("Fetching data for sme_band_complete")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    sme_df = cm_helper.get_sme_band_complete(use_date)
    sme_df.columns = [name.replace(" ", "") for name in sme_df.columns]
    return sme_df


def week_52_high_low_report(trade_date: str):
    """Get the 52-Week High Low Report data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.week_52_high_low_report('17-03-2022')
    """
    logger.debug("Fetching data for week_52_high_low_report")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYYYY.value)
    high_low_df = cm_helper.get_week_52_high_low_report(use_date)
    high_low_df.columns = [name.replace(" ", "") for name in high_low_df.columns]
    return high_low_df


def financial_results_for_equity(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    fo_sec: bool = False,
    fin_period: str = "Quarterly",
):
    """Get audited and un-auditable financial results for equities. as per
     https://www.nseindia.com/companies-listing/corporate-filings-financial-results

    :param fin_period: Quaterly/ Half-Yearly/ Annual/ Others
    :param fo_sec: True/False
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper
    """
    master_data_df, headers, ns, keys_to_extract = (
        cm_helper.get_financial_results_master(
            from_date, to_date, period, fo_sec, fin_period
        )
    )
    fin_df, df = pd.DataFrame(), pd.DataFrame()
    for row in master_data_df.itertuples(index=False):
        try:
            response_ = requests.get(row.xbrl, headers=headers)
            response_.raise_for_status()

            root = ET.fromstring(response_.content)

            extracted_data = {}
            for key in keys_to_extract:
                elem_ = root.find(f".//in-bse-fin:{key}", ns)
                extracted_data[key] = elem_.text if elem_ is not None else None

            df = pd.DataFrame([extracted_data])
        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"Request failed: {e}") from e
        except ET.ParseError as e:
            raise ET.ParseError(f"XML parsing failed: {e}") from e
        except Exception as e:
            raise RuntimeError(f"An error occurred: {e}") from e

        if fin_df.empty:
            fin_df = df
        else:
            fin_df = pd.concat([fin_df, df], ignore_index=True)
    return fin_df


def corporate_bond_trade_report(trade_date: str):
    """Get the NSE corporate bond trade report as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.corporate_bond_trade_report('17-03-2022')
    """
    logger.debug("Fetching data for corporate_bond_trade_report")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYY.value)
    bond_df = cm_helper.get_corporate_bond_trade_report(use_date)
    bond_df.columns = [name.replace(" ", "") for name in bond_df.columns]
    bond_df["SERIES"] = bond_df["SERIES"].str.replace(" ", "")
    return bond_df


def pe_ratio(trade_date: str):
    """Get the NSE pe ratio for all NSE equities data as per the traded date

    :param trade_date: eg:'20-06-2023'
    :return: pandas data frame

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.pe_ratio('17-03-2022')
    """
    logger.debug("Fetching data for pe_ratio")
    trade_date = datetime.strptime(trade_date, DateFormatEnum.DD_MM_YYYY.value)
    use_date = trade_date.strftime(DateFormatEnum.DDMMYY.value)
    pe_df = cm_helper.get_pe_ratio(use_date)
    pe_df.columns = [name.replace(" ", "") for name in pe_df.columns]
    return pe_df


def corporate_actions_for_equity(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    fno_only: bool = False,
):
    """Get corporate actions for equities as per
        https://www.nseindia.com/companies-listing/corporate-filings-actions

    :param fno_only: True/False
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper
    """
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )

    if fno_only:
        payload = f"from_date={from_date}&to_date={to_date}&fo_sec=true"
    else:
        payload = f"from_date={from_date}&to_date={to_date}"

    data_list = cm_helper.get_corporate_actions_for_equity(payload)
    master_data_df = pd.DataFrame(data_list)
    master_data_df.columns = [name.replace(" ", "") for name in master_data_df.columns]
    return master_data_df


def event_calendar_for_equity(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    period: Optional[str] = None,
    fno_only: bool = False,
):
    """Get event calendar for equities as per

    https://www.nseindia.com/companies-listing/corporate-filings-event-calendar

    :param fno_only: True/False
    :param from_date: '17-03-2022' ('dd-mm-YYYY')
    :param to_date: '17-06-2023' ('dd-mm-YYYY')
    :param period: use one {'1D': last day data,
                            '1W': for last 7 days data,
                            '1M': from last month same date,
                            '6M': last 6-month data,
                            '1Y': from last year same date)
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper
    """
    validate_date_param(from_date, to_date, period)
    from_date, to_date = derive_from_and_to_date(
        from_date=from_date, to_date=to_date, period=period
    )

    if fno_only:
        payload = f"from_date={from_date}&to_date={to_date}&fo_sec=true"
    else:
        payload = f"from_date={from_date}&to_date={to_date}"

    data_list = cm_helper.get_event_calendar_for_equity(payload)
    master_data_df = pd.DataFrame(data_list)
    master_data_df.columns = [name.replace(" ", "") for name in master_data_df.columns]
    return master_data_df


def top_gainers_or_losers(to_get: str = "gainers"):
    """Get top gainers or losers on live market, after market hour it will get as per last traded value

    :param to_get: gainers/loosers
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.top_gainers_or_losers()
    """
    logger.debug("Fetching data for top_gainers_or_losers")
    static_options_list = ["gainers", "loosers"]
    validate_param_from_list(to_get, static_options_list)
    data_json = cm_helper.get_top_gainers_or_losers(to_get)
    legends_dict = {item[0]: item[1] for item in data_json["legends"]}
    gainers_losers_df = pd.DataFrame()
    for i in legends_dict.keys():
        data_df = pd.DataFrame(data_json[i]["data"])
        data_df["legend"] = i
        if gainers_losers_df.empty:
            gainers_losers_df = data_df
        else:
            gainers_losers_df = pd.concat([gainers_losers_df, data_df])
    return gainers_losers_df


def most_active_equities(fetch_by: str = "value"):
    """To get most active equities fetched by value/volume in live market,
    after market hour it will get as per last traded value

    link : https://www.nseindia.com/market-data/most-active-equities

    :param fetch_by: select any volume/value
    :return: pandas dataframe
    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.most_active_equities()
    """
    logger.debug("Fetching data for most_active_equities")
    static_options_list = ["volume", "value"]
    validate_param_from_list(fetch_by, static_options_list)
    data_json = cm_helper.get_most_active_equities(fetch_by)
    return pd.DataFrame(data_json["data"])


def total_traded_stocks():
    """To get all total traded stocks detail in live market, after market hour it will get as per last traded value

    summary_dict - has the live market summary for the stocks in dictionary format
    detail_df - has all detail available in the current market in dataframe format
    link : https://www.nseindia.com/market-data/stocks-traded

    :return: summary_dict, detail_df

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.total_traded_stocks()
    """
    logger.debug("Fetching data for total_traded_stocks")
    data_json = cm_helper.get_total_traded_stocks()
    summary_dict = data_json["total"]["count"]
    detail_df = pd.DataFrame(data_json["total"]["data"])
    return summary_dict, detail_df


def _normalize_business_growth_cm_segment_financial_year(from_year, to_year):
    """Normalize financial year for business growth data.

    :param from_year: Start year
    :param to_year: End year
    :return: Tuple of normalized years

    Example:
    >>> from nselib import capital_market
    >>> from_year, to_year = capital_market._normalize_business_growth_cm_segment_financial_year('2025', '2026')
    """
    if to_year is None and from_year is not None and "-" in str(from_year):
        from_year, to_year = [part.strip() for part in str(from_year).split("-", 1)]
    if from_year is None or to_year is None:
        raise ValueError(
            " For monthly data provide from_year and to_year, e.g. from_year='2025', to_year='2026'"
        )
    return str(from_year).strip(), str(to_year).strip()


def _normalize_business_growth_cm_segment_daily_args(month, year):
    """Normalize daily arguments for business growth data.

    :param month: Month
    :param year: Year
    :return: Tuple of normalized month and year

    Example:
    >>> from nselib import capital_market
    >>> month, year = capital_market._normalize_business_growth_cm_segment_daily_args('Mar', '2026')
    """
    if year is None and month is not None and "-" in str(month):
        month, year = [part.strip() for part in str(month).split("-", 1)]
    if month is None or year is None:
        raise ValueError(
            "For daily data provide month and year, e.g. month='Mar', year='26'"
        )

    month = str(month).strip()
    try:
        month = datetime.strptime(month[:3].title(), "%b").strftime("%b")
    except ValueError as exc:
        raise ValueError(
            "Month should be a valid month name like 'Mar' or 'March'"
        ) from exc

    year = str(year).strip()
    if len(year) == 4 and year.isdigit():
        year = year[-2:]
    return month, year


def _business_growth_cm_segment_dataframe(data_json):
    """Normalize business growth data dataframe.

    :param data_json: Business growth data in json format
    :return: pandas.DataFrame

    Example:
    >>> from nselib import capital_market
    >>> data_json = capital_market._business_growth_cm_segment_dataframe(
    ...     {'data': [{'SYMBOL': 'TCS', 'COMPANYNAME': 'TATA CONSULTANCY SERVICES LTD',
    ...                 'DATE': '17-03-2022', 'OPEN': '3924.95', 'HIGH': '3924.95',
    ...                 'LOW': '3866.0', 'CLOSE': '3877.4', 'TOTTRDQTY': '1477663',
    ...                 'TRDVALUE': '5741581939', 'NOOFTRADES': '143816'}]
    ... })
    """
    records = [row.get("data", row) for row in data_json.get("data", [])]
    data_df = pd.DataFrame(records)
    if data_df.empty:
        return data_df

    data_df = data_df.replace({r"\\r": ""}, regex=True)
    for order_column in ["GTM_MONTH_YEAR_ORDER", "CDT_DATE_ORDER"]:
        if order_column in data_df.columns:
            data_df[order_column] = pd.to_datetime(
                data_df[order_column], errors="coerce"
            )

    preserve_columns = {
        "type",
        "TYPE",
        "GLY_MONTH_YEAR",
        "GLM_MONTH_YEAR",
        "F_TIMESTAMP",
    }
    null_map = {
        "": np.nan,
        "-": np.nan,
        "--": np.nan,
        "None": np.nan,
        "nan": np.nan,
        "NaN": np.nan,
    }

    for column_name in data_df.columns:
        if column_name in preserve_columns or column_name.endswith("_ORDER"):
            continue
        if pd.api.types.is_datetime64_any_dtype(data_df[column_name]):
            continue
        cleaned_series = (
            data_df[column_name]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.strip()
        )
        normalized_series = cleaned_series.replace(null_map)
        numeric_series = pd.to_numeric(normalized_series, errors="coerce")
        if numeric_series.notna().sum() == normalized_series.notna().sum():
            data_df[column_name] = numeric_series
        else:
            data_df[column_name] = normalized_series

    meta_keys = [key for key in data_json.keys() if key != "data"]
    if meta_keys:
        data_df.attrs["metadata"] = {key: data_json[key] for key in meta_keys}
    return data_df


def business_growth_cm_segment(
    data_type: str = "yearly",
    from_year: Optional[str] = None,
    to_year: Optional[str] = None,
    month: Optional[str] = None,
    year: Optional[str] = None,
):
    """Get historical business growth data for the NSE capital market (CM) segment.

    :param data_type: use one {'yearly', 'monthly', 'daily'}
    :param from_year: required for monthly data. eg: '2025' for FY 2025-2026
    :param to_year: required for monthly data. eg: '2026' for FY 2025-2026
    :param month: required for daily data. eg: 'Mar', 'March' or 'Mar-26'
    :param year: required for daily data. eg: '26' or '2026'
    :return: pandas.DataFrame
    :raise ValueError if the parameter input is not proper

    Example:
    >>> from nselib import capital_market
    >>> df = capital_market.business_growth_cm_segment()
    """
    logger.debug("Fetching data for business_growth_cm_segment")
    static_options_list = ["yearly", "monthly", "daily"]
    validate_param_from_list(data_type, static_options_list)

    if data_type == "yearly":
        data_json = cm_helper.get_business_growth_cm_segment_yearly()
    elif data_type == "monthly":
        from_year, to_year = _normalize_business_growth_cm_segment_financial_year(
            from_year, to_year
        )
        data_json = cm_helper.get_business_growth_cm_segment_monthly(
            from_year=from_year, to_year=to_year
        )
    else:
        month, year = _normalize_business_growth_cm_segment_daily_args(month, year)
        data_json = cm_helper.get_business_growth_cm_segment_daily(
            month=month, year=year
        )

    return _business_growth_cm_segment_dataframe(data_json)
