import logging
from io import BytesIO

import pandas as pd

from . import nse_config as conf
from nselib.libutil import NSEdataNotFound, nse_urlfetch

logger = logging.getLogger(__name__)


def get_class(index_category: str):
    category = f"Nifty{index_category}"
    category_class = getattr(conf, category, None)

    if category_class is None:
        raise ValueError(f"No such category in config: {category}")
    return category_class


def index_list(index_category: str = 'BroadMarketIndices'):
    """Get the available NSE indices for each category of indices.

    There are 4 categories defined by NSE:
    https://www.nseindia.com/static/products-services/about-indices

    Args:
        index_category: One of 'SectoralIndices', 'BroadMarketIndices',
            'ThematicIndices', 'StrategyIndices'.

    Returns:
        List of index names in the given category.
    """
    return get_class(index_category).indices_list


def validate_index_category(index_category: str = 'BroadMarketIndices'):
    category_list = ['SectoralIndices', 'BroadMarketIndices', 'ThematicIndices', 'StrategyIndices']
    if index_category in category_list:
        pass
    else:
        raise ValueError(f'{index_category} is not a valid index_category :: please select from: {category_list}')


def validate_index_name(index_category: str = 'BroadMarketIndices', index_name: str = 'Nifty 50'):
    validate_index_category(index_category)
    ind_list = index_list(index_category)
    if index_name in ind_list:
        pass
    else:
        raise ValueError(f'{index_name} is not a valid index_name :: please select from: {ind_list}')


def constituent_stock_list(index_category: str = 'BroadMarketIndices', index_name: str = 'Nifty 50'):
    """Get list of all constituent stocks for the given index.

    Args:
        index_category: One of 'SectoralIndices', 'BroadMarketIndices',
            'ThematicIndices', 'StrategyIndices'.
        index_name: Index name from the list provided by index_list().

    Returns:
        DataFrame containing constituent stock details.

    Raises:
        ValueError: If the index_category or index_name is invalid.
        FileNotFoundError: If data is not found.
    """
    validate_index_name(index_category, index_name)
    url = get_class(index_category).index_constituent_list_urls[index_name]
    if not url:
        raise FileNotFoundError(f"Data not found for index {index_name}")
    response = nse_urlfetch(url)
    if response.status_code == 200:
        stocks_df = pd.read_csv(BytesIO(response.content))
    else:
        raise FileNotFoundError("Data not found, check index_name or index_category")
    url_fs = get_class(index_category).index_factsheet_urls[index_name]
    logger.info("For more detail information related to %s, check the Fact Sheet: %s", index_name, url_fs)
    return stocks_df


def live_index_performances():
    """Get index performances in live market.

    After market hours, returns data as per last traded value.
    Link: https://www.nseindia.com/market-data/index-performances

    Returns:
        DataFrame containing performance data for all indices.

    Raises:
        NSEdataNotFound: If the NSE API is unavailable.
    """
    origin_url = "https://www.nseindia.com/market-data/index-performances"
    url = "https://www.nseindia.com/api/allIndices"
    try:
        data_json = nse_urlfetch(url, origin_url=origin_url).json()
        data_df = pd.DataFrame(data_json['data'])
    except Exception as e:
        raise NSEdataNotFound(f"Resource not available: {e}") from e
    data_df.drop(columns=['chartTodayPath', 'chart30dPath', 'chart365dPath'], inplace=True)
    return data_df
