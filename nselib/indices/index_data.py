import json
import logging
from io import BytesIO
from pathlib import Path

import pandas as pd

from nselib.errors import (
    IndexDataNotFound,
    InvalidIndexCategoryError,
    InvalidIndexError,
    NSEApiError,
)
from nselib.request_maker import nse_urlfetch

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent / "nse_indices.json"


def _load_config() -> dict:
    """Load the index configuration from the JSON file.

    Returns:
        dict: Nested dictionary keyed by category → index name → URLs.
    """
    with open(_CONFIG_PATH, encoding="utf-8") as f:
        return json.load(f)


_CONFIG: dict = _load_config()

# Valid categories derived from the JSON keys — no hardcoding
VALID_CATEGORIES: list = list(_CONFIG.keys())


def _validate(index_category: str, index_name: str) -> dict:
    """Validate category and index name, returning the index config entry.

    Performs a single, unified validation of both the category and index name
    against the JSON configuration. This replaces the old ``validate_index_category``
    and ``validate_index_name`` functions.

    Args:
        index_category (str): The index category (e.g., ``'BroadMarketIndices'``).
        index_name (str): The specific index name (e.g., ``'Nifty 50'``).

    Returns:
        dict: The index entry containing ``'constituent_url'`` and ``'factsheet_url'``.

    Raises:
        InvalidIndexCategoryError: If the category is not in the configuration.
        InvalidIndexError: If the index name is not found under the category.
    """
    if index_category not in _CONFIG:
        logger.warning("Valid categories: %s", VALID_CATEGORIES)
        raise InvalidIndexCategoryError(
            f"'{index_category}' is an invalid index category. "
            f"Valid: {VALID_CATEGORIES}"
        )

    category_data = _CONFIG[index_category]
    if index_name not in category_data:
        valid_names = list(category_data.keys())
        logger.warning(
            "Valid index names for '%s': %s", index_category, valid_names
        )
        raise InvalidIndexError(
            f"'{index_name}' is not a valid index in '{index_category}'."
        )

    return category_data[index_name]


def index_list(index_category: str = "BroadMarketIndices") -> list:
    """Fetch the list of available NSE indices for a specific index category.

    The National Stock Exchange (NSE) classifies indices into four main categories:
    - SectoralIndices
    - BroadMarketIndices
    - ThematicIndices
    - StrategyIndices

    Reference: https://www.nseindia.com/static/products-services/about-indices

    Args:
        index_category (str): The category of indices to fetch.
            Defaults to ``'BroadMarketIndices'``.

    Returns:
        list: A list of index names belonging to the specified category.

    Raises:
        InvalidIndexCategoryError: If the category is not valid.

    Example:
        >>> from nselib import indices
        >>> list_of_indices = indices.index_list('BroadMarketIndices')
    """
    if index_category not in _CONFIG:
        logger.warning("Valid categories: %s", VALID_CATEGORIES)
        raise InvalidIndexCategoryError(
            f"'{index_category}' is an invalid index category. "
            f"Valid: {VALID_CATEGORIES}"
        )
    return list(_CONFIG[index_category].keys())


def constituent_stock_list(
    index_category: str = "BroadMarketIndices",
    index_name: str = "Nifty 50",
) -> pd.DataFrame:
    """Retrieve the list of constituent stocks for a specific NSE index.

    Fetches the current composition of the requested index as a pandas DataFrame.

    Args:
        index_category (str): The category of the index.
            Defaults to ``'BroadMarketIndices'``.
        index_name (str): The specific name of the index.
            Defaults to ``'Nifty 50'``.

    Returns:
        pandas.DataFrame: A DataFrame containing the constituent stocks
        and their details.

    Raises:
        InvalidIndexCategoryError: If the index category is invalid.
        InvalidIndexError: If the index name is invalid.
        IndexDataNotFound: If the data for the index is unavailable.

    Example:
        >>> from nselib import indices
        >>> df = indices.constituent_stock_list('BroadMarketIndices', 'Nifty 50')
    """
    logger.debug(
        "Fetching constituents: %s / %s", index_category, index_name
    )
    entry = _validate(index_category, index_name)

    url = entry.get("constituent_url")
    if not url:
        raise IndexDataNotFound(
            f"No constituent data URL available for '{index_name}'"
        )

    response = nse_urlfetch(url)
    if response.status_code != 200:
        raise IndexDataNotFound(
            f"Failed to fetch data for '{index_name}' "
            f"(HTTP {response.status_code})"
        )

    stocks_df = pd.read_csv(BytesIO(response.content))

    factsheet_url = entry.get("factsheet_url")
    if factsheet_url:
        logger.info("Factsheet for %s: %s", index_name, factsheet_url)

    return stocks_df


def live_index_performances() -> pd.DataFrame:
    """Fetch the live or last traded performance data for all NSE indices.

    During market hours, this returns live data. After market hours, it returns
    the final index performance data for the day.

    Data Source: https://www.nseindia.com/market-data/index-performances

    Returns:
        pandas.DataFrame: A DataFrame containing performance metrics
        for all indices.

    Raises:
        NSEApiError: If the NSE API is unreachable or returns an error.

    Example:
        >>> from nselib import indices
        >>> df = indices.live_index_performances()
    """
    logger.debug("Fetching live index performances")
    origin_url = "https://www.nseindia.com/market-data/index-performances"
    url = "https://www.nseindia.com/api/allIndices"
    try:
        data_json = nse_urlfetch(url, origin_url=origin_url).json()
        data_df = pd.DataFrame(data_json["data"])
    except Exception as e:
        logger.error("NSE API error: %s", e)
        raise NSEApiError(f"Resource not available: {e}")

    data_df.drop(
        columns=["chartTodayPath", "chart30dPath", "chart365dPath"],
        inplace=True,
    )
    return data_df
