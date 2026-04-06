import datetime as dt
from io import BytesIO

import pandas as pd

from nselib.libutil import nse_urlfetch


def securities_available_for_trading(trade_date):
    """Fetch securities available for trading in the debt segment.

    Args:
        trade_date: Trade date in 'dd-mm-YYYY' format (e.g., '17-03-2022').

    Returns:
        DataFrame containing available debt securities.

    Raises:
        FileNotFoundError: If no data is available for the given date.
    """
    month = dt.datetime.strptime(trade_date, '%d-%m-%Y').strftime('%b').upper()
    year = dt.datetime.strptime(trade_date, '%d-%m-%Y').strftime('%Y')
    date_str = dt.datetime.strptime(trade_date, '%d-%m-%Y').strftime('%d%m%Y')
    origin_url = "https://www.nseindia.com/all-reports-debt"
    url = f"https://nsearchives.nseindia.com/content/historical/WDM/{year}/{month}/wdmlist_{date_str}.csv"
    file_chk = nse_urlfetch(url, origin_url=origin_url)
    if file_chk.status_code != 200:
        raise FileNotFoundError("No data available")
    try:
        data_df = pd.read_csv(BytesIO(file_chk.content))
    except Exception as e:
        raise FileNotFoundError(f"Equity List not found :: NSE error: {e}") from e
    return data_df
