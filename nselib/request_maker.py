
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)


default_header = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

header = {
    "referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Sec-Fetch-User": "?1",
    "Accept": "ext/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Accept-Language": "en-US,en;q=0.9,hi;q=0.8",
}

class NSEFetcher:
    _session = None

    @classmethod
    def get_session(cls) -> requests.Session:
        if cls._session is None:
            logger.debug("Initializing global NSE requests Session with retries.")
            cls._session = requests.Session()
            cls._session.trust_env = False
            
            # Setup retry strategy for transient errors
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "OPTIONS"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            cls._session.mount("https://", adapter)
            cls._session.mount("http://", adapter)
        return cls._session

    @classmethod
    def fetch(cls, url: str, origin_url: str = "https://www.nseindia.com", timeout: int = 10) -> requests.Response:
        """
        Fetch data from an NSE URL using a persistent session.

        Args:
            url (str): The target NSE API URL.
            origin_url (str, optional): The origin URL to fetch cookies from initially. Defaults to "https://www.nseindia.com".
            timeout (int, optional): The request timeout in seconds. Defaults to 10.

        Returns:
            requests.Response: The HTTP response object.
        """
        session = cls.get_session()

        if origin_url:
            logger.debug(f"Fetching cookies from origin_url: {origin_url}")
            session.get(origin_url, headers=default_header, timeout=timeout)

        logger.debug(f"Fetching data from url: {url}")
        return session.get(url, headers=header, timeout=timeout)


def nse_urlfetch(url: str, origin_url: str = "https://www.nseindia.com", timeout: int = 10) -> requests.Response:
    """
    Fetch data from an NSE URL using a session that mimics a real browser.

    Args:
        url (str): The target NSE API URL.
        origin_url (str, optional): The origin URL to fetch cookies from initially. Defaults to "https://www.nseindia.com".
        timeout (int, optional): Timeout in seconds. Defaults to 10.

    Returns:
        requests.Response: The HTTP response object.

    Example:
        >>> from nselib import request_maker
        >>> response = request_maker.nse_urlfetch('https://www.nseindia.com/api/holiday-master?type=trading')
    """
    return NSEFetcher.fetch(url, origin_url=origin_url, timeout=timeout)
