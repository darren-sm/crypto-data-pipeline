import requests
import logging 
from io import StringIO

def send_request(link: str, retries: int = 3) -> requests.models.Response:
    """
    Send an HTTP request to `link`. If the request failed, it will retry for `retry` times which is 3 by default.

    Parameters:
    -----------
    - link: string. Link to be accessed
    - retries: integer. Number of times to retry the request if the first fails. Defaults to 3 retry times.

    Returns:
    -----------
    - response: requests.models.Response. Response of the page we requested to access

    Example:
    -----------
    >>> import requests
    >>> link = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_de&per_page=100&page=1"
    >>> r = send_request(link)
    >>> print(type(r), r)
    <class 'requests.models.Response'> <Response [200]>
    """

    headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.61 Safari/537.36"}

    for retry_number in range(1, retries + 1):
        try:
            # Send the HTTP request on `link`
            logging.debug("Sending request number %s of %s on %s", retry_number, retries, link)
            r = requests.get(link, headers=headers)
        except requests.exceptions.Timeout as e:
            # Request Timeout - request took too much to load
            logging.warning("WARNING: Attempt number %s of %s failed. %s\n", retry_number, retries, e)
            continue
        except requests.exceptions.HTTPError as e:
            # HTTP Error - 401 Status, Unauthorized, etc. Exit the application if the request fails due to this
            logging.error("ERROR: Something is wrong with the link. %s. Now exiting the application", e)
            raise SystemExit()
        except requests.exceptions.RequestException as e:
            # Something went wrong with the request. Covers other unspecified errors like ConnectionError and TooManyRedirects
            logging.error("ERROR: Request failed. %s. Now exiting the application", e)
            raise SystemExit()

        # If the request proceeds without error, break the loop and return the response
        break
    else:
        # Exit the app if all retries are exhausted
        logging.error("ERROR: All %s attemps of HTTP request on link %s failed. Now exiting the application", retries, link)
        raise SystemExit()

    # If the code continues to run, it means that the request successfully went through.
    logging.debug("SUCCESS: Request granted with status code of %s", r.status_code)
    return r


if __name__ == "__main__":

    logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(filename)s.%(funcName)s): %(message)s', level=logging.DEBUG)
    link = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_de&per_page=100&page=1"
    send_request("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_de&per_page=100&page=1")
    send_request("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usasdasd&order=market_cap_de&per_page=100&page=1")
