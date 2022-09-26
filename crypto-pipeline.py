#!/usr/bin/env python3
import requests
import logging 
import datetime
import sqlite3

def GET_DATA(link: str, retries: int = 3) -> requests.models.Response:
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

def TRANSFORM_DATA(response: requests.models.Response) -> list:
    """
    Parse the response of the request we sent through `GET_DATA` function. Transofrm the JSON response suitable to the database schema.

    Parameters:
    -----------
    - response: requests.models.Response. Response from request.get method.    

    Returns:
    -----------
    - clean_data: list of dictionaries. Each dictionary contain the crypto data for a specific coin on that given timeframe

    Example:
    -----------
    >>> r = GET_DATA(link)
    >>> clean_data = TRANSFORM_DATA(r)
    >>> print(clean_data)
    [
        {
            'symbol': 'btc',
            'name': 'Bitcoin',
            'current_price': 18869.81,
            ...
        },
        {
            'symbol': 'eth',
            'name': 'Ethereum',
            'current_price': 1294.24,
            ...
        },
        ...
    ]
    """

    keys_to_remove = ["id", "image", "roi"]
    for record in response.json():
        for key in keys_to_remove:
            record.pop(key, None)        

    return response.json()

def LOAD_DATA(clean_data: list):
    """
    Initial state of loading data to a database. For now, I am loading the clean records to a local SQLite3 database. I will change it to fit for BigQuery.
    """
    conn = sqlite3.connect("/home/dsm/Documents/All Projects/Crypto/local.db")
    cursor = conn.cursor()

    with conn:
        cursor.execute("CREATE TABLE IF NOT EXISTS crypto(symbol text, name text, current_price real, market_cap integer, market_cap_rank integer, fully_diluted_valuation integer, total_volume integer, high_24h real, low_24h real, price_change_24h real, price_change_percentage_24h real, market_cap_change_24h real, market_cap_change_percentage_24h real, circulating_supply integer, total_supply integer, max_supply integer, ath real, ath_change_percentage real, ath_date text, atl real, atl_change_percentage real, atl_date text, last_updated text)")

    with conn:
        cursor.executemany("INSERT INTO crypto VALUES(:symbol, :name, :current_price, :market_cap, :market_cap_rank, :fully_diluted_valuation, :total_volume, :high_24h, :low_24h, :price_change_24h, :price_change_percentage_24h, :market_cap_change_24h, :market_cap_change_percentage_24h, :circulating_supply, :total_supply, :max_supply, :ath, :ath_change_percentage, :ath_date, :atl, :atl_change_percentage, :atl_date, :last_updated)", clean_data)


if __name__ == "__main__":
    log_filename = f'/home/dsm/Documents/All Projects/Crypto/{datetime.date.today().strftime("%Y-%m-%d")}.log'
    logging.basicConfig(filename = log_filename, format='[%(asctime)s] %(levelname)s (%(filename)s.%(funcName)s): %(message)s', level=logging.DEBUG)
    session_start = datetime.datetime.now()
    logging.info("---STARTING NEW SESSION: %s---", session_start.strftime("%Y-%m-%d %H:%M"))

    link = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_de&per_page=100&page=1"
    response = GET_DATA(link)
    clean_data = TRANSFORM_DATA(response)
    LOAD_DATA(clean_data)

    session_end = datetime.datetime.now()
    process_duration = session_end - session_start 
    logging.info("---END OF SESSION: %s---", session_end.strftime("%Y-%m-%d %H:%M"))
    logging.info("Process took %s second(s)\n", f"{process_duration.seconds}.{process_duration.microseconds}")