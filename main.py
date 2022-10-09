import requests
import logging 
import datetime
import psycopg2
import decimal
import json

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

    transformed_data = response.json()

    # Remove some values we do not need
    keys_to_remove = ["id", "image", "roi"]
    for record in transformed_data:
        for key in keys_to_remove:
            record.pop(key, None)        

    # Fix values based on attribute datatype and constraint. Set real attributes values to 2 decimal points
    decimal_attributes = {"current_price" : 2, "high_24h" : 2, "low_24h": 2, "price_change_24h" : 2, "circulating_supply" : 2, "price_change_percentage_24h" : 4, "market_cap_change_24h" : 2,   "market_cap_change_percentage_24h" : 4, "ath" : 2, "ath_change_percentage" : 2, "atl" :2, "atl_change_percentage" : 2}

    for record in transformed_data:
        for attribute, decimal_places in decimal_attributes.items():
            record[attribute] = round(decimal.Decimal(str(record[attribute])), decimal_places)

    return transformed_data

def LOAD_DATA(clean_data: list):
    """
    Initial state of loading data to a database. For now, I am loading the clean records to a cloud PostgreSQL database. I will change it to fit for BigQuery.
    """
    
    # read credentials from a JSON file
    with open("credentials.json", "r") as f:
        json_content = json.load(f)
        credentials = json_content['postgresql']

    # Setup connection to the PostgreSQL database
    conn = psycopg2.connect(host = credentials['host'], database = credentials['database'], user = credentials['user'], password = credentials['password'])
    cursor = conn.cursor()

    # Create table if it does not yet exist
    with conn:
        cursor.execute("CREATE TABLE IF NOT EXISTS crypto(symbol varchar(5), name varchar(25), current_price decimal(8,2), market_cap bigint, market_cap_rank integer, fully_diluted_valuation bigint, total_volume bigint, high_24h decimal(8,2), low_24h decimal(8,2), price_change_24h decimal(8,2), price_change_percentage_24h decimal(8,2), market_cap_change_24h decimal(20,2), market_cap_change_percentage_24h decimal, circulating_supply decimal(20,2), total_supply bigint, max_supply bigint, ath decimal(8,2), ath_change_percentage decimal, ath_date timestamp, atl decimal(8,2), atl_change_percentage decimal, atl_date timestamp, last_updated timestamp, PRIMARY KEY(symbol, last_updated))")

    with conn:
        cursor.executemany("INSERT INTO crypto VALUES(%(symbol)s, %(name)s, %(current_price)s, %(market_cap)s, %(market_cap_rank)s, %(fully_diluted_valuation)s, %(total_volume)s, %(high_24h)s, %(low_24h)s, %(price_change_24h)s, %(price_change_percentage_24h)s, %(market_cap_change_24h)s, %(market_cap_change_percentage_24h)s, %(circulating_supply)s, %(total_supply)s, %(max_supply)s, %(ath)s, %(ath_change_percentage)s, %(ath_date)s, %(atl)s, %(atl_change_percentage)s, %(atl_date)s, %(last_updated)s) ON CONFLICT ON CONSTRAINT crypto_pkey DO NOTHING;", clean_data)


def main(request):    
    logging.basicConfig(format='[%(asctime)s] %(levelname)s (%(filename)s.%(funcName)s): %(message)s', level=logging.DEBUG)
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

    return "Session Finished"