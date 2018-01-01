import threading
import requests
import datetime
import time
import boto3
from dateutil import tz
from numpy import long
import numpy as np
from decimal import Decimal

callInterval = 10.0 # Run every X seconds
updateCurrencyIntervalIteration = 360 # Update currency exchange rate every 1hr

USDKRW = 1100.0 # Init
USDHKD = 1.0 # Init

iteration = 0
starttime=time.time()

def run_check():

    # Run this function forever
    global iteration
    iteration += 1
    threading.Timer(callInterval, run_check).start()

    # Get/Update currency exchange rate
    if (iteration % updateCurrencyIntervalIteration) == 1:
        # get_currency_rate_usd_krw_currencylayer()
        print("ITERATION: " + str(iteration))
        print("USDKRW: " + str(USDKRW))
        print("USDHKD: " + str(USDHKD))

    # Get data from markets API
    # coinone = get_coinone_ticker()
    # print("Coinone Ticker")
    # print("Volume" + str(round(float(coinone["volume"]), 2)))
    # print("Price" + str(round(coinone["last"]) / USDKRW, 2))

    data = get_btc_ticker_info()
    print("--- Bithumb Ticker ---")
    print("Current Time: " + datetime.datetime.now().strftime("%Y-%m-%d, %H:%M:%S.%f"))
    print("Date: " + data["data"]["date"][:-3])

    txn = get_btc_recent_transactions()
    print("--- Bithumb Txn ---")
    print("Txn Status Code: " + txn["status"])
    print("Txn Date: " + txn["data"][0]["transaction_date"])
    print("Txn Price: " + str(round(float(txn["data"][0]["price"])/USDKRW, 2)))

    bitfinex = get_bitfinex_btx_ticker_info()
    print("--- Bitfinex ---")
    print("Bitfinex Last Price: " + bitfinex["last_price"])
    print("Bitfinex Volume: " + bitfinex["volume"])
    print("Bitfinex timestamp: " + bitfinex["timestamp"])

    gatecoin = get_gatcoin_btc_ticker_info()
    print("--- Gatecoin ---")
    print("Gatecoin Currency Pair: " + str(gatecoin["tickers"][2]["currencyPair"]))
    print("Gatecoin Last Price: " + str(gatecoin["tickers"][2]["last"]))
    print("Gatecoin 24h Volume: " + str(long(gatecoin["tickers"][2]["volume"])))
    print("Gatecoin timestamp: " + str(gatecoin["tickers"][2]["createDateTime"]))

    print("--------------------------------\n")

    rds = boto3.client('rds')

    hostname = 'fypinstance.csbqmphhsfqb.ap-northeast-2.rds.amazonaws.com'
    port = 3306
    username = 'password'
    password = 'password'
    database = 'dtw'

    # Simple routine to run a query on a database and print the results:
    def doQuery(conn):
        cur = conn.cursor()

        cur.execute("SELECT fname, lname FROM employee")

        for firstname, lastname in cur.fetchall():
            print
            firstname, lastname

    print ("Using mysql.connector…")
    import mysql.connector
    cnx = mysql.connector.connect(host=hostname, user=username, passwd=password, db=database)
    cursor = cnx.cursor()

    add_salary = ("INSERT INTO priceVolume "
                  "(timestamp, currency, marketName, price, volume) "
                  "VALUES (%(timestamp)s, %(currency)s, %(marketName)s, %(price)s, %(volume)s)")

    # Insert information
    inputData = {
        'timestamp': datetime.datetime.now(),
        'currency': "KRW",
        'marketName': "Bithumb",
        'price': str(round(float(txn["data"][0]["price"]) / USDKRW, 2)),
        'volume': str(round(float(data["data"]["volume_1day"]), 2))
    }
    cursor.execute(add_salary, inputData)

    # Make sure data is committed to the database
    cnx.commit()

    cursor.close()
    cnx.close()



    

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('market-prices')

    table.put_item(
        Item={
            'date': int(datetime.datetime.now().strftime("%Y%m%d")),
            'time': long(datetime.datetime.now().strftime("%H%M%S%f")[:-4]), # Microsecond precision 2
            'readableTS': str(datetime.datetime.now())[:-5],

            'BithumbTS': data["data"]["date"][:-3],
            'BithumbPrice': str(round(float(txn["data"][0]["price"]) / USDKRW, 2)),
            'Bithumb24Vol': str(round(float(data["data"]["volume_1day"]), 2)),

            # 'CoinoneTS': coinone["timestamp"],
            # 'CoinonePrice': str(round(coinone["last"]) / USDKRW, 2),
            # 'Coinone24Vol': str(round(float(coinone["volume"]), 2)),

            'GatecoinTS': gatecoin["tickers"][2]["createDateTime"],
            'GatecoinPrice': str(gatecoin["tickers"][2]["last"]),
            'Gatecoin24Vol': str(round(float(gatecoin["tickers"][2]["volume"]), 2)),

            'BitfinexTS': bitfinex["timestamp"],
            'BitfinexPrice': bitfinex["last_price"],
            'Bitfinex24Vol': bitfinex["volume"]
        }
    )

    # table.put_item(
    #     Item={
    #         'marketTypeTS': '00' + data["data"]["date"][:-3],
    #         'timestamp': str(long(data["data"]["date"][:-3])),
    #         'readableTS': convertTimeZone(data["data"]["date"][:-3]),
    #         'marketName': 'Bithumb',
    #         'price': str(round(float(txn["data"][0]["price"])/USDKRW, 2)),
    #         '24hVolume': str(round(float(data["data"]["volume_1day"]), 2))
    #         # String
    #     }
    # )
    #
    # table.put_item(
    #     Item={
    #         'marketTypeTS': '01' + str(gatecoin["tickers"][2]["createDateTime"]),
    #         'timestamp': str(gatecoin["tickers"][2]["createDateTime"]),
    #         'readableTS': str(convertTimeZone(gatecoin["tickers"][2]["createDateTime"])),
    #         'marketName': 'Gatecoin',
    #         'price': str(gatecoin["tickers"][2]["last"]),
    #         '24hVolume': str(round(float(gatecoin["tickers"][2]["volume"]), 2))
    #         # Float
    #     }
    # )

def truncate(f, n):
    '''Truncates/pads a float f to n decimal places without rounding'''
    s = '{}'.format(f)
    if 'e' in s or 'E' in s:
        return '{0:.{1}f}'.format(f, n)
    i, p, d = s.partition('.')
    return '.'.join([i, (d+'0'*n)[:n]])

def get_coinone_ticker():
    response = requests.get('https://api.coinone.co.kr/ticker/btc')
    print(response.text)
    return response.json()

def get_btc_ticker_info():

    response = requests.get('https://api.bithumb.com/public/ticker/BTC')
    print(response.text)
    return response.json()

def get_btc_recent_transactions():

    response = requests.get('https://api.bithumb.com/public/recent_transactions/BTC')
    print(response.text)
    return response.json()

def get_bitfinex_btx_ticker_info():
    url = "https://api.bitfinex.com/v1/pubticker/btcusd"
    response = requests.request("GET", url)
    print(response.text)
    return response.json()

def get_currency_rate_usd_krw_currencylayer():
    url = "http://apilayer.net/api/live?access_key=284c7fdfa6651d77256be4ba30fe95a6&currencies=KRW,%20HKD&source=USD&format=1"
    response = requests.request("GET", url).json()
    global USDKRW
    USDKRW = response["quotes"]["USDKRW"]
    global USDHKD
    USDHKD = response["quotes"]["USDHKD"]

def get_gatcoin_btc_ticker_info():
    url = "https://api.gatecoin.com/Public/LiveTickers"
    response = requests.request("GET", url)
    print(response.text)
    return response.json()

def convertTimeZone(ts):
    return datetime.datetime.fromtimestamp(int(ts)).strftime('%Y-%m-%d %H:%M:%S')


# def convertTimeZone(ts):
#     # METHOD 1: Hardcode zones:
#     from_zone = tz.gettz('UTC')
#     to_zone = tz.gettz('America/New_York')
#
#     # METHOD 2: Auto-detect zones:
#     # from_zone = tz.tzutc()
#     # to_zone = tz.tzlocal()
#
#     # utc = datetime.utcnow()
#     utc = ts
#
#     # Tell the datetime object that it's in UTC time zone since
#     # datetime objects are 'naive' by default
#     # utc = utc.replace(=from_zone)
#
#     # Convert time zone
#     return utc.astimezone(to_zone)

# def get_currency_rate_usd_krw():
#     page = requests.get('https://search.naver.com/search.naver?where=nexearch&sm=top_hty&fbm=0&ie=utf8&query=usd')
#     # tree = html.fromstring(page.content)
#     # # This will create a list of buyers:
#     # buyers = tree.xpath('//span[@id="drt_to_span"]/value()')
#     # print(buyers[0])
#
#     soup = BeautifulSoup(page.content, 'html.parser')
#     print(soup.prettify())
#     drt_to_span = soup.find_all(id="drt_to_span")
#     print(drt_to_span)


run_check()
