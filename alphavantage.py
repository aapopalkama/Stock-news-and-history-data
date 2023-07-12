
import requests
import csv
import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO

# Replace 'your_api_key' with your actual Alpha Vantage API key
#api_key = "D3SGMGI6LOJSA5FN"


#['ZVSA', 'ZyVersa Therapeutics Inc', 'NASDAQ', 'Stock', '2022-12-12', 'null', 'Active']
#['ZVV', 'LISTED TEST SYMBOL', 'NYSE ARCA', 'Stock', '2017-10-11', 'null', 'Active']
#['ZVZZT', 'NASDAQ TEST STOCK', 'NASDAQ', 'Stock', '2017-09-22', 'null', 'Active']

def get_listing_status(api_key):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'LISTING_STATUS',
        'apikey': api_key
    }
    response = requests.get(base_url, params=params)
    decoded_response = response.content.decode('utf-8')
    cr = csv.reader(decoded_response.splitlines(), delimiter=',')
    my_list = list(cr)
    return my_list
#    for row in my_list:
#        print(row)



# Define a function to fetch stock data
def get_stock_data(symbol, api_key):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'GLOBAL_QUOTE',
        'symbol': symbol,
        'apikey': api_key,
    }
    response = requests.get(base_url, params=params)
    return response.json()

# Fetch stock data for Nokia and Konecranes
#nokia_data = get_stock_data('NOK', api_key)
#konecranes_data = get_stock_data('KCR.HE', api_key)  # Konecranes is listed on the Helsinki Stock Exchange (.HE)

# Print the stock data
#print("Nokia (NOK) data:")
#print(nokia_data)

#print("\nKonecranes (KCR.HE) data:")
#print(konecranes_data)

#date, daily open, daily high, daily low, daily close, daily volume


import requests


def get_inflation(api_key):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'INFLATION',
        'apikey': api_key
    }
    response = requests.get(base_url, params=params)
    data = response.json()
#    print(data)    
    for d in data["data"]:
        print(d["date"],d["value"])
    exit(0)
    
def get_moving_average(api_key,symbol):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'SMA',
        'apikey': api_key,
        'symbol':symbol,
        'interval':'weekly',
        'series_type':'open',
        'time_period':'10'
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    print(data)    
    exit(0)
    
  

def get_real_GDP(api_key):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'REAL_GDP',
        'apikey': api_key,
        'interval': "annual",
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    print(data)    
    exit(0)

#time', 'open', 'high', 'low', 'close', 'volume'],
#2023-03-10 04:30:00    4.73  4.74    4.73   4.73     300
def xxxget_intraday_extended_data(api_key,symbol):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY_EXTENDED',
        'symbol': symbol,
        'apikey': api_key,
        'interval': "5min",
        'slice' : 'year1month1'
    }
    response = requests.get(base_url, params=params)
    decoded_response = response.content.decode('utf-8')
    fields = ['time', 'open', 'high', 'low', 'close', 'volume']
    return fields,decoded_response


def xxget_intraday_data(api_key,symbol):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'apikey': api_key,
        'interval': "5min"
    }
    response = requests.get(base_url, params=params)
    decoded_response = response.content.decode('utf-8')
    fields = ['time', 'open', 'high', 'low', 'close', 'volume']
    return fields,decoded_response

def get_intraday_data(api_key,symbol,interval):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize' : 'full',
        'interval': interval
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    return data

#    print(response.content)
    return response.content
#    exit(0)
#    return response.json()


# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
#CSV_URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=IBM&interval=15min&slice=year1month1&apikey=demo'
def get_intraday_data_extended(api_key,symbol,interval,slice="year1month1"):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY_EXTENDED',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize' : 'full',
        'interval': interval,
        'slice': slice
    }
    download = requests.get(base_url, params=params)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    return my_list

###########################################################

def av_get_news(api_key,time_from="202101010000",time_to=None,topics=None,limit=1):
    base_url = 'https://www.alphavantage.co/query'
    if topics == None:
        params = {
            'function': 'NEWS_SENTIMENT',
    #       'tickers': None,
    #       'topics': None,
            'time_from': time_from,
            'time_to': time_to,
            'apikey': api_key,
            'sort' :'latest',
            'limit' : limit
        }
    else:
        params = {
            'function': 'NEWS_SENTIMENT',
    #       'tickers': None,
            'topics': topics,
            'time_from': time_from,
            'time_to': time_to,
            'apikey': api_key,
            'sort' :'latest',
            'limit' : limit
        }

    response = requests.get(base_url, params=params)
    data = response.json()
    return data
    
    
    
def av_get_daily_data(api_key,symbol,outputsize='compact'):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'apikey': api_key,
        'outputsize' : outputsize
    }
    response = requests.get(base_url, params=params)
    data = response.json()
    return data

def av_get_intraday_data_extended(api_key,symbol,interval,slice="year1month1"):
    base_url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_INTRADAY',
        'symbol': symbol,
        'apikey': api_key,
        'interval': interval,
       # 'outputsize' : 'full',
       # 'slice': slice
    }
    #url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'
    
    try:
        download = requests.get(base_url, params=params)
        data = download.json()
    except:
        pass
    #print(data)
    #decoded_content = download.content.decode('utf-8')
#    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    #csv_reader = csv.DictReader(decoded_content.splitlines(), delimiter=',')
   # print(csv_reader)
    return data
    logging.info(csv_reader)
    for row in csv_reader:
        logging.info(row)
        exit(0)
    exit(0)

    my_list = list(cr)
    return my_list
