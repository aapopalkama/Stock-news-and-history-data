from sqlalchemy import create_engine, Column, String, JSON, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
#from alphavantage import get_listing_status,get_intraday_data, get_stock_data,get_intraday_data_extended,get_daily_data
from alphavantage import av_get_intraday_data_extended,av_get_daily_data
from xmodels import xTimeseries, Base
from xdb import session_chooser, session_list
#from xmodels import xTimeseries
import requests
import csv
from io import StringIO
from datetime import datetime, timedelta
import time
import argparse
import logging


###################################
# some helper functions

def elapsed_time_in_hms(start_time):
    end_time = time.time()
    elapsed_time = end_time - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    return int(hours), int(minutes), seconds, elapsed_time

echo = False
alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

def do_timelogging(start_time,text1="",text2="",text3=""):
    hours, minutes, seconds,elapsed_seconds = elapsed_time_in_hms(start_time)
    logging.info(f"Elapsed time: {hours} hours, {minutes} minutes, {seconds:.2f} seconds - {text1}/{text2}/{text3}")

def secret_get_api_key():
    api_key = "<your_api_key"
    return api_key

def sqldb_populate_with_daily_data(symbol,outputsize):
    start_time = time.time()
    print(start_time)
    api_key = secret_get_api_key()
    session = session_chooser(symbol)
    logging.info(f"{symbol} start DAILY load to database - {outputsize}")               
    data = av_get_daily_data(api_key,symbol,outputsize)
 #   meta_data = data['Meta Data']
    time_series_data = data['Time Series (Daily)']
    count =  exists_count = 0
    for timestamp, ts_data in time_series_data.items():
        try:
            datetime_string = f"{timestamp} 20:00:00"
            ts = datetime.strptime(datetime_string, '%Y-%m-%d %H:%M:%S')        
        except:
            print("can not generate timestamp", timestamp)
            continue

        xts_obj,exists = xTimeseries.insert_if_not_exist(session, symbol=symbol, function='TIME_SERIES_DAILY_ADJUSTED', interval="daily", time=ts, json_data=ts_data) 
        if xts_obj != None:
            count = count + 1
        if exists:
            exists_count = exists_count + 1
    logging.info(f"{symbol} end DAILY load to database - {outputsize} count:{count}, exits:{exists_count}")        
    do_timelogging(start_time,text1=symbol,text2="final",text3=str(count))
    session.commit()    
    

def sqldb_populate_with_intraday_extended_data(symbol,lastmonth_only=True,interval="5min"):
    start_time = time.time()
    print(start_time)
    api_key = secret_get_api_key()
    symbols = symbol

    print(symbols)
    print(f"We start with {symbol}")
    session = session_chooser(symbol)
    interval = "5min" # Hardcoded interval value, replace it as needed

    if lastmonth_only:
        slices = ["year1month1"]
        logging.info(f"{symbol} start REFRESH load to database (last month only)")        
    else:
        slices = [f"year{year}month{month}" for year in range(1, 3) for month in range(1, 13)]
        logging.info(f"{symbol} start FULL load to database")

    count = 0
    call_time = 0
    exists_count = 0
    for slice in slices:
        call_time_delta = time.time() - call_time
        if call_time_delta < 12:  # can't call more often than 5 times a minute
            print(f"delta is {call_time_delta} - sleep few seconds")
            time.sleep(12 - call_time_delta)
        call_time = time.time()
        data = av_get_intraday_data_extended(api_key=api_key, symbol=symbol, interval=interval, slice=slice)

        timeseries_key = next((key for key in data.keys() if "Time Series" in key), None)
        if timeseries_key:
            for t, row in data[timeseries_key].items():
                mapping = {'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'}
                row = {mapping[k]: v for k, v in row.items()}

                count = count + 1
                if (count % 500) == 0:
                    do_timelogging(start_time, text1=symbol, text2=slice, text3=str(count))

                try:
                    ts = datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    print("can not generate timestamp", row)
                    continue
                #print(f"time: {t}, data: {row}")
                
                xts_obj, exists = xTimeseries.insert_if_not_exist(session, symbol=symbol, function='TIME_SERIES_INTRADAY_EXTENDED',
                                                                    interval=interval, time=ts, json_data=row)
                
                session.commit()

                if exists:
                    exists_count = exists_count + 1

        logging.info(f"count {count} exits_count {exists_count}")

        do_timelogging(start_time, text1=symbol, text2="final", text3=str(count))

def populate_intraday_data(symbols,lastmonth_only=True):
    print("Polulate intraday data")
    for s in symbols:
        print("Populate....",s)
        sqldb_populate_with_intraday_extended_data(symbol=s,lastmonth_only=lastmonth_only)

def populate_daily_data(symbols,outputsize='compact'):
    #print("we are here")
    for s in symbols:
        print(s)
        sqldb_populate_with_daily_data(symbol=s,outputsize=outputsize)
        # time to sleep 5 request per min
        time.sleep(12)

def import_action(symbols,load):
    print("We are here")
    logging.info("import_action({symbol} {load})---->")
    if load == "full":
        #populate_daily_data(symbols,outputsize="full")
        #populate_intraday_data(symbols,lastmonth_only=False)
        print("Not working atm")
    else:
        print(symbols)
      #  populate_daily_data(symbols,outputsize="compact")
        populate_intraday_data(symbols,lastmonth_only=True)

def export_action(symbols):
    for s in symbols:
        session = session_chooser(s)
        msg = xTimeseries.csv(session,symbol=s,function="TIME_SERIES_DAILY_ADJUSTED")
        logging.info(msg)
        msg = xTimeseries.csv(session,symbol=s,function="TIME_SERIES_INTRADAY_EXTENDED")
        logging.info(msg)
                     
def statistic_action():
    for a in alphabets:
        session = session_chooser(a)        
        stats = xTimeseries.statistics(session)
        for s in stats:
            print(f"{a}/{s[0]}/{s[1]}, count:{s[2]} - first:{s[3]}, last:{s[4]}") 
    
def main():
    parser = argparse.ArgumentParser(description="A simple script to load prices from alphavantage and save them to databases.")    
    parser.add_argument("-action", choices=["import", "statistics","export"], help="The action to perform, either 'statistics', 'import' or 'export'.")
    parser.add_argument("-symbols", nargs="*", default = [], help='List of symbols (strings).  examples of symbols =             "NDAQ","MSFT","GOOG","AMZN","NVDA"   ,"TSLA","META","TSM","TCEHY","ASML","AVGO","ORCL","BABA","CSCO","CRM","ABDE","NFLX","SAP","AMD","IBM" ')
    parser.add_argument("-load", choices=["full", "refresh"], default="refresh", help="import load - full load or refresh load (1 month) only. Required if action is 'import'.")
    parser.add_argument("--verbose", action="store_true", help="Enable debug mode with verbose logging.")
    print("test")
    args = parser.parse_args()
    sessions = session_list()
    symbols = xTimeseries.get_all_symbols(sessions)
    print(symbols)
    import_action(symbols,"refresh") 

    """
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.action == "import":
        if len(args.symbols) == 0:
            logging.warning(f"No symbols given. give at least one symbol as an argument, eg. MSFT (you can give multiple by having a space between them")
            return        

        if not args.load:
            logging.info(f"the --load argument was not given, using the {args.load} as default (please check the --help to verify options)")
        import_action(symbols,args.load)                        
    elif args.action == "statistics":
        statistic_action()
    elif args.action == "export":
        export_action(args.symbols)
    else:
        print("error")
    """
if __name__ == "__main__":
    main()






