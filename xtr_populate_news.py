from sqlalchemy import create_engine, Column, String, JSON, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, and_,extract,column
import sqlite3 as sqlite3


#from alphavantage import get_listing_status,get_intraday_data, get_stock_data,get_intraday_data_extended,get_daily_data
from alphavantage import av_get_intraday_data_extended,av_get_daily_data,av_get_news
from xmodels import xTimeseries, Base,xNews,xTicker,xTopic,xNewsTickers,xNewsTopics

import requests
import csv
from io import StringIO
from datetime import datetime, timedelta,date
import time
import argparse
import logging

# py xtr_populate_news.py -action import -fromdate 20230622 -todate 20230701
###################################
# some helper functions

def elapsed_time_in_hms(start_time):
    end_time = time.time()
    elapsed_time = end_time - start_time

    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)

    return int(hours), int(minutes), seconds, elapsed_time




#folder = "d:\\xtrade\db"

##########################################################
echo = False

db = create_engine("sqlite:///" +r"D:\xtrade\db\xtrade_news.db",echo=echo)
Base.metadata.create_all(db)
Session = sessionmaker()
Session.configure(bind=db)
session = Session()

###################################





def do_timelogging(start_time,text1="",text2="",text3=""):
    hours, minutes, seconds,elapsed_seconds = elapsed_time_in_hms(start_time)
    logging.info(f"Elapsed time: {hours} hours, {minutes} minutes, {seconds:.2f} seconds - {text1}/{text2}/{text3}")


# never have your secret key like this.... do not put this to git...
def secret_get_api_key():
    api_key = "D3SGMGI6LOJSA5FN"
    return api_key


                            
def sqldb_populate_news(time_from,time_to,topics,limit):
    news_dict = av_get_news(api_key=secret_get_api_key(),time_from=time_from,time_to=time_to,topics=topics,limit=limit)
    #news_feed_list = news_dict["feed"]
    news_feed_list = news_dict.get("feed", [])
    count =  exists_count = 0
    for news in news_feed_list:
        title = news["title"]
        url = news["url"]
        time = news["time_published"]
        topics = news["topics"]
        tickers = news["ticker_sentiment"]
        overall_sentiment_score = news["overall_sentiment_score"]
#        datetime_string = f"{time} 20:00:00" #20230422T123400
        ts = datetime.strptime(time, '%Y%m%dT%H%M%S')
#        exit(0)
        print(f"insert {title}")
        news_obj,exists = xNews.insert_if_not_exist(session, title=title, function='NEWS_SENTIMENT', url=url, time=ts, json_data=news) 
        for topic in topics:
            news_obj.add_topic(session,topic = topic["topic"],relevance = topic["relevance_score"],sentiment=overall_sentiment_score)
        for ticker in tickers:
            news_obj.add_ticker(session,ticker = ticker["ticker"],relevance = ticker["relevance_score"],sentiment=ticker["ticker_sentiment_score"])
        count = count + 1
        if exists == True:
            exists_count = exists_count + 1

    session.commit()
    print(f"count:{count}, exists_count:{exists_count}")
    logging.info(f"end news load {count}")        

    return count                            

def import_news(time_from, time_to,topics,delta,limit=1):
    # Parse input strings as datetime objects

    dfr = f"{time_from}T0000"
    dto = f"{time_to}T2359"
    date_from = datetime.strptime(dfr, '%Y%m%dT%H%M')
    date_to = datetime.strptime(dto, '%Y%m%dT%H%M')

    start_time = time.time()
    
    # Loop through at intervals of delta days
    total_count = 0
    call_time = 0
    delta = 6
    while date_from < date_to:
        # Process news for current interval
        interval_end = date_from + timedelta(hours=delta)
        if interval_end > date_to:
            interval_end = date_to
        print(f"Processing news for {date_from:%Y%m%dT%H%M} - {interval_end:%Y%m%dT%H%M}")
        time_from = f"{date_from:%Y%m%dT%H%M}"
        time_to = f"{interval_end:%Y%m%dT%H%M}"

        call_time_delta = time.time() - call_time
        if call_time_delta < 12: # can't call more often thatn 5 times a minute
            print(f"delta is {call_time_delta} - sleep few seconds")
            time.sleep(12-call_time_delta)            
        call_time = time.time()


        count = sqldb_populate_news(time_from,time_to,topics,limit)
        total_count = total_count + count
        print(f"topics:{topics} : {time_from} {time_to} count:{count} total:{total_count}")
        # Move to next interval
        date_from = interval_end


    return total_count



def news_statistics_ticker_details(date_start,date_end,exportfolder,filter):
    exportfolder = exportfolder + "/" if not exportfolder.endswith("/") else exportfolder    
    print(date_start,date_end)
    from_date = datetime.strptime(date_start, "%Y%m%d")
    to_date = datetime.strptime(date_end, "%Y%m%d")

    filename = exportfolder + "detail_tickers_" + filter + "_" + date_start + "_" + date_end + ".csv"
    file = open(filename,"w",encoding='utf-8',newline = '')
    writer1 = csv.writer(file)
    fields = ["date","ticker","sentiment","relevance","title","url"]
    writer1.writerow(fields)

    news_query2 = session.query(
        func.strftime('%Y-%m-%d', xNews.time).label('day'), 
        xTicker.ticker, 
        xNewsTickers.sentiment,
        xNewsTickers.relevance,
        xNews.title,
        xNews.url
    ).filter(
        xNews.time >= from_date,
        xNews.time <= to_date,
        xNews.id == xNewsTickers.news_id,
        xNewsTickers.ticker_id == xTicker.id,
        xTicker.ticker == filter
    )
    news_list2 = news_query2.all()
    for row in news_list2:
        print(" - ",row)
        writer1.writerow(row)
    print("total number of ticker rows",date,len(news_list2))           





def export_action_info():
    news_query = session.query(
        func.strftime('%Y-%m-%d', xNews.time).label('day'), 
        func.count(xNews.id).label('count'),  # Add count of news items
    ).filter(
    ).group_by(
        func.strftime('%Y-%m-%d', xNews.time)
    ).order_by(
        func.strftime('%Y-%m-%d', xNews.time)
    )
    news_list = news_query.all()
    for row in news_list:
        print(" - ",row)
        
    first_date = session.query(
        func.strftime('%Y-%m-%d', xNews.time).label('day')
    ).order_by(xNews.time.asc()).first()

    if first_date:
        first_date = first_date.day

    latest_date = session.query(
        func.strftime('%Y-%m-%d', xNews.time).label('day')
    ).order_by(xNews.time.desc()).first()

    if latest_date:
        latest_date = latest_date.day
    print("first",first_date)
    print("last",latest_date)


# works well
def news_statistics_summary(date_start,date_end,exportfolder):
    exportfolder = exportfolder + "/" if not exportfolder.endswith("/") else exportfolder    
    print(date_start,date_end)
    from_date = datetime.strptime(date_start, "%Y%m%d")
    to_date = datetime.strptime(date_end, "%Y%m%d")

    filename = exportfolder + "summary_topics_" + date_start + "_" + date_end + ".csv"
    file = open(filename,"w",encoding='utf-8',newline = '')
    writer1 = csv.writer(file)
    fields = ["date","topic","count","sentiment"]
    writer1.writerow(fields)

    filename2 = exportfolder + "summary_tickers_" + date_start + "_" + date_end + ".csv"
    file2 = open(filename2,"w",encoding='utf-8',newline = '')
    writer2 = csv.writer(file2)
    fields2 = ["date","ticker","count","sentiment"]
    writer2.writerow(fields)


    while from_date < to_date:
        news_query = session.query(xNews).filter(xNews.time.between(from_date, from_date + timedelta(days = 1))).order_by(xNews.time)
        news_list = news_query.all()
        if len(news_list):
            date = news_list[0].time
            totals = len(news_list)
            print(" - topics................................")
            news_query2 = session.query(
                func.strftime('%Y-%m-%d', xNews.time).label('day'), 
                xTopic.topic, 
                func.count(xNews.id).label('count'),  # Add count of news items
                func.avg(xNewsTopics.sentiment)
            ).filter(
                    xNews.time >= from_date,
                    xNews.time < from_date + timedelta(days=1),
                xNews.id == xNewsTopics.news_id,
                xNewsTopics.topic_id == xTopic.id
            ).group_by(
                xTopic.topic,
                func.strftime('%Y-%m-%d', xNews.time)
            )    
            news_list2 = news_query2.all()
            for row in news_list2:
                print(" - ",row)
                writer1.writerow(row)
            print("total number of topic rows",date,len(news_list2))           



            print(" - tickers................................")
            news_query3 = session.query(
                func.strftime('%Y-%m-%d', xNews.time).label('day'), 
                xTicker.ticker, 
                func.count(xNews.id).label('count'),  # Add count of news items
                func.avg(xNewsTickers.sentiment)
            ).filter(
                    xNews.time >= from_date,
                    xNews.time < from_date + timedelta(days=1),
#                xNews.time == from_date,
#                 xNews.time.between(from_date, to_date),
                xNews.id == xNewsTickers.news_id,
                xNewsTickers.ticker_id == xTicker.id
            ).group_by(
                xTicker.ticker,
                func.strftime('%Y-%m-%d', xNews.time)
            )    
            news_list3 = news_query3.all()
            for row in news_list3:
                if row[2] > 5: # print only those having more than 5 news
                    print(" - ",row)
                writer2.writerow(row)
            print("total number of ticker rows",date,len(news_list3))           





        else:
            print("...",from_date,"no data")
        from_date = from_date + timedelta(days = 1)



def export_action_ticker_or_topic(name,date_start,date_end,exportfolder):
    exportfolder = exportfolder + "/" if not exportfolder.endswith("/") else exportfolder
    print(date_start,date_end)
    from_date = datetime.strptime(date_start, "%Y%m%d")
    to_date = datetime.strptime(date_end, "%Y%m%d")
#    news_query = session.query(xNews,xNewsTickers,xTicker).filter(xNews.time.between(from_date, to_date),xNews.id == xNewsTickers.news_id,xNewsTickers.ticker.ticker == name).order_by(xNews.time)

    print(name)
        
    if name == "*":    # go through all tickers
        print("topics................................")
        news_query = session.query(
            xTopic.topic, 
            func.strftime('%Y-%m-%d', xNews.time).label('day'), 
            func.count(xNews.id).label('count'),  # Add count of news items
            func.avg(xNewsTopics.sentiment)
        ).filter(
            xNews.time.between(from_date, to_date),
            xNews.id == xNewsTopics.news_id,
            xNewsTopics.topic_id == xTopic.id
        ).group_by(
            xTopic.topic,
            func.strftime('%Y-%m-%d', xNews.time)
        )    
        news_list = news_query.all()
        total_sentiment = 0
        for row in news_list:
            print(row)
        print("tickers................................")
        news_query = session.query(
            xTicker.ticker, 
            func.strftime('%Y-%m-%d', xNews.time).label('day'), 
            func.count(xNews.id).label('count'),  # Add count of news items
            func.avg(xNewsTickers.sentiment)
        ).filter(
            xNews.time.between(from_date, to_date),
            xNews.id == xNewsTickers.news_id,
            xNewsTickers.ticker_id == xTicker.id
        ).group_by(
            xTicker.ticker,
            func.strftime('%Y-%m-%d', xNews.time)
        )    
        news_list = news_query.all()
        total_sentiment = 0
        for row in news_list:
            print(row)

    elif name == "+":    # go through all tickers but sum all to one
        news_query = session.query(
            column("*"), 
            func.strftime('%Y-%m-%d', xNews.time).label('day'), 
            func.count(xNews.id).label('count'),  # Add count of news items
            func.avg(xNewsTickers.sentiment)
        ).filter(
            xNews.time.between(from_date, to_date),
            xNews.id == xNewsTickers.news_id,
            xNewsTickers.ticker_id == xTicker.id
        ).group_by(
            func.strftime('%Y-%m-%d', xNews.time)
        )    
        news_list = news_query.all()
        total_sentiment = 0
        for row in news_list:
            print(row)

    else:
        news_query = session.query(
            xTicker.ticker, 
            func.strftime('%Y-%m-%d', xNews.time).label('day'), 
            func.count(xNews.id).label('count'),  # Add count of news items
            func.avg(xNewsTickers.sentiment)
        ).filter(
            xNews.time.between(from_date, to_date),
            xNews.id == xNewsTickers.news_id,
            xNewsTickers.ticker_id == xTicker.id,
            xTicker.ticker == name
        ).group_by(
            xTicker.ticker,
            func.strftime('%Y-%m-%d', xNews.time)
        )    
        news_list = news_query.all()
        total_sentiment = 0
        for row in news_list:
            print(row)

        
    

def get_min_max_date(): # max and current date from database
    import pandas as pd
    conn = sqlite3.connect("D:/xtrade/db/xtrade_news.db")
    c = conn.cursor()
    min_max_date = pd.read_sql_query(
        """select min(time) as time_min,
        max(time) as time_max
        FROM xnews"""
        , conn)
    conn.close()
    date_object = datetime.strptime(min_max_date["time_max"][0], "%Y-%m-%d %H:%M:%S.%f")
    max_date = date_object.strftime("%Y%m%d")

    current_date = datetime.now()
    current_date = current_date.strftime("%Y%m%d")
    current_date

    return max_date,current_date



def main():

    max_date , current_date = get_min_max_date()

    parser = argparse.ArgumentParser(description="A simple script to load news from alphavantage and save them to databases.")    
    parser.add_argument("-action", default="import", help="The action to perform, either 'info', 'import', 'exportsummary', 'exportdetails' or '<any ticker> or <any topic>.")
    parser.add_argument("-fromdate", default=max_date, help="first load date in form YYYYMMDD")
    parser.add_argument("-todate", default=current_date, help="last load date in form YYYYMMDD")
    parser.add_argument("-exportfolder", default="./data/", help="exportfolder, where the exported datafiles to be saved, default is './data/'")
    parser.add_argument("--verbose", action="store_true", help="Enable debug mode with verbose logging.")

  #  args = parser.add_argument()
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if args.action == "import":
        if not args.fromdate:
            logging.warning(f"fromdate musg be given")
            return        
        if not args.todate:
            logging.warning(f"todate musg be given")
            return        

        topics = ["blockchain","earnings","ipo","mergers_and_acquisitions","financial_markets","eonomy_fiscal","economy_monetary","economy_macro","energy_transportation","finance","life_sciences","manufacturing","real_estate","retail_wholesale","technology"]
        #    ximport_news(time_from,time_to,"economy_monetary",1)
        all_topics_count = 0
        for topic in topics:
#            total_count = ximport_news(time_from,time_to,topic,delta=1,limit=200)
            total_count = import_news(args.fromdate,args.todate,topic,delta=1,limit=200)
            all_topics_count = all_topics_count + total_count
            print(f"topic:{topic}, count:{total_count}")
        print(f"All topics count:{all_topics_count}")
 
    elif args.action == "exportsummary":
        news_statistics_summary(args.fromdate,args.todate,args.exportfolder)
    elif args.action == "exportdetails":
        export_action_ticker_or_topic(args.action,args.fromdate,args.todate,args.exportfolder)
    elif args.action == 'info':
        export_action_info()
    else:
        print("exporting details of a given ticker(s)")
        news_statistics_ticker_details(args.fromdate,args.todate,args.exportfolder,args.action)
if __name__ == "__main__":
    main()






