from sqlalchemy import create_engine, Column, String, JSON, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func
from sqlalchemy.orm import relationship, backref
from sqlalchemy import ForeignKey
from sqlalchemy.ext.associationproxy import association_proxy

from typing import Dict

import pandas as pd

import csv
from datetime import datetime, timedelta
import time
import logging





Base = declarative_base()

class xNews(Base):
    __tablename__ = 'xnews'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    url = Column(String)
    function = Column(String)
    time = Column(DateTime, nullable=False)
    json_data = Column(JSON)
  
     # Modified topics relationship
    #topics = relationship('xTopic', secondary='xnews_topics', back_populates='news_topics')
  
#    topics = relationship('xTopic', secondary='xnews_topics', back_populates='news')
#    tickers = relationship('xTicker', secondary='xnews_tickers', back_populates='news')

 #   news_topics = relationship('xNewsTopics', backref='news')
    topics = relationship('xNewsTopics', back_populates='news')
    tickers = relationship('xNewsTickers', back_populates='news')


    __table_args__ = (
        UniqueConstraint('url'),
    )


    @classmethod
    def insert_if_not_exist(cls, session: Session, title: str, function: str, url: str, time: datetime, json_data: dict):
        # Check if the row already exists in the database
    #    print("insert if not....",url)
    #    print(session)
    #    print(title)
    #    print(function)
    #    print(url)
    #    print(time)
    #    print(json_data)
    #    exit(0)
#        print("insert if not....",url)
        existing_row = session.query(cls).filter_by(url=url).first()
        if existing_row is not None:
            return existing_row,True
        
        # If the row does not exist, create a new instance and add it to the session
        new_row = cls(title=title, function=function, url=url, time=time, json_data=json_data)
        session.add(new_row)
        return new_row,False



    def add_topic(self,session, topic,relevance,sentiment):
        """
        Adds a new topic to the news item and creates a relationship between the
        news item and the topic using the xNewsTopics association object.
        """
        # Check if the topic already exists
        topic_obj = session.query(xTopic).filter_by(topic=topic).first()
        
        # If the topic doesn't exist, create a new xTopic object
        if not topic_obj:
            topic_obj = xTopic(topic=topic)
            session.add(topic_obj)
            session.flush()
            print("add topic",topic_obj.id,topic_obj.topic)
        # Create a new xNewsTopics object to create the relationship
        news_topic = xNewsTopics.insert_if_not_exists(session=session,relevance=relevance, sentiment=sentiment, topic_id=topic_obj.id, news_id=self.id)
        session.add(news_topic)

        # Add the topic to the news object's topics list
        self.topics.append(news_topic)
#        self.topics.append(topic_obj)


    def add_ticker(self,session, ticker,relevance,sentiment):
        """
        Adds a new topic to the news item and creates a relationship between the
        news item and the topic using the xNewsTopics association object.
        """
        # Check if the topic already exists
        ticker_obj = session.query(xTicker).filter_by(ticker=ticker).first()
        
        # If the topic doesn't exist, create a new xTopic object
        if not ticker_obj:
            ticker_obj = xTicker(ticker=ticker)
            session.add(ticker_obj)
            session.flush()
#            print("adding ticker",ticker_obj.id,ticker_obj.ticker)
        # Create a new xNewsTopics object to create the relationship
#        news_ticker = xNewsTickers(relevance=relevance, sentiment=sentiment, ticker_id=ticker_obj.id, news_id=self.id)
        news_ticker = xNewsTickers.insert_if_not_exists(session=session,relevance=relevance, sentiment=sentiment, ticker_id=ticker_obj.id, news_id=self.id)
        session.add(news_ticker)
        
        # Add the topic to the news object's topics list
        self.tickers.append(news_ticker)

    def statistics(self,session, date: str) -> Dict[str, Dict[str, float]]:
        # Convert date string to datetime object
        date_obj = datetime.strptime(date, "%Y%m%d")

        # Query the database for all articles with the given date
        articles = session.query(xNews).filter(func.date(xNews.time) == date_obj.date()).all()
        print(articles)
        exit(0)
        # Group articles by topic
        topics = {}
        for article in articles:
            for topic in article.topics:
                if topic.topic not in topics:
                    topics[topic.topic] = []
                topics[topic.topic].append((article, topic.relevance, topic.sentiment))

        # Calculate average relevance and sentiment for each topic
        topic_stats = {}
        for topic, topic_articles in topics.items():
            relevance_sum = 0
            sentiment_sum = 0
            for article, relevance, sentiment in topic_articles:
                relevance_sum += relevance
                sentiment_sum += sentiment
            relevance_avg = relevance_sum / len(topic_articles)
            sentiment_avg = sentiment_sum / len(topic_articles)
            topic_stats[topic] = {"relevance": relevance_avg, "sentiment": sentiment_avg}
        return topic_stats




class xTopic(Base):
    __tablename__ = 'xtopics'
    id = Column(Integer, primary_key=True)
    topic = Column(String)

# Modified news relationship
    news_obj = relationship('xNewsTopics', back_populates='topic')
    
    def __repr__(self):
        return f"xTopic{self.id} {self.topic}"
    
#    news = relationship('xNews', secondary='xnews_topics', back_populates='topics')

class xTicker(Base):
    __tablename__ = 'xtickers'
    id = Column(Integer, primary_key=True)
    ticker = Column(String)
    
   # news = relationship('xNews', secondary='xnews_tickers', back_populates='tickers')
    news_obj = relationship('xNewsTickers', back_populates='ticker')


    def __repr__(self):
        return f"xTicker{self.id} {self.ticker}"
    



class xNewsTopics(Base):
    __tablename__ = 'xnews_topics'
    news_id = Column(Integer, ForeignKey('xnews.id'), primary_key=True)
    topic_id = Column(Integer, ForeignKey('xtopics.id'), primary_key=True)
    relevance = Column(Float)
    sentiment = Column(Float)

    # New relationship to the xTopic object
    news = relationship('xNews', back_populates='topics') ##--> News
    topic = relationship('xTopic', back_populates='news_obj') ## --> Topic

    def __repr__(self):
        return f"xNewsTopic: news_id:{self.news_id} topic_id:{self.topic_id} relevance:{self.relevance} sentiment:{self.sentiment}"
    

    @classmethod
    def insert_if_not_exists(cls, session, news_id, topic_id, relevance, sentiment):
        """
        Insert a new row into the xnews_topics table if a row with the same
        news_id and topic_id does not already exist.
        """
#        print("insert if it does not exist",news_id,topic_id)
        existing_row = session.query(cls).filter_by(news_id=news_id, topic_id=topic_id).first()
        if existing_row is None:
            new_row = cls(news_id=news_id, topic_id=topic_id, relevance=relevance, sentiment=sentiment)
            session.add(new_row)
            session.commit()
#            print("new row")
            return new_row
        else:
#            print("existing row")
            return existing_row


class xNewsTickers(Base):
    __tablename__ = 'xnews_tickers'
    news_id = Column(Integer, ForeignKey('xnews.id'), primary_key=True)
    ticker_id = Column(Integer, ForeignKey('xtickers.id'), primary_key=True)
    relevance = Column(Float)
    sentiment = Column(Float)

    news = relationship('xNews', back_populates='tickers') ## --> News
    ticker = relationship('xTicker', back_populates='news_obj') ## --> Tickers

    def __repr__(self):
        return f"xNewsTicker: news_id:{self.news_id} ticker_id:{self.ticker_id} relevance:{self.relevance} sentiment:{self.sentiment}"


    @classmethod
    def insert_if_not_exists(cls, session, news_id, ticker_id, relevance, sentiment):
        """
        Inserts a new xNewsTickers object into the database if an object with the
        same news_id and ticker_id does not already exist. Returns the newly created
        object if it was inserted, otherwise returns the existing object.

        Args:
            session (Session): The SQLAlchemy session to use for the operation.
            news_id (int): The ID of the news article to associate with the ticker.
            ticker_id (int): The ID of the ticker to associate with the news article.
            relevance (float): The relevance of the ticker to the news article.
            sentiment (float): The sentiment score of the ticker in the news article.

        Returns:
            xNewsTickers: The newly created or existing xNewsTickers object.
        """
        obj = cls(news_id=news_id, ticker_id=ticker_id, relevance=relevance, sentiment=sentiment)
        try:
            session.add(obj)
            session.commit()
        except IntegrityError:
            session.rollback()
            obj = session.query(cls).filter_by(news_id=news_id, ticker_id=ticker_id).first()
        return obj


###################################33
# xTimeseries

class xTimeseries(Base):
    __tablename__ = 'xtimeseries'
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    function = Column(String)
    interval = Column(String)
    time = Column(DateTime, nullable=False)
    json_data = Column(JSON)
    
    __table_args__ = (
        UniqueConstraint('symbol', 'function', 'interval', 'time', name='uc_xtimeseries'),
    )
    
    
    def __repr__(self):
        return f"xTimeseries(symbol={self.symbol!r}, function={self.function!r}, interval={self.interval!r}, time={self.time!r}, json_data={self.json_data!r})"



    @classmethod
    def get_symbols(cls, session):
        symbols = session.query(cls.symbol).distinct().all()
        return [symbol[0] for symbol in symbols]

    @classmethod
    def get_all_symbols(cls, sessions):
        symbol_names = []
        for session in sessions:
            symbols = session.query(cls.symbol).distinct().all()
            for symbol in symbols:
                symbol_names.append(symbol[0])
        return symbol_names
        
        
    @classmethod
    # xTimeseries.insert_if_not_exist(session, symbol=symbol, function='TIME_SERIES_INTRADAY_EXTENDED', interval=interval, time=ts, json_data=row)
    def insert_if_not_exist(cls, session: Session, symbol: str, function: str, interval: str, time: datetime, json_data: dict):
        # Check if the row already exists in the database
        existing_row = session.query(cls).filter_by(symbol=symbol, function=function, interval=interval, time=time).first()
        if existing_row is not None:
#            print("....insert if not exist....",symbol,function,interval,time,"-->exits")
            return existing_row,True
        
        # If the row does not exist, create a new instance and add it to the session
        new_row = cls(symbol=symbol, function=function, interval=interval, time=time, json_data=json_data)
        session.add(new_row)
        return new_row,False

    @classmethod
    def insert(cls, session: Session, symbol: str, function: str, interval: str, time: datetime, json_data: dict):
        try:
            print("....try to insert.",symbol,function,interval,time)
            new_row = cls(symbol=symbol, function=function, interval=interval, time=time, json_data=json_data)
            session.add(new_row)
            return new_row,False
        except IntegrityError as e:
            print("....INTEGRITY ERROR",symbol,function,interval,time)
            exit(0)            
            session.rollback()
            existing_row = session.query(cls).filter_by(symbol=symbol, function=function, interval=interval, time=time).first()
            return existing_row,True



    @classmethod
    def statistics(cls,session):
        """Query the amount of rows and first/last time values for each symbol and function"""
        results = (
            session.query(
                cls.symbol,
                cls.function,
                func.count(cls.id),
                func.min(cls.time),
                func.max(cls.time),
            )
            .group_by(cls.symbol, cls.function)
            .all()
        )
        session.close()
        return results



    @classmethod
    def df(cls, session, symbol, function):
        print("df",symbol)
        """Read all objects for a given symbol and function combination and write to CSV file"""
        data = session.query(cls).filter_by(symbol=symbol, function=function).all()
        session.close()
        if not data:
            return None
        column_names = ["symbol","time","function"] + list(data[0].json_data.keys())
        rows = []
        for row in data:
            row_data = [row.symbol, row.time, row.function] + list(row.json_data.values())
            rows.append(row_data)
        print("len",len(rows))
        dataframe = pd.DataFrame(rows)
        dataframe.columns = column_names
        
#        dataframe = pd.DataFrame(data = rows, columns=column_names)
        return dataframe

        
    @classmethod
    def csv(cls, session, symbol, function):
        """Read all objects for a given symbol and function combination and write to CSV file"""
        data = session.query(cls).filter_by(symbol=symbol, function=function).all()
        session.close()

        if not data:
            return "No data found for the given symbol and function combination"

        # Extract the column names from the JSON data
        column_names = list(data[0].json_data.keys())

        # Open a new CSV file and write the header row
        filename = f"{symbol}_{function}.csv"
        with open(filename, mode='w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['symbol', 'time', 'function'] + column_names)

            # Write each row of data to the CSV file
            for row in data:
                row_data = [row.symbol, row.time, row.function] + list(row.json_data.values())
                writer.writerow(row_data)

        return f"CSV file '{filename}' has been created"
    
    
    
