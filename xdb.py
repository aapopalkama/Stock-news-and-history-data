from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from xmodels import xTimeseries, Base



alphabets = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#alphabets = "ABCD"
echo = False

def db_name(selector):
    name = "xtrade_" + selector + ".db"
    return name

sessions = []
dbs = []

folder = "d:\\xtrade\db"


for a in alphabets:
  #  db = create_engine("sqlite:///" + folder + "/" +  db_name(a),echo=echo)
    #create_engine("sqlite:///" +r"D:\xtrade\db\xtrade_news.db",echo=echo)
  #  db = create_engine("sqlite:///D:\\xtrade\\db\\xtrade_" + a, echo=echo)
    db = create_engine(f"sqlite:///D:\\xtrade\\db\\xtrade_{a}.db", echo=echo)


   # print("sqlite:///D:\\xtrade\\db\\xtrade_" + a)
    # "D:\xtrade\db\xtrade_N.db"


    print(db)
    dbs.append(db)           
    Base.metadata.create_all(db)
    Session = sessionmaker()
    Session.configure(bind=db)
    sessions.append(Session())
#    session = Session()
print(f"db engine and session created for multiple databases to folder {folder}")



def session_chooser(symbol:str):
    selector = symbol[0].upper()
#    print("session for",symbol,"is",selector,", database is", )
    index = alphabets.index(selector)
    session = sessions[index]
    print("session for",symbol,"is",selector,", database is", db_name(selector))
    return session
 
def session_list():
    return sessions 


