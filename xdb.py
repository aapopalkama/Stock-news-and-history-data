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

import os
for a in alphabets:
    db = create_engine("sqlite:///" + folder + "/" +  db_name(a),echo=echo)

    print(db)
    dbs.append(db)           
    Base.metadata.create_all(db)
    Session = sessionmaker()
    Session.configure(bind=db)
    sessions.append(Session())

print(f"db engine and session created for multiple databases to folder {folder}")



def session_chooser(symbol:str):
    selector = symbol[0].upper()
    index = alphabets.index(selector)
    session = sessions[index]
    print("session for",symbol,"is",selector,", database is", db_name(selector))
    return session
 
def session_list():
    return sessions 


