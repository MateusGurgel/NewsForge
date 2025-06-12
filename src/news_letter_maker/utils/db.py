import os
from sqlalchemy import create_engine

from news_letter_maker.utils.env import CONSUMPTION_DATABASE_CONNECTION_STRING

def get_db_engine():
    print(CONSUMPTION_DATABASE_CONNECTION_STRING)
    return create_engine(CONSUMPTION_DATABASE_CONNECTION_STRING)
