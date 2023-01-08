import psycopg2
import pandas as pd
from config import database_connection as dbc
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


query = 'SELECT * FROM lkp_data.lkp_state_details'
conn = psycopg2.connect( host='localhost', user='postgres', password='password', dbname='cards_ingest', port=5432 )
cur = conn.cursor()
cur.execute( query, [] )
data = cur.fetchall()
column_names = [i[0] for i in cur.description]
frame = pd.DataFrame(data, columns=column_names)
frame.to_csv('lkp_state_data.csv', index = False)
