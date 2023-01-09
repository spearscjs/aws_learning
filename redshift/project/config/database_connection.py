# # before logging in see https://www.postgresql.org/docs/8.1/libpq-pgpass.html
import os

is_redshift_conn = False # boolean, use redshift? else use localhost 


if is_redshift_conn:
    hostname = os.environ.get('REDSHIFT_HOSTNAME')
    username = os.environ.get('REDSHIFT_USERNAME')
    password = os.environ.get('REDSHIFT_PASSWORD')
    database = os.environ.get('REDSHIFT_DATABASE')
    port = os.environ.get('REDSHIFT_PORT')
    drivername = 'postgresql'
else:
    hostname = 'localhost'
    username = 'postgres'
    password = 'password'
    database = 'cards_ingest'
    port = '5432'
    drivername = 'postgresql'

