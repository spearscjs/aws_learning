import psycopg2
import pandas as pd
from config import database_connection as dbc
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL



def do_query( query : str, args : list ) :
    """
    execute postgresql query on connected database

    :param query: postgresql query string 
    :param args: arguments for query string (see https://www.psycopg.org/docs/usage.html#the-problem-with-the-query-parameters)
    :return: pandas DataFrame for query result
    :rtype: pd.DataFrame
    """
    # 1. Connect to sql database using python.
    conn = psycopg2.connect( host=dbc.hostname, user=dbc.username, password=dbc.password, dbname=dbc.database )
    cur = conn.cursor()
    cur.execute( query, args )
    # pandas frame
    frame = None
    
    if cur.description : # checks for returned table (if it has a description, the query has returned a table)
        data = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        frame = pd.DataFrame(data, columns=column_names)
    cur.close()
    conn.commit()   
    conn.close()
    
    return frame



def do_frameToTable(frame, table_name : str, schema : str, if_exists = 'replace') :
    """
    execute postgresql query to replace schema.table_name with frame data on connected database

    :param frame: pandas frame that will overwrite schema.table_name
    :param table_name: name of table to be overwritten
    :param schema: name of schema that table_name is in 
    :param if_exists: {‘fail’, ‘replace’, ‘append’} -- pandas to_sql parameter, defines behavior for frameToTable
    :return: None or Int
    :rtype: None if rows not returned, Int equal to number of rows affected (if integer returned for rows by sqlalchemy)
    """
    r = url_object = URL.create(
        'postgresql',
        username=dbc.username,
        password=dbc.password,  # plain (unescaped) text
        host=dbc.hostname,
        port = dbc.port,
        database=dbc.database
    )
    engine = create_engine(url_object)
    return frame.to_sql(table_name, con = engine, schema=schema, if_exists=if_exists, index=False)
