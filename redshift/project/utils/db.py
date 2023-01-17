import psycopg2
import pandas as pd
from config import database_connection as dbc
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import sqlparse


engine = create_engine(URL.create(
    dbc.drivername,
    username=dbc.username,
    password=dbc.password,  # plain (unescaped) text
    host=dbc.hostname,
    port = dbc.port,
    database=dbc.database
))

# public variable, used in IAM argument for queries requiring redshift IAM
redshift_iam = dbc.redshift_iam 


def do_query(query : str, args : list ) :
    """
    execute postgresql query via postcopg2 on database configured in config/database_connection.py

    :param query: postgresql query string 
    :param args: arguments for query string (see https://www.psycopg.org/docs/usage.html#the-problem-with-the-query-parameters)
    :return: pandas DataFrame for query result
    :rtype: pd.DataFrame
    """
    conn = psycopg2.connect( host=dbc.hostname, user=dbc.username, password=dbc.password, dbname=dbc.database, port=dbc.port )
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



def do_frameToTable(frame, table_name : str, schema : str) :
    """
    execute postgresql query to apend schema.table_name with frame data on connected database

    :param frame: pandas frame that will overwrite schema.table_name
    :param table_name: name of table to be overwritten
    :param schema: name of schema that table_name is in 
    :param if_exists: {‘fail’, ‘replace’, ‘append’} -- pandas to_sql parameter, defines behavior for frameToTable
    :return: None or Int
    :rtype: None if rows not returned, Int equal to number of rows affected (if integer returned for rows by sqlalchemy)
    """
    # CLOSE THE ENGINE???
    return frame.to_sql(table_name, con = engine, schema=schema, if_exists='append', index=False, method='multi')



def do_query_file(filePathname : str, args : list):
    """
    execute postgresql queries from filePathname on connected database

    :param filePathname: string including path/filename (i.e. 'sql/ct_testTable.sql')
    :return: result of last query in file in pandas dataframe
    :rtype: pandas dataframe
    """
    conn = psycopg2.connect( host=dbc.hostname, user=dbc.username, password=dbc.password, dbname=dbc.database, port=dbc.port )
    cur = conn.cursor()
    frame = None
    with open(filePathname, encoding = 'utf8') as f:
        stripped = sqlparse.format(f.read(), strip_comments=True).strip() 
        ret = cur.execute(stripped, args)
        print(filePathname + ' done.')
    if cur.description : # checks for returned table (if it has a description, the query has returned a table)
        data = cur.fetchall()
        column_names = [i[0] for i in cur.description]
        frame = pd.DataFrame(data, columns=column_names)
    cur.close()
    conn.commit()   
    conn.close()
    return frame



def loadFromCSV(filePathName : str, table_name : str, schema : str):
    """
    execute postgresql query to append schema.table_name with frame data on connected database; 
    frame is filled by data in filePathName csv file

    :param filePathName: csv filename to fill the pandas table with
    :param table_name: name of table to be overwritten
    :param schema: name of schema that table_name is in 
    :param dtype: dict of data types for each column (defaults to varchar if nothing entered)
    :param if_exists: {‘fail’, ‘replace’, ‘append’} -- pandas to_sql parameter, defines behavior for frameToTable
    :return: None or Int
    :rtype: None if rows not returned, Int equal to number of rows affected (if integer returned for rows by sqlalchemy)
    """
    frame = pd.read_csv(filePathName)
    return do_frameToTable(frame, table_name, schema)