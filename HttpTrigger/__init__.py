import logging

import azure.functions as func

import os
import pyodbc
import struct
import _thread


def run_query( threadName):

    server="your-sqlserver.database.windows.net"
    database="your_db"
    driver="{ODBC Driver 17 for SQL Server}"
    query="SELECT * FROM users waitfor delay '00:00:05:10'"
    username = 'username'
    password = 'password'
    db_token = 'yourtoken'
    connection_string = 'DRIVER='+driver+';SERVER='+server+';DATABASE='+database

    if os.getenv("MSI_SECRET"):
        conn = pyodbc.connect(connection_string+';Authentication=ActiveDirectoryMsi')
        
    else:
        SQL_COPT_SS_ACCESS_TOKEN = 1256

        exptoken = b''
        for i in bytes(db_token, "UTF-8"):
            exptoken += bytes({i})
            exptoken += bytes(1)

        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken
        # conn = pyodbc.connect(connection_string, attrs_before = { SQL_COPT_SS_ACCESS_TOKEN:tokenstruct })
        conn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

    cursor = conn.cursor()
    cursor.execute(query) 
    row = cursor.fetchone()
    while row:
        print(row[0])
        row = cursor.fetchone()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        _thread.start_new_thread(run_query, ("Thread-1", ) )
    except:
        print ("Error")
    
    return func.HttpResponse(
            'Accepted',
            status_code=202
    )
