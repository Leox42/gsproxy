import os
import re
import mysql.connector as connection
from Proxy.src.sbcommlib import *
import swat

def data_source_type_check(source, dataStoragePath):
    if os.path.isfile(dataStoragePath + source):
        return 'filesystem'
    elif source[:5] == 'mySQL':
        return 'mySQL'
    elif source[:7] == 'SASViya':
        return 'SASViya'
    else:
        raise ValueError('Unable to retrieve data')

def data_remote_retrieval(source):
    print(source)
    pass

def data_SAS_Viya_retrieval(source, archivePath):
    os.environ['CAS_CLIENT_SSL_CA_LIST'] = archivePath+'/trustedcerts.pem'
    connectionID, source = source.split(':')
    usr, pwd = read_connection_details(connectionID)
    caslib, table = source.split('/')
    conn = swat.CAS('path/',443,usr,pwd,protocol='https')
    tbl = conn.CASTable(table, caslib=caslib.replace("\\",""))
    data = tbl.to_frame()
    return data


def read_connection_details(connectionID):
    if connectionID[:5] == "mySQL":
        return "IP", 'usr', 'pwr'
    elif connectionID[:7] =='SASViya':
        return 'usr', 'pwr'
    else:
        return "no available connection!"

def mySQL_retrieval(source, query):
    connectionID, db = source.split(':')
    query = "Select * from person;"
    print(connectionID, db)
    host, user, passwd = read_connection_details(connectionID)
    mydb = connection.connect(host=host, database = db,user=user, passwd=passwd,use_pure=True)
    df = pd.read_sql(query,mydb)
    mydb.close()
    return df

def data_retrieval(dataStoragePath, archivePath, source, query, task, url, messageLogFolder):

    dataSourceType = data_source_type_check(source, dataStoragePath)

    if dataSourceType =='filesystem':
        data = pd.read_csv(dataStoragePath+source)
        query = re.sub("\\band\\b", "&", query)
        query = re.sub("\\bor\\b", "|", query)
        query = re.sub("\\b##", "", query)
        query = re.sub("##\\b", "", query)
        try:
            if query != "":
                data = data.query(query)
        except:
            print("Unsupported query! Using the whole datamart")
            send_status_message(task, "info.status.job", "Unsupported query! Using the whole datamart", url,
                                messageLogFolder)
    elif dataSourceType == 'mySQL':
        data = mySQL_retrieval(source, query)
    elif dataSourceType == 'SASViya':
        data = data_SAS_Viya_retrieval(source, archivePath)
    return data