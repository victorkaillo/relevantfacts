import pandas as pd
import urllib
from sqlalchemy import create_engine
from datetime import datetime 
import traceback
import json

from webapp.repositories.relevant_facts_repository import getByFilter, getAll
from kinvolog import LogService
from environs import Env
env = Env()
env.read_env()

def getAllRelevantFactsjson():
    sql = getAll()
    relevantFactsByFilter = execute(sql)
    result = json.loads(relevantFactsByFilter.to_json(orient="index"))
    return result

def getByFilterjson(startDate, endDate):
    sql = getByFilter(startDate, endDate)
    relevantFactsByFilter = execute(sql)
    result = json.loads(relevantFactsByFilter.to_json(orient="index"))
    return result

def execute(sql):
    try:
        print(sql)
        # params = urllib.parse.quote_plus(f"{env.str('DRIVER')}")
        # engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
        # con = engine.connect()
        # oldDf = pd.read_sql_query('SELECT * FROM dbo.[AI.MiningNews]', con=con)

        connection  = getInstance()
        # print(con)
        df = pd.read_sql(sql, connection)
        
        LogService.sendLog(success=True, job=f"Executar SQL: {sql}", source="kinvo.crawler.ri",
            textMessage="Success in read_sql to dataframe", level="INFO", environment="PRODUCTION", details=traceback.format_exc())

        connection.close()
        
        return df
    except Exception as e:
        LogService.sendLog(success=False, job=f"Executar SQL: {sql}", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())

def executeDeleteAno(tablename='[RelevantFacts]',anoAnterior=0):
    try:
        con  = getInstance(True)
        anoAnterior = anoAnterior or int(datetime.now().year) - 1
        con.execute(f"DELETE FROM {tablename} WHERE (DATEPART(yy, PubDate) = {anoAnterior})")
        con.close()
    except Exception as e:
        LogService.sendLog(success=False, job=f"Deletar da tabela SQL: {tablename}", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())

def executeDeleteWeek():
    weekDay = int(datetime.now().isoweekday()) - 1
    con  = getInstance(True)
    for day in range(weekDay,-1,-1):
        con.execute(
            f"DELETE FROM [RelevantFacts] WHERE (DATEPART(yy, PubDate) = {int(datetime.now().year)}) "
            f"and (DATEPART(mm, PubDate) = {int(datetime.now().month)}) "
            f"and (DATEPART(dd, PubDate) = {int(datetime.now().day) - day})"
        )
    con.close()

def executeDeleteHoje(tablename='[RelevantFacts]'):
    try:
        sql = "WITH cte AS (SELECT Company, Sector,Link, ROW_NUMBER() OVER (PARTITION BY Company, Link ORDER BY CreationDate) row_num FROM [RelevantFacts]) DELETE FROM cte WHERE row_num > 1;"
        con  = getInstance(True)
        con.execute(
            f"DELETE FROM {tablename} WHERE (DATEPART(yy, PubDate) = {int(datetime.now().year)}) "
            f"and (DATEPART(mm, PubDate) = {int(datetime.now().month)}) "
            f"and (DATEPART(dd, PubDate) = {int(datetime.now().day)})"
        )
        con.close()
    except Exception as e:
        LogService.sendLog(success=False, job=f"Deletar da tabela SQL: {tablename}", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())

def executeDeleteHojeAnoAnterior(tablename='[RelevantFacts]'):
    try:
        if int(datetime.now().day)==29 and int(datetime.now().month)==2:
            return
        con  = getInstance(True)
        con.execute(
            f"DELETE FROM {tablename} WHERE (DATEPART(yy, PubDate) = {int(datetime.now().year)-1}) "
            f"and (DATEPART(mm, PubDate) = {int(datetime.now().month)}) "
            f"and (DATEPART(dd, PubDate) = {int(datetime.now().day)})"
        )
        con.close()
    except Exception as e:
        LogService.sendLog(success=False, job=f"Deletar da tabela SQL: {tablename}", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())

def executeDeleteRepeated():
    # try:
        sql_query = "WITH cte AS (SELECT Company, Sector,Type,Link, ROW_NUMBER() OVER (PARTITION BY Company, Link ORDER BY Company, Link) row_num FROM [RelevantFacts]) DELETE FROM cte WHERE row_num > 1;"
        con  = getInstance(True)
        con.execute(sql_query)
        con.close()
    # except Exception as e:
    #     LogService.sendLog(success=False, job=f"Deletar da tabela SQL", source="kinvo.crawler.ri",
    #     textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())
   
#    

def executeInsert(sql, values):
    try:
        con  = getInstance(True)

        con.execute(sql, values[0], values[1], values[2], values[3])

        con.close()
    except Exception as e:
        LogService.sendLog(success=False, job=f"Executar insert {sql} com os valores {values}", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())

def getInstance(envProd=False):
    try:
        if envProd:
            # params = urllib.parse.quote_plus(env.str("SQLSERVERKINVDB"))
            params = urllib.parse.quote_plus("DRIVER={ODBC Driver 17 for SQL Server};SERVER=tcp:kinv.database.windows.net;DATABASE=kinv;UID=kinvusr_datalake;PWD=G5tGUdJu9gYkbVuW;Trusted_connection=no")
        else:
            params = urllib.parse.quote_plus(env.str("DRIVE"))
        engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
        connection = engine.connect()
        LogService.sendLog(success=True, job="Instanciar banco de dados", source="kinvo.crawler.ri",
        textMessage="DATABASE connect SUCCESS", level="INFO", environment="PRODUCTION", details=traceback.format_exc())
# "INSERT INTO [dbo].[RelevantFacts]([Company],[Sector],[Type],[PubDate],[CreationDate],[Link]) VALUES('VIBRA ENERGIA S.A.','Comércio (Atacado e Varejo)','Fato relevante','07/20/2022','2022-07-20 13:06:40','https://www.rad.cvm.gov.br/ENET/frmExibirArquivoIPEExterno.aspx?NumeroProtocoloEntrega=999099')"
        return connection
    except Exception as e:
        print(e)
        LogService.sendLog(success=False, job="Instanciar banco de dados", source="kinvo.crawler.ri",
        textMessage=str(e), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())
