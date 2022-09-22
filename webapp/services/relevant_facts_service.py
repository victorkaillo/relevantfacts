# import sys
# from pathlib import Path
# file = Path(__file__).resolve()
# package_root_directory = file.parents[1]
# sys.path.append(str(package_root_directory))

import logging
import os
import traceback
from datetime import datetime  # TODO
from time import sleep
import pandas as pd
import pytz
from environs import Env
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from seleniumConfig import getDriver
from webapp.services.storage_connection_service import upload_file
from webapp.services.sql_server_connection_service import execute
from bs4 import BeautifulSoup
from webapp.services.sql_server_connection_service import getInstance, executeDeleteHoje, executeDeleteAno, executeDeleteWeek

env = Env()
env.read_env()

def tableRelevantFactsWeek():
    try:
        if datetime.now().day < 5:
            return
        executeDeleteWeek()
        # datetime.day(5)

        driver = getDriver()
        connection = getInstance(True)
        urlLogin = "https://estadaori.estadao.com.br/?s=&setor_de_atividade=&datas=dia&post_type=fatos_relevantes"
        relevantFacts = crawlerByPage(driver,connection,5,True)
        
        # relevantFacts.to_sql('RelevantFacts', con=connection, if_exists='append', index=False)
        # connection.close()
        
        # relevantFacts.to_csv('relevant_facts.csv')
        driver.quit()
        del driver

        return relevantFacts
    except Exception as err:
        pass


def tableRelevantFactsToday():
    try:
        # executeDeleteHoje()

        driver = getDriver()
        urlLogin = "https://estadaori.estadao.com.br/?s=&setor_de_atividade=&datas=dia&post_type=fatos_relevantes"
        relevantFacts = crawlerByUrl(driver,urlLogin)
        print("crawler sucess")
        relevantFactsByFilter = pd.read_json('relevant_facts.json', orient="index")
        print("load file sucess")
        df =  pd.concat([relevantFacts,relevantFactsByFilter]).drop_duplicates().reset_index(drop=True)
        print("concat sucess")


        
        # connection = getInstance(True)
        # relevantFacts.to_sql('RelevantFacts', con=connection, if_exists='append', index=False)
        # connection.close()
        
        # relevantFacts.to_csv('relevant_facts.csv')
        
        driver.quit()
        del driver

        return df
    except Exception as err:
        pass

def tableRelevantFacts(pageNumber=110):
    try:
        # LogService.sendLog(success=True, job="tableRelevantFacts", source="kinvo.crawler.ri",
        #             textMessage=f"STARTING GET tableRelevantFacts DATA", level="INFO", environment="PRODUCTION")
        
        lista_de_dataframes = []
        driver = getDriver()
        connection = getInstance(True)
        pageNumber = pageNumber
        lista_de_dataframes = crawlerByPage(driver,connection,pageNumber)
        print(f'Quantidade de dfs: {len(lista_de_dataframes)}')

        relevantFacts =  pd.concat(lista_de_dataframes).drop_duplicates(keep='last').reset_index(drop=True)
        # connection = getInstance(True)
        # relevantFacts.to_sql('RelevantFacts', con=connection, if_exists='replace', index=False)
        
        print(relevantFacts)
        # transformar DATA DA PUBLICACAO 22/10/2021 --> 2021-10-22 usar .str e fazer no dataframe

        relevantFacts.to_json('relevant_facts.json', orient="records")
        upload_file(file_system='relevantfacts', file_name=f'relevant_facts.json')
        # os.remove('fluxo_de_caixa_mensal.json')
        # os.remove(pathDownloadedFile)
        connection.close()
        driver.quit()
        del driver
        # executeDeleteAno()
        # anoAnterior = int(datetime.now().year) - 2
        # executeDeleteAno(anoAnterior=anoAnterior)
    except Exception as err:
        print(err)
        driver.quit()
        del driver
        
    return None

def crawlerByPage(driver,connection,pageNumber,week=False):
    lista_de_dataframes = []

    for pagina in range(pageNumber,0,-1):
        # driver = getDriver()
        if week:
            urlLogin = f"https://estadaori.estadao.com.br/page/{pagina}/?s&setor_de_atividade&datas=semana&post_type=fatos_relevantes#038;setor_de_atividade&datas=semana&post_type=fatos_relevantes"
        else:
            urlLogin = f"https://estadaori.estadao.com.br/fatos_relevantes/page/{pagina}/"
        # 
        ## urlLogin = "https://estadaori.estadao.com.br/?s=&setor_de_atividade=&datas=dia&post_type=fatos_relevantes"
        relevantFacts = crawlerByUrl(driver,urlLogin)
        # relevantFacts.to_sql('RelevantFacts', con=connection, if_exists='append', index=False)
        lista_de_dataframes.append(relevantFacts)

    
    # pathDownloadedFile = 'downloads/....csv'
        
    return lista_de_dataframes

def crawlerByUrl(driver,urlLogin):
    for retry in range(10):
        try:
            print(f'started page: {urlLogin[33:]}')
            driver.get(urlLogin)
            sleep(1)
            element = driver.find_element_by_xpath('//*[@id="content"]/section[3]/table[1]')
            html_content = element.get_attribute('outerHTML')
            soup = BeautifulSoup(html_content, 'html.parser')
            table = soup.find(name='table')
            df = pd.read_html(str(table))[0]
            table_parse = str(table).split("<a ")
            links_lista = []
            for tag_a in table_parse:
                if '<table class="table"' in tag_a:
                    continue
                links_lista.append(tag_a.split('href="')[1].split('" target')[0])
            df["Publicação"] = links_lista

            tz = pytz.timezone("Brazil/East")
            date_agora = datetime.now(tz)
            date_list = []
            for _ in range(0, len(df.index)):
                date_list.append(str(date_agora.strftime("%Y-%m-%d %H:%M:%S")))
            df['dataExecucao'] = date_list
            ## altera colunas
            columnsName = df.columns
            oldLastColumn = columnsName[-1]
            lastColumn = columnsName[-2]
            newColumns = list(columnsName[:-2])
            newColumns.append(oldLastColumn)
            newColumns.append(lastColumn)
            df = df[newColumns]
            df.columns = ['Company', 'Sector', 'Type', 'PubDate', 'CreationDate', 'Link']
            df['PubDate'] = pd.to_datetime(df['PubDate'], dayfirst=True, format="%d/%m/%Y")
            df.PubDate.dt.strftime("%Y-%m-%d")
            print(df['PubDate'])

            df = df.drop_duplicates(keep='last').reset_index(drop=True)
            # driver.quit()
            # del driver
            # driver = getDriver()
            # df.to_sql('RelevantFacts', con=connection, if_exists='append', index=False)
            # cast(CreatedDate as date) = CAST(getdate() as date)
            
            print(df.head())
            print(f'success: {urlLogin[33:]}')
            return df

        except Exception as err:
            print(err)
            if retry > 7:
                logging.error(err)
            else:
                logging.warn(err)
            driver.quit()
            del driver
            driver = getDriver()

def insertRelevantFacts(Company,Sector,Type,PubDate,CreationDate,Link):
    sql_query = "INSERT INTO [RelevantFacts]([Company],[Sector],[Type],[PubDate],[CreationDate],[Link]) VALUES('[VIBRA ENERGIA S.A.]','[Comércio (Atacado e Varejo)]','[Fato relevante]',[20/07/2022],'2022-07-20-11:42:18','https://www.rad.cvm.gov.br/ENET/frmExibirArquivoIPEExterno.aspx?NumeroProtocoloEntrega=999099');"
    # 0,VIBRA ENERGIA S.A.,Comércio (Atacado e Varejo),Fato relevante,20/07/2022,https://www.rad.cvm.gov.br/ENET/frmExibirArquivoIPEExterno.aspx?NumeroProtocoloEntrega=999099,2022-07-20-11:42:18

    #     ('{Company}','{Sector}','{Type}',{PubDate},{CreationDate},'{Link}'),
#     ('{Company}','{Sector}','{Type}',{PubDate},{CreationDate},'{Link}');
    # CREATE TABLE [RelevantFacts]
    # ( [Company] varchar(100),
    # [Sector] varchar(100), 
    # [Type] varchar(50),
    # [PubDate] Date,
    # [CreationDate] DateTime,
    # [Link] nvarchar(1000));
        # WITH cte AS (SELECT Company, Sector,Link, ROW_NUMBER() OVER (PARTITION BY Company, Link ORDER BY CreationDate) row_num FROM [RelevantFacts]) DELETE FROM cte WHERE row_num > 1;
