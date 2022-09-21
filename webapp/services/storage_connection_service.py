import logging
from azure.storage.filedatalake import DataLakeServiceClient
from environs import Env
import pandas as pd
import traceback
import json
import os

#TODO
env = Env()
env.read_env()


try:  
    global service_client
    service_client = DataLakeServiceClient(account_url=env.str("AZURE_STORAGE_ACCOUNT_URL"),
                                           credential=env.str("AZURE_STORAGE_CREDENTIAL"))

except Exception as err:
    pass

def upload_file(file_system="compartilhamento", file_name='relevant_facts.json', directory='relevantfacts'):
    # try:
    # "https://kaillodatalake.blob.core.windows.net/relevantfacts?si=relevantfacts&sv=2021-06-08&sr=c&sig=Mil39rExAP9vRF2edwlB2qjhVqxyjlBNIpqkD6%2FlY0U%3D"
        file_system_client = service_client.get_file_system_client(file_system=str(file_system))
        if directory != None:
            print("create_file")
            file_client = file_system_client.create_file(f"{directory}/{file_name}")
        else:
            print("create_file")
            file_client = file_system_client.create_file(f"{file_name}")
        
        print("created file")
        
        localFile = open(f"{file_name}",'rb')

        file_contents = localFile.read()
        
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

        logging.info("sucess upload_file")
    # except Exception as err:
    #     pass


def downloadFile(file_system, file_name, directory=None):
    try:
        file_system_client = service_client.get_file_system_client(file_system=str(file_system))
        if directory != None:
            fileClient = file_system_client.get_file_client(f"{directory}/{file_name}")
        else:
            fileClient = file_system_client.get_file_client(f"{file_name}")
            
        download = fileClient.download_file()
        downloaded_bytes = download.readall()

        logging.info("sucess download_file")

        return downloaded_bytes
    except Exception as err:
        pass



def updateFile(newData, columnID, columnDate, directory, fileNameStr):
    none_file = False
    try:
        newDf = pd.DataFrame(newData)
        try:
            donwloadStorage = json.loads(downloadFile(file_system='financial', directory=directory, file_name=fileNameStr))
            oldDf = pd.DataFrame(donwloadStorage)
            
            df = pd.concat([oldDf,newDf], ignore_index=True, sort=False)\
                .sort_values(columnDate).drop_duplicates(subset=columnID, keep='last')
        except json.decoder.JSONDecodeError:
            print('None file')
            none_file = True
            newDf.to_json(fileNameStr, orient="records")
            upload_file(file_system='financial', directory=directory, file_name=fileNameStr)
            os.remove(fileNameStr)
        except Exception as err:
            pass
        # FOR FIRST NEW TABLES
        # df = newDf
        if not none_file:
            df.to_json(fileNameStr, orient="records")
            upload_file(file_system='financial', directory=directory, file_name=fileNameStr)

            os.remove(fileNameStr)
            
            
    except Exception as err:
        pass