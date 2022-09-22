from multiprocessing import Process
from threading import Thread

import uvicorn
from environs import Env
from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from webapp.services.relevant_facts_service import (
    tableRelevantFacts,
    tableRelevantFactsToday,
)
from webapp.services.sql_server_connection_service import (
    executeDeleteAno,
    getByFilterjson,
    getAllRelevantFactsjson,
)
from webapp.utils.util import handlerToken

env = Env()
env.read_env()

app = FastAPI()


def threadTableRelevantFacts():
    x = Process(target=tableRelevantFacts)
    x.start()
    x.join()
    del x


@app.get("/")
def root():
    return {"Version": "0.0.3"}


@app.get("/getTableRelevantFacts")
def getTableRelevantFacts(request: Request):
    isValid, message = handlerToken(request.headers.get("Authorization"))
    if isValid:
        x = Thread(target=threadTableRelevantFacts)
        x.start()
        return {"message": "Running the task"}
    else:
        return message


@app.get("/getTableRelevantFactsToday")
def getTableRelevantFactsToday(request: Request):
    isValid, message = handlerToken(request.headers.get("Authorization"))
    if isValid:
        x = Thread(target=tableRelevantFactsToday)
        x.start()
        return {"message": "Running the task"}
    elif not isValid:
        return message


@app.get("/getExecuteDeleteAno")
def getExecuteDeleteAno(request: Request):
    isValid, message = handlerToken(request.headers.get("Authorization"))
    if isValid:
        x = Thread(target=executeDeleteAno)
        x.start()
        return {"message": "Running the task"}
    elif not isValid:
        return message


@app.get("/getByFilterRelevantFacts")
def getByFilterExecute(request: Request, startDate: str, endDate: str):
    isValid, message = handlerToken(request.headers.get("Authorization"))
    if isValid:
        print(f"startDate: {startDate}, endDate: {endDate}")
        relevantFactsjson = getByFilterjson(startDate, endDate)
        json_compatible_item_data = jsonable_encoder(relevantFactsjson)
        return JSONResponse(content=json_compatible_item_data)
        # return {'startDate': startDate, 'endDate': endDate}
    elif not isValid:
        return message


@app.get("/getAllRelevantFacts")
def getAllExecute(request: Request):
    isValid, message = handlerToken(request.headers.get("Authorization"))
    if isValid:
        relevantFactsjson = getAllRelevantFactsjson()
        json_compatible_item_data = jsonable_encoder(relevantFactsjson)
        return JSONResponse(content=json_compatible_item_data)
        # return {'startDate': startDate, 'endDate': endDate}
    elif not isValid:
        return message


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=80)
