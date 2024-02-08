import pathlib
from time import sleep
from APIsMaker.TadaBaseAPIs.APITableConfig import GenericTableId
from DataModels.TadaBase.TadaBaseBulkAction import TadaBaseGenericBulkCreate, TadaBaseGenericBulkDelete, TadaBaseGenericBulkUpdate, TadaBaseGenericBulkUpdateCreate, TBBulkActionChildInsert, TBBulkCount, TBBulkActionUpdateCreaeResponse, TBBulkActionUpdateResponse, TBBulkCount, TadaBaseGenericBulkSync, TBBulkActionResponse
from APIsMaker.TadaBaseAPIs.APIAppConfig import BESTrainingHeadersConfig
from APIsMaker.TadaBaseAPIs.APISchema import InsertRecordsSchema, UpdateRecordsSchema, DeleteRecordsSchema
import requests
from requests import ReadTimeout
import time
from Mappers.GeneralMapper import create_tadabase_insert_payload, create_tadabase_post_payload
from DAOs.Redis.Connection import RedisHandler
from fastapi import HTTPException
import concurrent.futures
from re import sub
from APIsMaker.RedisAPIs import KeySchema
from Validator.DuplicationValidator import checkDuplicationField
from typing import Union
import json
from Routers.GENs.RunningGenerator import generate_running_id
import asyncio
import importlib.util
from Routers.SYNCs.SYNCGrant import syn_grant_ssg_tb
from DAOs.MongoDB.errorLogger import errorLogger
from DAOs.MongoDB.APILogger import APILogger
from APICallCounter.APICallCounter import APICallCounter

class TadaBaseBulkActionDao:
    ErrorLogger = errorLogger()
    ApiLogger = APILogger()
    APICounter = APICallCounter('TB_API_Call')

    def __init__(self, target: str):
        self.pydanicModel = self.getFiles('DataModels/TadaBase', f'TB{self._camelCase(target)}', f'{self._camelCase(target)}Equation')

    tableValidation = {
        "Attendance and Assessment": {'keys': {"session": "session", "attendeeNric": "attendee Nric"}, "returnField": []},
        "Registration Enrolment Grant": {'keys': {"courseRun": "course Run", "booking": "booking"}, "returnField": ["trainee Id"]},
        "Trainee": {'keys': {"traineeId": "trainee Id", "employer": "employer"}, "returnField": []}
    }
    
    runningNumber = {
        'Attendance and Assessment': {'target': 'Attendance', 'runningNumberKey': 'attendanceRunningNumber', 'runningNumberWithPrefixKey': 'attendanceId'},
        'Registration Enrolment Grant': {'target': 'Registration', 'runningNumberKey': 'registrationRunningNumber', 'runningNumberWithPrefixKey': 'externalRegistrationNumber'}
    }
    
    updateRunningNumber = {
        'Attendance and Assessment': {'target': 'Assessment', 'runningNumberKey': 'assessmentRunningNumber', 'runningNumberWithPrefixKey': 'assessmentReferenceNumber'},
        'Registration Enrolment Grant': {'target': 'Enrolment', 'runningNumberKey': 'enrolmentRunningNumber', 'runningNumberWithPrefixKey': 'enrolmentReferenceNumber'}
    }


    def getFiles(self, folder_name, module_name, modelName):
        try:
            # Construct the full module/file path
            file_path = f"{folder_name}/{module_name}.py"

            # Load the module using importlib
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Now you can access the objects defined in the module
            return getattr(module, f'TB{modelName}', None)
        except:
            return None
        
    def _camelCase(self, string) -> str:
        s = sub(r"( )+", " ", string).title().replace(" ", "")
        return ''.join([string[0], s[1:]])

    def _nameReformat(self, stringName: str) -> str:
        s = sub(r"( )+", " ", stringName).replace(" ", "")
        return ''.join([stringName[0], s[1:]])
    
    def _checkJsonExists(self, key) -> bool:
        return RedisHandler.exists(key)

    def _compareUpdatedJson(self, redisJson: dict, postPayload: dict) -> dict:
        for k in redisJson.keys():
            if postPayload.get(k) is not None:
                redisJson[k] = postPayload.get(k)
        
        # print('redisJson: ', redisJson)
        return redisJson

    def _getEmployerRecordId(self, employerUen: str) -> str:
        if employerUen:
            indexName = "tadabase:employer"
            response = RedisHandler.ft(indexName).search(employerUen).docs
            if response:
                return json.loads(response[0].json)['recordId']
        
        return None

    def _updateRecordToRedis(self, redisKey: str, payloadObj: dict) -> dict:
        try:

            RedisHandler.json().set(redisKey, ".", payloadObj)

            return {'recordId': payloadObj.get('recordId'), 'msg': 'insert success', 'status_code': 200}
        except Exception as e:
            print('error: ', e)
            return {'recordId': payloadObj.get('recordId'), 'msg': 'insert failed', 'status_code': 400}

    def _checkJsonExists(self, key) -> bool:
        return RedisHandler.exists(key)

    def _insertRecordToRedis(self, redisKey: str, payloadObj: dict) -> dict:
        KeyExists = self._checkJsonExists(redisKey)
        if not KeyExists:
            RedisHandler.json().set(redisKey, ".", payloadObj)

    async def _insertRecordRequest(self, target: str, payloadObj: dict) -> dict:
        headers = BESTrainingHeadersConfig()
        targetTableId = GenericTableId(target)
        redisSchema = getattr(KeySchema, f'TadaBase{self._camelCase(target)}JsonKeySchema', None)
        
        # running number
        isRunningNum = self.runningNumber.get(target)
        if isRunningNum:
            runningNumberResponse = await generate_running_id(isRunningNum.get('target'))
            payloadObj[isRunningNum.get('runningNumberKey')] = runningNumberResponse['runningNumber']
            payloadObj[isRunningNum.get('runningNumberWithPrefixKey')] = runningNumberResponse['runningNumberWithPrefix']
            # payloadObj['createdBy'] = 'DVWQW7GQZ4'
            payloadObj['modifiedBy'] = 'DVWQW7GQZ4'

            # check if pydanicModel to run model equation
            if self.pydanicModel:
                pynaticObj = self.pydanicModel(**payloadObj)
                payloadObj = pynaticObj.dict()
            
        print('inserted payload: ', payloadObj)
        try:
            postPayload = create_tadabase_insert_payload(
            str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
            f"/Schemas/TadaBase/Storage/{self._camelCase(target)}",
            payloadObj)
        except:
            raise HTTPException(status_code=404, detail=f"No such file or directory {target.replace(' ', '')}")

        print('inserted postPayload: ', postPayload)

        APIEndPoint = InsertRecordsSchema(targetTableId)
        retry = 0
        while retry<=5:
            retry +=1
            count, ttl = self.APICounter.retrieve()
            if count >=100:
                print("Wait before you do another request... ")
                time.sleep(ttl)
            try:
                RegistrationResponseBack = requests.request("POST", APIEndPoint, headers=headers, data=postPayload)
                
                if RegistrationResponseBack.status_code == 200:
                    count, ttl = self.APICounter.increment()
                    if isRunningNum:
                        childRecordId = RegistrationResponseBack.json()['recordId']
                        key = redisSchema(childRecordId)
                        
                        payloadObj['recordId'] = childRecordId
                        self._insertRecordToRedis(key, payloadObj)

                    responseObj = {}
                    responseObj = RegistrationResponseBack.json()
                    responseObj['status_code'] = 200
                    log = self.ApiLogger.api(tag="Tadabase API", method="POST", status_code=RegistrationResponseBack.status_code, endpoint=APIEndPoint, input=postPayload, response=responseObj)
                    self.ApiLogger.logAPI(log)
                    return responseObj
                else:
                    if RegistrationResponseBack.status_code == 429 or RegistrationResponseBack.status_code == 504:
                        print("count limit happened ", count," ",  ttl)
                        log = self.ApiLogger.api(tag="Tadabase API",  status_code=RegistrationResponseBack.status_code, method="POST", endpoint=APIEndPoint, input=postPayload)
                        self.ApiLogger.logAPI(log)
                        continue
                    log = self.ApiLogger.api(tag="Tadabase API", status_code=RegistrationResponseBack.status_code, method="POST", endpoint=APIEndPoint, input=postPayload)
                    self.ApiLogger.logAPI(log)
                    return {'status_code': 400, 'detail': 'Bulk insert failed', 'target': target, 'payload': payloadObj}
            except Exception as e:
                error = self.ErrorLogger.error(status_code=500, description="Error while inserting into TB Bulk", extraInfo={"error": str(e),"target": target, "payload": payloadObj})
                self.ErrorLogger.logError(error)

    def _updateDeleteRequest(self, target: str, action: str, insertPayload: dict) -> dict:
        headers = BESTrainingHeadersConfig()
        targetTableId = GenericTableId(target)

        try:
            postPayload = create_tadabase_insert_payload(
            str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
            f"/Schemas/TadaBase/Storage/{self._camelCase(target)}",
            insertPayload)
        except:
            print(f"No such file or directory {target.replace(' ', '')}")
            return 
        retry =0
        while retry<=5:
            retry +=1
            count, ttl = self.APICounter.retrieve()
            if count >=100:
                print("Wait before you do another request... ")
                time.sleep(ttl)
            try:
                if action == 'update':
                    APIEndPoint = UpdateRecordsSchema(targetTableId, insertPayload.get('recordId'))
                    try:
                        RegistrationResponseBack = requests.request("POST", APIEndPoint, headers=headers, data=postPayload, timeout = 5)
                    except ReadTimeout as e:
                        return {'status_code': 200, 'detail': f'Timeout happened during updating TB Table {targetTableId}', 'recordId': insertPayload.get('recordId')}
                else:
                    APIEndPoint = DeleteRecordsSchema(targetTableId, insertPayload.get('recordId'))
                    try:
                        RegistrationResponseBack = requests.request("DELETE", APIEndPoint, headers=headers, data={}, timeout=5)
                    except ReadTimeout as e:
                        return {'status_code': 200, 'detail': f'Timeout happened during updating TB Table {targetTableId}', 'recordId': insertPayload.get('recordId')}

                if RegistrationResponseBack.status_code == 200:
                    responseObj = {}
                    responseObj = RegistrationResponseBack.json()
                    responseObj['status_code'] = 200
                    log = self.ApiLogger.api(tag="Tadabase API", method="POST", status_code=RegistrationResponseBack.status_code, endpoint=APIEndPoint, input=postPayload, response=responseObj)
                    self.ApiLogger.logAPI(log)
                    return responseObj
                else:
                    if RegistrationResponseBack.status_code == 429 or RegistrationResponseBack.status_code == 504:
                        print("count limit happened ", count," ",  ttl)
                        log = self.ApiLogger.api(tag="Tadabase API",  status_code=RegistrationResponseBack.status_code, method="POST", endpoint=APIEndPoint, input=postPayload)
                        self.ApiLogger.logAPI(log)
                        continue
                    log = self.ApiLogger.api(tag="Tadabase API", status_code=RegistrationResponseBack.status_code, method="POST", endpoint=APIEndPoint, input=postPayload)
                    self.ApiLogger.logAPI(log)
                    return {'status_code': RegistrationResponseBack.status_code, 'detail': f'Bulk {action} failed', 'recordId': insertPayload.get('recordId')}
            except Exception as e:
                error = self.ErrorLogger.error(status_code=500, description=f"Error while {action}ing into TB Bulk", extraInfo={"error": str(e),"target": target, "payload": insertPayload})
                self.ErrorLogger.logError(error)

    def checkDuplicate(self, payload: dict, target: str) -> bool:
        duplicateKey = self.tableValidation.get(target)
        if not duplicateKey:
            return False
        
        returnFields = duplicateKey.get('returnField')
        checkFields = {duplicateKey['keys'][k]: v for k, v in payload.items() if k in duplicateKey['keys']}

        if target == 'Registration Enrolment Grant':
            traineeId = RedisHandler.json().get(f'tadabase:trainee:{payload.get("trainee")}')
            checkFields['trainee Id'] = str(traineeId.get('traineeId')) if traineeId else ""

        try:
            results = checkDuplicationField(target, checkFields, returnFields)
            
            if results.get('duplication'):
                return True

            return False
        except:
            return False


    def backgroundUpdateDeleteToTB(self, payload: Union[TBBulkActionUpdateCreaeResponse, TBBulkActionUpdateResponse]):
        updatedRecords = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            concurrentCalls = []

            for record in payload.successUpdateList:
                sleep(0.5)
                concurrentCalls.append(executor.submit(self._updateDeleteRequest, payload.target, payload.action, record))

            for f in concurrent.futures.as_completed(concurrentCalls):
                print('\nTB response back: ', f.result())
                updatedRecords.append(f.result())
        print('updatedRecords: ', updatedRecords)


    def updateCreatePayload(self, payload: TadaBaseGenericBulkUpdateCreate) -> TBBulkActionUpdateCreaeResponse:
        targetTableId = GenericTableId(payload.target)
        redisSchema = getattr(KeySchema, f'TadaBase{self._camelCase(payload.target)}JsonKeySchema', None)

        if not redisSchema or not targetTableId:
            raise HTTPException(status_code=404, detail="Table not found")
        
        duplicatedResponses = []
        successCreatePayload = []
        successUpdatePayload = []
        notFoundPayload = []
        failedPayload = []

        for record in payload.records:

            payloadObj = payload.dict()
            del payloadObj['target']
            del payloadObj['action']
            del payloadObj['records']

            keysList = list(record.dict().keys())
            for key in keysList:
                    payloadObj[key] = record.dict().get(key)

            if payload.target == 'Trainee':
                payloadObj['employer'] = self._getEmployerRecordId(payloadObj.get('employerUen'))
                if not payloadObj.get('employer'):
                    failedPayload.append(payloadObj)
                    continue

            print('updateCreatePayload: ', payloadObj)
            if not payloadObj.get('recordId'):
                del payloadObj['recordId']
                if self.checkDuplicate(payloadObj, payload.target):
                    duplicatedResponses.append({'status_code': 409, 'detail': 'record already exisit', 'payload': payloadObj})
                else:
                    successCreatePayload.append(payloadObj)
            else:
                redisKey = redisSchema(record.recordId)
                key_exists = self._checkJsonExists(redisKey)
                if not key_exists:
                    notFoundPayload.append({'key': redisKey, 'msg': 'no record in redis', 'status_code': 404})
                    continue 

                redisObj = RedisHandler.json().get(redisKey)
                try:
                    insertPayload = create_tadabase_insert_payload(
                    str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
                    f"/Schemas/TadaBase/Storage/{self._camelCase(payload.target)}",
                    payloadObj)

                    postPayload = create_tadabase_post_payload(
                        str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
                        f"/Schemas/TadaBase/Storage/{self._camelCase(payload.target)}",
                        insertPayload)
                except:
                    raise HTTPException(status_code=404, detail=f"No such file or directory {payload.target.replace(' ', '')}")
                
                key = redisSchema(record.recordId)
                updatedJson = self._compareUpdatedJson(redisObj, postPayload)

                redisResponse = self._updateRecordToRedis(redisKey, updatedJson)

                if redisResponse.get('status_code') == 200 or redisResponse.get('status_code') == '200':
                    successUpdatePayload.append(updatedJson)
                elif redisResponse.get('status_code') == 404 or redisResponse.get('status_code') == '404':
                    notFoundPayload.append(updatedJson)
                else:
                    failedPayload.append(updatedJson)

        success = len(successCreatePayload) + len(successUpdatePayload)

        return TBBulkActionUpdateCreaeResponse(target=payload.target, successChildList=successCreatePayload, duplicatedChildList=duplicatedResponses, successUpdateList=successUpdatePayload, bulkCount=TBBulkCount(success=success, duplicated=len(duplicatedResponses), notFound=len(notFoundPayload), failed=len(failedPayload)))


    async def updateDeleteRecord(self, payload: Union[TadaBaseGenericBulkDelete, TadaBaseGenericBulkUpdate]) -> TBBulkActionUpdateResponse:
        targetTableId = GenericTableId(payload.target)
        redisSchema = getattr(KeySchema, f'TadaBase{self._camelCase(payload.target)}JsonKeySchema', None)

        if not redisSchema or not targetTableId:
            raise HTTPException(status_code=404, detail="Table not found")
        
        successUpdatePayload = []
        notFoundPayload = []
        failedPayload = []

        for record in payload.records:

            redisKey = redisSchema(record.recordId)
            key_exists = self._checkJsonExists(redisKey)
            if not key_exists:
                notFoundPayload.append({'key': redisKey, 'msg': 'no record in redis', 'status_code': 404})
                continue 

            redisObj = RedisHandler.json().get(redisKey)

            payloadObj = payload.dict()
            del payloadObj['target']
            del payloadObj['action']
            del payloadObj['records']

            # get unique keys for each record in update action 
            if payload.action == 'update':
                keysList = list(record.dict().keys())
                for key in keysList:
                    payloadObj[key] = record.dict().get(key)

            # run generate_running_id for table in updateRunningNumber dict
            updateRunningNum = self.updateRunningNumber.get(payload.target)
            if updateRunningNum:
                if (payload.target == 'Attendance and Assessment' and payloadObj.get('assessmentRecordStatus') == 'Created') or (payload.target == 'Registration Enrolment Grant' and payloadObj.get('registrationStatus') == 'Enrolled'):
                    runningNumberResponse = await generate_running_id(updateRunningNum.get('target'))
                    payloadObj[updateRunningNum.get('runningNumberKey')] = runningNumberResponse['runningNumber']
                    payloadObj[updateRunningNum.get('runningNumberWithPrefixKey')] = runningNumberResponse['runningNumberWithPrefix']

            updatedJson = self._compareUpdatedJson(redisObj, payloadObj)

            # check if pydanicModel to run model equation and return only keys in dict
            if self.pydanicModel:
                pynaticObj = self.pydanicModel(**updatedJson)
                pydanicDict = pynaticObj.dict()
                updatedJson = pydanicDict.copy()

            print('updatedJson: ', updatedJson)
            try:
                insertPayload = create_tadabase_insert_payload(
                str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
                f"/Schemas/TadaBase/Storage/{self._camelCase(payload.target)}",
                updatedJson)

                postPayload = create_tadabase_post_payload(
                str(pathlib.Path(__file__).parent.parent.parent.resolve()) +
                f"/Schemas/TadaBase/Storage/{self._camelCase(payload.target)}",
                insertPayload)
            except:
                raise HTTPException(status_code=404, detail=f"No such file or directory {payload.target.replace(' ', '')}")
            
            redisResponse = self._updateRecordToRedis(redisKey, postPayload)

            if redisResponse.get('status_code') == 200 or redisResponse.get('status_code') == '200':
                successUpdatePayload.append(postPayload)
            elif redisResponse.get('status_code') == 404 or redisResponse.get('status_code') == '404':
                notFoundPayload.append(postPayload)
            else:
                failedPayload.append(postPayload)

        return TBBulkActionUpdateResponse(action=payload.action, target=payload.target, successUpdateList=successUpdatePayload, bulkCount=TBBulkCount(success=len(successUpdatePayload), notFound=len(notFoundPayload), failed=len(failedPayload)))


    def insertRecord(self, payload: TadaBaseGenericBulkCreate) -> TBBulkActionChildInsert:
        targetTableId = GenericTableId(payload.target)

        if not targetTableId:
            raise HTTPException(status_code=404, detail="Table not found")
        
        duplicatedResponses = []
        successResponses = []

        recordArray = [k for k in payload.dict() if type(payload.dict()[k]) == list]
        if not len(recordArray) > 0:
            raise HTTPException(status_code=404, detail="Bulk not found")
        else:
            recordArray = recordArray[0]

        for runRecord in payload.__dict__.get(recordArray):

            nestedArrays = [k for k in runRecord if type(runRecord[k]) == list]

            if not len(nestedArrays) > 0:
                keysList = list(runRecord.keys())

                payloadObj = payload.dict()
                del payloadObj['target']
                del payloadObj['action']
                # payloadObj['createdBy'] = 'DVWQW7GQZ4'
                # payloadObj['modifiedBy'] = 'DVWQW7GQZ4'
                payloadObj[recordArray] = runRecord.get('recordId')

                for key in keysList:
                    if key != 'recordId':
                        payloadObj[key] = runRecord.get(key)
            
                if self.checkDuplicate(payloadObj, payload.target):
                    duplicatedResponses.append({'status_code': 409, 'detail': 'record already exisit', 'payload': payloadObj})
                else:
                    successResponses.append(payloadObj)
            else:
                for nestedRecord in runRecord[nestedArrays[0]]:
                    
                    payloadObj = payload.dict()
                    del payloadObj['target']
                    del payloadObj['action']
                    # payloadObj['createdBy'] = 'DVWQW7GQZ4'
                    # payloadObj['modifiedBy'] = 'DVWQW7GQZ4'

                    # add first connection recordId & nested connection recordId
                    payloadObj[recordArray] = runRecord.get('recordId')
                    payloadObj[nestedArrays[0]] = nestedRecord.get('recordId')

                    keysList = list(nestedRecord.keys())
                    # add any extra fields to each record
                    for key in keysList:
                        if key != 'recordId':
                            payloadObj[key] = nestedRecord.get(key)

                    if self.checkDuplicate(payloadObj, payload.target):
                        duplicatedResponses.append({'status_code': 409, 'detail': 'record already exisit', 'payload': payloadObj})
                    else:
                        successResponses.append(payloadObj)

        response = TBBulkActionChildInsert(target=payload.target, successChildList=successResponses, duplicatedChildList=duplicatedResponses, bulkCount=TBBulkCount(success=len(successResponses), duplicated=len(duplicatedResponses)))

        return response
    
    async def syncEnrolment(self, payload: TadaBaseGenericBulkSync) -> TBBulkActionResponse:
        try:
            successResponses = []
            failedResponses = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                concurrentCalls = []

                for refnum in payload.records:
                    sleep(0.5)
                    concurrentCalls.append(executor.submit(asyncio.run, syn_grant_ssg_tb(refnum)))


                for f in concurrent.futures.as_completed(concurrentCalls):
                    try:
                        print('\nTB response back: ', f.result())
                        successResponses.append(f.result())
                    except Exception as e:
                        print(f'failed to sync enrolment: {str(e)}')
                        failedResponses.append({
                            'message': 'failed to sync enrolment',
                            'stats_code': 400,
                            'detail': str(e)
                        })

            response = TBBulkActionResponse(payload=(successResponses + failedResponses), bulkCount=TBBulkCount(success=len(successResponses), failed=len(failedResponses)))

            return response
        except Exception as e:
            raise HTTPException(status_code=400, detail='something went wrong')
        
    def backgroundInsert(self, payload: Union[TBBulkActionChildInsert, TBBulkActionUpdateCreaeResponse]) -> list:
        print('insert child')

        TadaBaseResponses = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            concurrentCalls = []

            for record in payload.successChildList:
                sleep(0.5)
                concurrentCalls.append(executor.submit(asyncio.run, self._insertRecordRequest(payload.target, record)))

            for f in concurrent.futures.as_completed(concurrentCalls):
                print('\nTB response back: ', f.result())
                TadaBaseResponses.append(f.result())
        

        print('Done')
        return TadaBaseResponses
