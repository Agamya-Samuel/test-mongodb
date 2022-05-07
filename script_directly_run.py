import os
from dotenv import load_dotenv

from pymongo import MongoClient

import requests
import json


load_dotenv()

MONGODB_URL = os.getenv('MONGODB_URL')
IMDB_API_URL = os.getenv('IMDB_API_URL')
MONGODB_DATABASE_NAME = os.getenv('MONGODB_DATABASE_NAME')
MONGODB_COLLECTION_NAME = os.getenv('MONGODB_COLLECTION_NAME')


def connectTo_MongoDB() -> None:
    global collection, cluster
    cluster = MongoClient(MONGODB_URL)
    database = cluster[MONGODB_DATABASE_NAME]
    collection = database[MONGODB_COLLECTION_NAME]


def generateIMDB_ID(num:int) -> str:
    numStr = str(num)   # converting num to string
    if num <= 999_999:
        imdb_id = ("tt" + numStr.zfill(7))
    else:
        imdb_id = ("tt" + numStr)
    return imdb_id


def generateUnique_ID(num:int) -> int:
    numStr = str(num)
    unique_id = int("1" + numStr.zfill(9))
    return unique_id


def getMetadaFromIMDB_API(imdb_id:str) -> str:
    IMDB_API_REQ_URL = f"{IMDB_API_URL}/suggestion/t/{imdb_id}.json"
    req = requests.request("GET", IMDB_API_REQ_URL).text
    return req


def findLastEntertedDocument_InDB() -> object:
    doc = collection.find().sort("_id", -1).limit(1)
    return doc


def closeConnectionToDB() -> None:
    cluster.close()
    print("DB connection Terminated!!")


def documentGenrator(uid:str, IMDB_ID:str, IMDB_Title:str, IMDB_Year:str, IMDB_Poster_url:str) -> object:
    document = {
                "_id": uid, 
                "title": IMDB_Title, 
                "year": IMDB_Year, 
                "poster": IMDB_Poster_url, 
                "db": [
                        {
                            "src": "IMDB", 
                            "id": IMDB_ID
                        }
                    ],
                "available": False
                }
    return document


def clearEntireDB() -> None:
    clearDB = str(input("Do you want clear the Entire DB ? (y/n): "))
    if clearDB == "y":
        collection.delete_many({})
        print("DB data cleared!!")
    else:
        print("DB data retained!!")


def insertIntoTheDB(document: object) -> None:
    collection.insert_one(document)


def removeYearFromTitle(IMDB_Title:str, IMDB_Year:int) -> str:
    if str(IMDB_Year) != IMDB_Title: # some movies have same Title and Relase Year
            if (str(IMDB_Year) in IMDB_Title):
                if (
                        (
                            (IMDB_Title[-1] == ")") and 
                            (IMDB_Title[-11] == "(") and 
                            (IMDB_Title[-5] == "-")
                        ) 
                        or 
                        (IMDB_Title[-6] == "-")
                    ):
                    pass
                else:
                    IMDB_Title = (
                                    IMDB_Title.replace(
                                                        str(IMDB_Year), 
                                                        ""
                                                    )
                                ).strip(" ()") # removes IMDB_Year and "()"" from IMDB_Title if present any 
    return IMDB_Title


def extractDataFromLastDocument() -> list:
    global extractedData
    try:
        lastDocument = findLastEntertedDocument_InDB()[0]
        lastDocument_ID_index = (lastDocument["_id"] % 1_000_000_000)
        lastDocument_IMDB_ID = lastDocument["db"][0]["id"]
        lastDocument_IMDB_ID_num = int(lastDocument_IMDB_ID[2:])
    except IndexError: # catch IndexError -->> Error happening when returned 0 ducuments in the MongoDB
        lastDocument_ID_index = 0
        lastDocument_IMDB_ID = "tt0000000"
        lastDocument_IMDB_ID_num = 0
    print(f"{lastDocument_ID_index = }")
    print(f"{lastDocument_IMDB_ID = }")
    print(f"{lastDocument_IMDB_ID_num = }")
    extractedData = [lastDocument_ID_index, lastDocument_IMDB_ID, lastDocument_IMDB_ID_num]
    return extractedData


def extractAll_DataFromIMDB(imdb_id:str) -> str:
    metadata = getMetadaFromIMDB_API(imdb_id=imdb_id)
    return metadata


def validateIMDB_Data(imdb_id) -> None:
    print(f"Checking if {imdb_id} has IMDB_Description or not.")
    metadataJSON = json.loads(extractAll_DataFromIMDB(imdb_id=imdb_id))
    try:
        metadataJSON["d"]
        print(f"IMDB_Description Found for {imdb_id = } \nReturning True, Exiting Recursive extractUseable_DataFromIMDB()")
        return True
    except KeyError:
        print(f"IMDB_Description Not Found for {imdb_id = } \nReturning False, going into Recursive extractUseable_DataFromIMDB()")
        return False


def extractUseable_DataFromIMDB(imdb_id) -> list:    # recursive function
    global extractedData
    IMDB_Data_is_Valid = validateIMDB_Data(imdb_id=imdb_id)
    if IMDB_Data_is_Valid:
        global finalData
        metadataJSON = json.loads(extractAll_DataFromIMDB(imdb_id=imdb_id))
        IMDB_Title = metadataJSON["d"][0]["l"]
        IMDB_Year = metadataJSON["d"][0]["y"]
        IMDB_Title = removeYearFromTitle(IMDB_Title=IMDB_Title, IMDB_Year=IMDB_Year)
        try:
            IMDB_Poster_url = metadataJSON["d"][0]["i"]["imageUrl"]
        except KeyError:
            IMDB_Poster_url = None
        print(f"{imdb_id = }, {IMDB_Title = }, {IMDB_Year = }, {IMDB_Poster_url = }")
        finalData = [imdb_id, IMDB_Title, IMDB_Year, IMDB_Poster_url]
    else:
        lastDocument_IMDB_ID_num = (int(extractedData[2]) + 1)
        lastDocument_IMDB_ID = generateIMDB_ID(lastDocument_IMDB_ID_num)
        # extractedData[0] updation is managed by uid_counter() function
        extractedData[1] = lastDocument_IMDB_ID
        extractedData[2] = lastDocument_IMDB_ID_num
        finalData = extractUseable_DataFromIMDB(lastDocument_IMDB_ID)
    return finalData


def uid_counter(start:int, end:int, extractedData:list) -> None:
    for num in range(start, end):
        lastDocument_IMDB_ID_num = int(extractedData[2])
        IMDB_ID = generateIMDB_ID(lastDocument_IMDB_ID_num + 1)
        uid = generateUnique_ID(num)
        extractedData[0] = num  # lastDocument_ID_index
        extractedData[1] = IMDB_ID  # lastDocument_IMDB_ID
        extractedData[2] = (lastDocument_IMDB_ID_num + 1)  # lastDocument_IMDB_ID_num
        finalData = extractUseable_DataFromIMDB(IMDB_ID)
        print(f"{finalData = }")
        imdb_id, IMDB_Title, IMDB_Year, IMDB_Poster_url = finalData
        doc = documentGenrator(
                                uid=uid, 
                                IMDB_ID=imdb_id, 
                                IMDB_Title=IMDB_Title, 
                                IMDB_Year=IMDB_Year, 
                                IMDB_Poster_url= IMDB_Poster_url
                                )
        insertIntoTheDB(document=doc)


def takeInputFromUser(startLimit:int) -> None:
    global extractedData
    #num_of_IDs = int(input("Enter the MaxLimit (Enter 0 to Exit) : "))
    num_of_IDs = 200_000
    if num_of_IDs != 0:
        uid_counter(
                    start=startLimit,
                    end=(startLimit + num_of_IDs), 
                    extractedData=extractedData
                    )
        print(f"Enterd {num_of_IDs} ids into th DB")


def main() -> None:
    global extractedData
    connectTo_MongoDB()
    #clearEntireDB()
    extractedData = extractDataFromLastDocument()
    lastDocument_ID_index = int(extractedData[0])
    startLimit = (lastDocument_ID_index + 1)
    takeInputFromUser(startLimit=startLimit)
    closeConnectionToDB()
    print("Program Exited Successfully!!")


extractedData = [] # acting as a global list
finalData = [] # acting as a global list
collection
cluster 


if __name__ == "__main__":
    main()