from pymongo import MongoClient
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
import os
from bson import ObjectId
load_dotenv()
 
CONN_STRING = os.getenv("COSMOS_URI")
DB_NAME = os.getenv("dbName")
COLL_NAME = os.getenv("COLLECTION")
 
app = FastAPI()
client = AsyncIOMotorClient(CONN_STRING, tls=True, tlsAllowInvalidCertificates=True)
db = client[DB_NAME]
collection = db[COLL_NAME]
 
def fix_id(doc):
    doc["id"] = str(doc["_id"])
    del doc["_id"]
    return doc
 
@app.get("/")
async def root():
    return {"message": "Cosmos App running"}
 
# Insert
@app.post("/products")
async def create_product(product: dict):
    result = await collection.insert_one(product)
    return {"id:": str(result.inserted_id)}
 
# Read
@app.get("/products")
async def read_product():
    cursor = await collection.find().to_list(100)
    return [fix_id(p) for p in cursor]
 
# Read a product
@app.get("/products/{id}")
async def get_product(id: str):
    product = await collection.find_one({"_id": ObjectId(id)})
    if not product:
        raise HTTPException(404, "Product not Found")
    return fix_id(product)
 
# Update
@app.put("/products/{id}")
async def update_item(id: str, data: dict):
    result = await collection.update_one(
        {"_id": ObjectId(id)},
        {"$set": data}
    )
    if result.modified_count == 0:
        raise HTTPException(404, "Product not Found")
    return {"messsage": "Updated"}
 
# Delete
@app.delete("/products/{id}")
async def delete_product(id: str):
    result = await collection.delete_one({"_id": ObjectId(id)})
    if result.deleted_count == 0:
        raise HTTPException(404, "Product not Found")
    return {"message": "Deleted Successfully"}