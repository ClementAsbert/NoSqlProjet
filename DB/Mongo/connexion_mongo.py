from pymongo import MongoClient
from config import MONGO_URI
import certifi

def get_db():
    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    return client["entertainment"]

def get_dbCollection():
    return get_db()["films"]