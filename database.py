import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables from .env file
load_dotenv()

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017")

client = MongoClient(MONGO_URL)

user_db = client["User"]
user_collection = user_db["Users"]
user_status_collection = user_db["Status"]

chat_db = client["Chat"]
chat_collection = chat_db["Chat"]