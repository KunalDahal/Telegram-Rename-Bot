import os
from pyrogram import Client
from dotenv import load_dotenv

load_dotenv()

API_ID = os.getenv("API_ID", "your_api_id") 
API_HASH = os.getenv("API_HASH", "your_api_hash")
BOT_TOKEN = "8997549247:AAGMOk8aCLYj9r-34bxyGo06HysPuydZ100"

app = Client(
    "bot_session_generator",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True,
)

with app:
    print(app.export_session_string())