import urllib.parse
from fastapi import FastAPI, Request
import asyncio
import logging
import random
import urllib

app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/process_file")
async def process_file(request: Request):
    event_data = await request.json()
    # Извлечение пути к объекту
    object_key = urllib.parse.unquote(event_data["Records"][0]["s3"]["object"]["key"])
    bucket_name = event_data["Records"][0]["s3"]["bucket"]["name"]

    logger.info("Начало таймера")
    await asyncio.sleep(random.randrange(2, 5))
    logger.info(f"Обрабатываю файл: {bucket_name}/{object_key}")

    return {"status": "success", "processed_file": object_key}
