import urllib.parse
from fastapi import FastAPI, Request, HTTPException
import logging
import urllib
from pathlib import Path
from datetime import datetime, timezone, timedelta

from s3.client import get_file_as_df_from_s3, put_df_to_s3, s3_client
from etl.pipeline import run_dlt_pipeline
from processing.processing_funcs import procces_df
from processing.helpers import extract_month_and_year


app = FastAPI()
logger = logging.getLogger("uvicorn.error")


@app.get("/")
async def root():
    return {"api": "sucess"}


@app.post("/process_file")
async def process_file(request: Request):
    event_data = await request.json()

    # Извлечение пути к объекту
    bucket_name = (
        event_data.get("Records", [{}])[0].get("s3", {}).get("bucket", {}).get("name")
    )
    object_key = urllib.parse.unquote(
        (event_data.get("Records", [{}])[0].get("s3", {}).get("object", {}).get("key"))
    )
    file_name = Path(object_key).stem
    file_name_ext = Path(object_key).name

    # Загрузка файла датафреймом
    try:
        df = get_file_as_df_from_s3(object_key)
    except Exception as e:
        logger.error(f"FAIL: <{file_name_ext}> не удалось загрузить как df: {e}")
        return {"status": "FAIL"}

    # Основной блок
    try:
        # Обрабатываем файл
        processed_file = procces_df(df, file_name)
        # Запускаем pipeline
        load_info = run_dlt_pipeline(processed_file, file_name)
        month, year = extract_month_and_year(file_name)
        # Формируем путь для обработанного файла
        processed_path = (
            f"processed/{file_name}/{year}/{month}/{Path(object_key).name}.parquet"
        )
        # Закидываем файл в S3
        put_df_to_s3(processed_path, processed_file)
        logger.info(
            f"OK <{file_name}>, сохранен по пути: {processed_path}\nЛОГИ DLT:\n{str(load_info)}"
        )
        return {
            "status": "OK",
        }
    # Обработка ошибка
    except Exception as e:
        # Выводим лог
        error_message = str(e)
        logger.error(f"FAIL <{file_name}>: {error_message}")

        # Создаем лог-файл
        log_content = f"Ошибка при обработке {datetime.now(timezone(timedelta(hours=3)))}: {error_message}"
        log_file_name = f"failed/{file_name}/{file_name}.log"

        # Сохраняем лог в S3
        s3_client.put_object(Bucket="main", Key=log_file_name, Body=log_content)

        # Перемещаем оригинальный файл в failed
        failed_file_path = f"failed/{file_name}/{Path(object_key).name}"

        # Копируем файл в failed
        s3_client.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": object_key},
            Key=failed_file_path,
        )
        return {"status": "FAIL"}
