# Сервис для загрузки файлов в S3 с последующем ETL в PostgresSQL

## Состоит из следующих микросервисов:

* ### PostgreSQL

* ### Minio

* ### Worker

  * **FastAPI**: Для удобного создания API
  * **dltHub**: Для удобного создания ETL pipeline`a
  * **boto3**: Для взаимодействия с S3
  * **pandas**: Для обработки сырых файлов в готовый parquet

## Принцип работы

![image](process.drawio.svg)
