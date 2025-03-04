import dlt
import pandas as pd


# Функция для запуска DLT пайплайна
def run_dlt_pipeline(data: pd.DataFrame, dataset_name: str):
    # Настройка и запуск dlt пайплайна
    pipeline = dlt.pipeline(
        pipeline_name="excel_to_dremio",
        destination="dremio",
        dataset_name=dataset_name,
    )

    # Загрузка данных с append write_disposition
    load_info = pipeline.run(
        data,
        table_name="all_data",
        write_disposition="append",
    )
    return load_info
