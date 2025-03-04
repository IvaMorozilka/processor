from . import S3_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, BUCKET
import boto3
import pandas as pd
import io

s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    use_ssl=False,
)


def get_file_as_df_from_s3(object_key: str, bucket: str = BUCKET):
    response = s3_client.get_object(Bucket=bucket, Key=object_key.replace("+", " "))
    file_content = response["Body"].read()
    df = pd.read_excel(io.BytesIO(file_content))
    return df


def put_df_to_s3(path: str, df: pd.DataFrame, bucket: str = BUCKET):
    with io.BytesIO() as buffer:
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_client.put_object(Bucket=bucket, Key=path, Body=buffer.getvalue())
