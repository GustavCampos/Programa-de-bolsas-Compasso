# TODO: import sys
from awsglue.transforms import *
# TODO: from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
# TODO: from awsglue.job import Job

# TODO: Remove dotenv import

# Custom imports ==================================================================================
import re as regex
import boto3
from botocore.exceptions import ClientError

# Custom Functions ================================================================================
def bucket_name_from_url(url: str) -> str:
    return url.split("/")[2]

def s3_key_from_url(url: str) -> str:
    return "/".join(url.split("/")[3:])

def date_from_s3_key(obj_key: str) -> str:
    rule = r"\d{4}/\d{2}/\d{2}/[\w|.]+$"
    date = regex.search(rule, obj_key).group()
    return '-'.join(date.split("/")[:-1])

def main():
    # Loading Env Vars ________________________________________________________
    load_dotenv()
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
    S3_MOVIE_INPUT_PATH = os.getenv("S3_MOVIE_INPUT_PATH")
    S3_SERIES_INPUT_PATH = os.getenv("S3_SERIES_INPUT_PATH")
    S3_TARGET_PATH = os.getenv("S3_TARGET_PATH")
    
    BUCKET_NAME = bucket_name_from_url(S3_MOVIE_INPUT_PATH)

    s3_client = boto3.client("s3",
        aws_access_key_id=      AWS_ACCESS_KEY_ID,
        aws_secret_access_key=  AWS_SECRET_ACCESS_KEY,
        aws_session_token=      AWS_SESSION_TOKEN
    )

    response = s3_client.list_objects(
        Bucket=BUCKET_NAME, 
        Prefix=s3_key_from_url(S3_MOVIE_INPUT_PATH)
    )

    for obj in response["Contents"]:
        obj_date = date_from_s3_key(obj["Key"])

        
        
if __name__ == "__main__":
    main()
