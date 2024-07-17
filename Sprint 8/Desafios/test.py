import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Custom imports ==================================================================================
import boto3
import json
import re as regex
from botocore.exceptions import ClientError


# Custom Functions ================================================================================
# S3 key manipulation functions _______________________________________________
def bucket_name_from_url(url: str) -> str:
    return url.split("/")[2]

def s3_key_from_url(url: str) -> str:
    return "/".join(url.split("/")[3:])

def date_from_s3_key(obj_key: str) -> str:
    rule = r"\d{4}/\d{2}/\d{2}/[\w|.]+$"
    date = regex.search(rule, obj_key).group()
    return '-'.join(date.split("/")[:-1])

# _____________________________________________________________________________
def load_args(arg_list: list=None, file_path: str=None) -> dict:
    if "--local" in sys.argv:
        with open(file_path) as file:
            return json.load(file)
    else:
        return getResolvedOptions(sys.argv, arg_list)
    
# Main Function ================================================================================
def main():
    # Loading Job Parameters __________________________________________________
    ARGS_LIST = [
        "JOB_NAME",
        "S3_MOVIE_INPUT_PATH", 
        "S3_SERIES_INPUT_PATH", 
        "S3_TARGET_PATH"
    ]
    
    args = load_args(ARGS_LIST, "createTrustedDataLocal_parameters.json")
    
    # Creating Job Context ____________________________________________________
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args[ARGS_LIST[0]], args)
    # # Custom Code ===============================================================================
    
    # df = glueContext.create_dynamic_frame.from_options(
    #     "s3", {"paths": ["s3://gustavcampos-pb-data-lake/Raw/Local/CSV/Movies/2024/07/01/movies.csv"]},
    #     "csv", {"withHeader": True, "separator":"|"},
    # )
    
    # df.show()
    
    
    
    
    
    
    
    # Setting Constants _______________________________________________________ 
    S3_MOVIE_INPUT_PATH = args[ARGS_LIST[1]]
    S3_SERIES_INPUT_PATH = args[ARGS_LIST[2]]
    S3_TARGET_PATH = args[ARGS_LIST[3]]
    BUCKET_NAME = bucket_name_from_url(S3_MOVIE_INPUT_PATH)

    s3_client = boto3.client("s3")

    response = s3_client.list_objects(
        Bucket=BUCKET_NAME, 
        Prefix=s3_key_from_url(S3_MOVIE_INPUT_PATH)
    )

    print(response)

    for obj in response["Contents"]:
        key = obj["Key"]
        obj_date = date_from_s3_key(key)
        print(f"Key: {key}")
        print(f"Date: {obj_date}")
    
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
