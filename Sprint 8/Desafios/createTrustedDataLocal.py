import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Custom imports ==================================================================================
import json
import re as regex
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as dff


# Custom Functions ================================================================================
# S3 key manipulation functions _______________________________________________
def url_to_bucket_name(url: str) -> str:
    return url.split("/")[2]

def url_to_s3_key(url: str) -> str:
    return "/".join(url.split("/")[3:])

def s3_key_to_url(obj_key: str, bucket_name=str) -> str:
    return f's3://{bucket_name}/{obj_key}'

def s3_key_to_date(obj_key: str) -> str:
    rule = r"\d{4}/\d{2}/\d{2}/[\w|.]+$"
    date = regex.search(rule, obj_key).group()
    return '-'.join(date.split("/")[:-1])

# Glue Job Functions __________________________________________________________
def load_args(arg_list: list=None, file_path: str=None) -> dict:
    if "--local" in sys.argv:
        with open(file_path) as file:
            return json.load(file)
    else:
        return getResolvedOptions(sys.argv, arg_list)

def generate_unified_df(glue_context: GlueContext, s3_client: boto3.client, s3_path: str,  
                        file_format: str, format_options: dict) -> spark.sql.DataFrame:
    print(f'Getting Object List In {s3_path}')
    bucket_name = url_to_bucket_name(s3_path)
    
    response = s3_client.list_objects(
        Bucket=bucket_name, 
        Prefix=url_to_s3_key(s3_path)
    )

    found_objects_number = len(response["Contents"])
    print(f"{found_objects_number} Found In {s3_path}!")

    # Importing Found Objects _________________________________________________ 
    print("Loading Found Files...")
    
    unified_df = None
    for i in range(found_objects_number):
        obj_key = response["Contents"][i]['Key']
        obj_date = s3_key_to_date(obj_key)

        print(f"{i+1}/{found_objects_number} - Loading {obj_key}")

        obj_dyf = glue_context.create_dynamic_frame.from_options(
            connection_type=        "s3",
            connection_options=     {"paths": [s3_key_to_url(obj_key, bucket_name)]},
            format=                 file_format,
            format_options=         format_options
        )

        obj_df = obj_dyf.toDF()
        with_date_df = obj_df.withColumn("IngestionDate", dff.lit(obj_date).cast("date"))
        unified_df = (unified_df.union(with_date_df), with_date_df)[i == 0]

    return unified_df

    
# Main Function ================================================================================
def main():
    # Loading Job Parameters __________________________________________________
    print("Loading Job Parameters...")

    ARGS_LIST = [
        "JOB_NAME",
        "S3_MOVIE_INPUT_PATH", 
        "S3_SERIES_INPUT_PATH", 
        "S3_TARGET_PATH"
    ]

    ## @params: [JOB_NAME, S3_MOVIE_INPUT_PATH, S3_SERIES_INPUT_PATH, S3_TARGET_PATH]
    args = load_args(ARGS_LIST, "createTrustedDataLocal_parameters.json")
    
    # Creating Job Context ____________________________________________________
    print("Creating Job Context...")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args[ARGS_LIST[0]], args)
    
    # # Custom Code ===============================================================================
    # Setting Constants _______________________________________________________ 
    S3_MOVIE_INPUT_PATH = args[ARGS_LIST[1]]
    S3_SERIES_INPUT_PATH = args[ARGS_LIST[2]]
    S3_TARGET_PATH = args[ARGS_LIST[3]]
    
    BUCKET_NAME = url_to_bucket_name(S3_MOVIE_INPUT_PATH)
    MOVIE_PREFIX_KEY = url_to_s3_key(S3_MOVIE_INPUT_PATH)
    SERIES_PREFIX_KEY = url_to_s3_key(S3_SERIES_INPUT_PATH)

    print('Creating S3 Client...')
    s3_client = boto3.client("s3")

    movies_df = generate_unified_df(
        glue_context=   glueContext,
        s3_client=      s3_client,
        s3_path=        S3_MOVIE_INPUT_PATH,
        file_format=    "csv", 
        format_options= {"withHeader": True, "separator": "|"}
    )

    print("Movie Data Import Complete!")
    movies_df.show(3)
    
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
