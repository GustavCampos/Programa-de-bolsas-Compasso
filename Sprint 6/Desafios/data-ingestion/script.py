import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from json import load
from os.path import realpath, dirname, join


def upload_obj_in_bucket(s3_client, local_path, bucket_name, obj_key):
    print(f"Verifying if {obj_key} exists in bucket {bucket_name}...")
    try:
        obj_request = s3_client.head_object(Bucket=bucket_name, Key=obj_key)
        
        print("Request: {}, Status: {}".format(
            obj_request["ResponseMetadata"]["RequestId"],
            obj_request["ResponseMetadata"]["HTTPStatusCode"]
        ))
        
        print(f"Object {obj_key} found in bucket {bucket_name}!")
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        
        match error_code:
            case "404":
                print(f"Object {obj_key} not found in bucket {bucket_name}. Uploading...")
                try:
                    s3_client.upload_file(local_path, bucket_name, obj_key)
                    print(f"Object {obj_key} created successfully in bucket {bucket_name}!") 
                except ClientError as error:
                    print(f"Error: {error}")
                    exit()
                    
            case "403":
                print(f"Access denied to object {obj_key} in bucket {bucket_name}.")
                exit()
            case _: 
                print(f"Error: {error}")
                exit()

def create_bucket(s3_client, bucket_name):
    print(f"Verifying if bucket {bucket_name} exists...")
    try:
        bucket_request = s3_client.head_bucket(Bucket=bucket_name)
        
        print("Request: {}, Status: {}".format(
            bucket_request["ResponseMetadata"]["RequestId"],
            bucket_request["ResponseMetadata"]["HTTPStatusCode"]
        ))
        
        print(f"Bucket {bucket_name} found!")
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        
        match error_code:
            case "404":
                print(f"Bucket {bucket_name} not found. Creating...")
                try:
                    s3_client.create_bucket(Bucket=bucket_name)
                    print(f"Bucket {bucket_name} created successfully")
                except ClientError as error:
                    print(f"Error: {error}")
                    exit()
                    
            case "403":
                print(f"Access denied to bucket {bucket_name}.")
                exit()
            case _: 
                print(f"Error: {error}")
                exit()


def main():
    LOCATION = dirname(realpath(__file__))
    PARAMETERS_JSON = join(LOCATION, "data", "aws_parameters.json")
    MOVIES_CSV = join(LOCATION, "data", "movies.csv")
    SERIES_CSV = join(LOCATION, "data", "series.csv")
    TODAY = datetime.now()
    
    with open(PARAMETERS_JSON, "r") as file:
        AWS_PARAMETERS = load(file)

    S3_PARTIAL_PATH = "Raw/Local/CSV"
    S3_DATE_PATH = f"{TODAY.year}/{TODAY.month:02d}/{TODAY.day:02d}"
    S3_MOVIES_PATH = f"{S3_PARTIAL_PATH}/Movies/{S3_DATE_PATH}/movies.csv"
    S3_SERIES_PATH = f"{S3_PARTIAL_PATH}/Series/{S3_DATE_PATH}/series.csv"

    print("Creating S3 client...")
    s3_client = boto3.client('s3',
        region_name=            AWS_PARAMETERS["AWSRegion"],
        aws_access_key_id=      AWS_PARAMETERS["AWSAccessKeyId"],
        aws_secret_access_key=  AWS_PARAMETERS["AWSSecretAccessKey"],
        aws_session_token=      AWS_PARAMETERS["AWSSessionToken"]
    )
    
    create_bucket(s3_client, AWS_PARAMETERS["AWSBucketName"])
    
    upload_obj_in_bucket(
        s3_client=      s3_client, 
        local_path=     MOVIES_CSV, 
        obj_key=        S3_MOVIES_PATH, 
        bucket_name=    AWS_PARAMETERS["AWSBucketName"],         
    )
    
    upload_obj_in_bucket(
        s3_client=      s3_client,
        local_path=     SERIES_CSV,
        obj_key=        S3_SERIES_PATH,
        bucket_name=    AWS_PARAMETERS["AWSBucketName"],
    )
    
    print("Data ingestion finished!")
    
if __name__ == "__main__":
    main()