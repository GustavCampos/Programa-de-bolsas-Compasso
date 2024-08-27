import boto3
import json
from botocore.exceptions import ClientError


def check_bucket_exists(s3_client: boto3.client, bucket_name: str) -> bool:
    print(f"Verifying if bucket {bucket_name} exists...")
    
    try:
        bucket_request = s3_client.head_bucket(Bucket=bucket_name)
    
        print("Request: {}, Status: {}".format(
            bucket_request["ResponseMetadata"]["RequestId"],
            bucket_request["ResponseMetadata"]["HTTPStatusCode"]
        ))
    
        return True
    
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
       
        match error_code:
            case "404":
                print(f"Error: Bucket {bucket_name} not exists.")            
            case "403":
                print(f"Error: Access denied to bucket {bucket_name}.")
            case _: 
                print(f"Error: {error}")
       
        return False


def get_object_in_bucket(s3_client: boto3.client, bucket_name: str, obj_key: str) -> dict | None:
    print(f"Retrieving object {obj_key} from bucket {bucket_name}...")
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=obj_key)
        print(f"{obj_key} retrieved successfully!")
        return json.load(response["Body"])
    
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        print(f"Error {error_code}: {error}")
        
        return {
            "status": "error",
            "code": error_code,
            "message": f"Error {error_code}: {error}"
        }
    
    
def upload_json_to_bucket(s3_client: boto3.client, bucket_name: str, obj_key: str, json_dict: dict) -> bool:
    print(f"Trying to upload {obj_key} to bucket {bucket_name}...")
    
    try:
        response = s3_client.put_object(
            Body=   json.dumps(json_dict, indent=4),
            Bucket= bucket_name,
            Key=    obj_key
        )
    
        print("Request {0}: {1} Uploaded to bucket {2}!".format(
            response["ResponseMetadata"]["RequestId"],
            obj_key, bucket_name
        ))
    
        return True
    
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        print(f"Error {error_code}: {error}")
        return False