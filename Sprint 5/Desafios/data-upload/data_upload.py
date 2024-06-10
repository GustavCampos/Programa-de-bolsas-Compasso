import boto3
from os.path import join, dirname, realpath
from botocore.exceptions import ClientError


def get_credentials(credentials_file: str) -> dict:
    return_dict = {}
    
    with open(credentials_file, 'r') as file:
        for line in file.readlines():
            key, value = line.strip().replace("\n", "").split("=")
            return_dict[key.upper()] = value
            
    return return_dict

def main():
    LOCATION = join(dirname(realpath(__file__)))
    CSV_FILE = join(LOCATION, "data", 'canais-credenciados.csv')
    AWS_CREDENTIALS = get_credentials(join(LOCATION, "data", 'aws_parameters.env'))
    AWS_REGION = AWS_CREDENTIALS["AWS_REGION"]
    BUCKET_NAME = AWS_CREDENTIALS["AWS_BUCKET"]
    OBJ_IN_BUCKET_NAME = AWS_CREDENTIALS["AWS_BUCKET_OBJECT"]

    print("Creating S3 client...")
    s3_client = boto3.client('s3', region_name=AWS_REGION,
        aws_access_key_id=AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=AWS_CREDENTIALS["AWS_SESSION_TOKEN"]
    ) 
        
    print(f"Verifying if bucket {BUCKET_NAME} exists...")
    try:
        bucket_request = s3_client.head_bucket(Bucket=BUCKET_NAME)
        
        print("Request: {}, Status: {}".format(
            bucket_request["ResponseMetadata"]["RequestId"],
            bucket_request["ResponseMetadata"]["HTTPStatusCode"]
        ))
        
        print(f"Bucket {BUCKET_NAME} found!")
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        
        match error_code:
            case "404":
                print(f"Bucket {BUCKET_NAME} not found. Creating...")
                try:
                    s3_client.create_bucket(Bucket=BUCKET_NAME)
                    print(f"Bucket {BUCKET_NAME} created successfully")
                except ClientError as error:
                    print(f"Error: {error}")
                    exit()
                    
            case "403":
                print(f"Access denied to object {OBJ_IN_BUCKET_NAME} in bucket {BUCKET_NAME}.")
                exit()
            case _: 
                print(f"Error: {error}")
                exit()
            
    print(f"Verifying if {OBJ_IN_BUCKET_NAME} exists in bucket {BUCKET_NAME}...")
    try:
        obj_request = s3_client.head_object(Bucket=BUCKET_NAME, Key=OBJ_IN_BUCKET_NAME)
        
        print("Request: {}, Status: {}".format(
            obj_request["ResponseMetadata"]["RequestId"],
            obj_request["ResponseMetadata"]["HTTPStatusCode"]
        ))
        
        print(f"Object {OBJ_IN_BUCKET_NAME} found in bucket {BUCKET_NAME}!")
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        
        match error_code:
            case "404":
                print(f"Object {OBJ_IN_BUCKET_NAME} not found in bucket {BUCKET_NAME}. Uploading...")
                try:
                    s3_client.upload_file(CSV_FILE, BUCKET_NAME, OBJ_IN_BUCKET_NAME)
                    print(f"Object {OBJ_IN_BUCKET_NAME} created successfully in bucket {BUCKET_NAME}!") 
                except ClientError as error:
                    print(f"Error: {error}")
                    
            case "403":
                print(f"Access denied to object {OBJ_IN_BUCKET_NAME} in bucket {BUCKET_NAME}.")
            case _: 
                print(f"Error: {error}")
                
        
        
if __name__ == "__main__":
    main()