import boto3
from functools import reduce
from os.path import join, dirname, realpath
from botocore.exceptions import ClientError


def get_credentials(credentials_file: str) -> dict:
    return_dict = {}
    
    with open(credentials_file, 'r') as file:
        # Ignore first line in file
        for line in file.readlines()[1:]:
            key, value = line.strip().replace("\n", "").split("=")
            return_dict[key.upper()] = value
            
    return return_dict

def main():
    LOCATION = join(dirname(realpath(__file__)))
    CSV_FILE = join(LOCATION, 'data.csv')
    AWS_CREDENTIALS = get_credentials(join(LOCATION, 'aws_credentials.env')) 
    AWS_REGION = "us-east-1"
    BUCKET_NAME = "gustav-campos-pb-sp5"
    OBJ_IN_BUCKET_NAME = "canais-credenciados.csv"

    print("Creating S3 client...")
    s3_client = boto3.client('s3', region_name=AWS_REGION,
        aws_access_key_id=AWS_CREDENTIALS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=AWS_CREDENTIALS["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=AWS_CREDENTIALS["AWS_SESSION_TOKEN"]
    )
    
    print(f"Verifying if bucket {BUCKET_NAME} exists...")
    try:
        bucket_request = s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} found!")
    except ClientError as error:
        error_code = error.response["Error"]["Code"]
        
        if error_code != "404":
            if error_code == "403":
                print(f"Access denied to bucket {BUCKET_NAME}, try another bucket name")
            else:
                print(f"Error: {error}")
            exit() 
            
        print(f"Bucket {BUCKET_NAME} not found. Creating...")
        try:
            s3_client.create_bucket(Bucket=BUCKET_NAME)
            print(f"Bucket {BUCKET_NAME} created successfully")
        except ClientError as error:
            print(f"Error: {error}")
            exit()
    
    print(f"Verifying if {OBJ_IN_BUCKET_NAME} exists in bucket {BUCKET_NAME}...")
    try:
        request = s3_client.head_object(Bucket=BUCKET_NAME, Key=OBJ_IN_BUCKET_NAME)
    except ClientError as error:
        pass
    
    # for bucket in bucket_list:
    #     print(bucket.name)
        
    # bucket_exists = BUCKET_NAME in buckt_list_request["Buckets"]
    
    # print(bucket_exists)
        
if __name__ == "__main__":
    main()