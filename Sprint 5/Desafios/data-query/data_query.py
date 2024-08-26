import boto3
import json
from os.path import join, dirname, realpath


def get_credentials(credentials_file: str) -> dict:
    return_dict = {}
    
    with open(credentials_file, 'r') as file:
        for line in file.readlines():
            key, value = line.strip().replace("\n", "").split("=")
            return_dict[key.upper()] = value
            
    return return_dict

def main():
    LOCATION = dirname(realpath(__file__))
    S3_QUERY_PATH = join(LOCATION, 'query.sql')
    AWS_PARAMETERS = get_credentials(join(LOCATION, "data", "aws_parameters.env"))
    RESULT_JSON = join(LOCATION, "data", 'query-result.json')
    
    print("Creating S3 client...")
    s3_client = boto3.client('s3', 
        aws_access_key_id=      AWS_PARAMETERS["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=  AWS_PARAMETERS["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=      AWS_PARAMETERS["AWS_SESSION_TOKEN"],
        region_name=            AWS_PARAMETERS["AWS_REGION"],
    )
    
    print("Loading S3 select query...")
    with open(S3_QUERY_PATH, 'r') as file:
        S3_SELECT_QUERY = ""
        for line in file.readlines()[4:]:
            S3_SELECT_QUERY += f"{line.strip()} "

    print(f"Loaded query: \n{S3_SELECT_QUERY}")
    
    print("Querying S3 object...")
    s3_query_response = s3_client.select_object_content(
        Bucket=                 AWS_PARAMETERS["AWS_BUCKET"],
        Key=                    AWS_PARAMETERS["AWS_BUCKET_OBJECT"],
        Expression=             S3_SELECT_QUERY,
        ExpressionType=         'SQL',
        RequestProgress=        {"Enabled": True},
        InputSerialization=     {'CSV': {'FileHeaderInfo': 'Use'}},
        OutputSerialization=    {'JSON': {'RecordDelimiter': '\n'}}
    )
    
    print("Query complete, processing results...")
    for event in s3_query_response['Payload']:        
        if 'Records' in event:
            records = json.loads(event['Records']['Payload'].decode('utf-8'))
            
    print(f"Saving results to {RESULT_JSON}")
    with open(RESULT_JSON, 'w') as file:
        json.dump(records, file, indent=4, ensure_ascii=False)
    
    print("Results saved, printing JSON...")
    print(json.dumps(records, indent=4, ensure_ascii=False))

if __name__ == '__main__':
    main()