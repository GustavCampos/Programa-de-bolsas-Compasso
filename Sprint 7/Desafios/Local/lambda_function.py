import boto3
import constants
import json
import os
import request_functions as rf
import s3_functions as s3f
import utils
from datetime import datetime


def lambda_handler(event, context) -> dict:
    LOCAL = os.path.dirname(os.path.realpath(__file__))
    JSON_PATH = os.path.join(LOCAL, "config.json")
    NOW_DATE = datetime.now()
    
    # Checking enviroment variables ___________________________________________
    print(constants.LOG_ENV_VAR_CHECK)
    if not utils.check_env_vars():
        print(constants.LOG_ENV_VAR_MISSING)
        return {"statusCode": 500, "body": json.dumps(constants.LOG_ENV_VAR_MISSING)}
    
    TMDB_SESSION_TOKEN = os.getenv("TMDB_SESSION_TOKEN")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    S3_BUCKET_REGION = os.getenv("S3_BUCKET_REGION")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
        
    # Checking configuration variables ________________________________________
    try:
        print(constants.LOG_JSON_CONFIG_IMPORT)
        REQUEST_PARAMETERS = utils.import_json(JSON_PATH)
    except FileNotFoundError:
        print(constants.LOG_JSON_CONFIG_MISSING)
        return {"statusCode": 500, "body": json.dumps(constants.LOG_JSON_CONFIG_MISSING)}
    
    # Creating s3 client ______________________________________________________
    print(constants.LOG_CREATE_S3_CLIENT)
    s3_client = boto3.client("s3",
        region_name=            S3_BUCKET_REGION,
        aws_access_key_id=      AWS_ACCESS_KEY_ID,
        aws_secret_access_key=  AWS_SECRET_ACCESS_KEY,
        aws_session_token=      AWS_SESSION_TOKEN
    )
    
    # Checking progress JSON file _____________________________________________
    PROGRESS_OBJ_KEY = REQUEST_PARAMETERS["progressObjKey"]
    
    PROGRESS_JSON = s3f.get_object_in_bucket(
        s3_client=      s3_client,
        bucket_name=    S3_BUCKET_NAME,
        obj_key=        PROGRESS_OBJ_KEY
    )
            
    if "status" in PROGRESS_JSON.keys():
        if PROGRESS_JSON["code"] == constants.CREATE_PROGRESS_JSON_ERROR_CODE:
            print(constants.LOG_JSON_PROGRESS_CREATE)
            PROGRESS_JSON = {
                f"{constants.API_MOVIE_MODIFIER}CurrentPage": 1,
                f"{constants.API_SERIES_MODIFIER}CurrentPage": 1
            }
        else:
            print(PROGRESS_JSON)
            return {"statusCode": 500, "body": json.dumps(PROGRESS_JSON)}
        
    # Creating request header _________________________________________________
    print(constants.LOG_REQUEST_HEADER_CREATE)
    REQUEST_HEADER = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_SESSION_TOKEN}"
    }
    
    # Check API modifier ______________________________________________________
    print(constants.LOG_REQUEST_CHECK_PROGRESS)
    api_modifier = rf.get_api_modifier(REQUEST_PARAMETERS, PROGRESS_JSON)
    
    if api_modifier is None:
        print(constants.LOG_REQUEST_PROGRESS_COMPLETE)
        return {"statusCode": 200, "body": json.dumps(PROGRESS_JSON)}
    
    # Requesting data for movies/series _______________________________________
    start_page = PROGRESS_JSON[f"{api_modifier}CurrentPage"]
    
    for _ in range(REQUEST_PARAMETERS["filesPerCall"]):
        # Early return to prevent unnecessary requests ________________________
        if start_page > REQUEST_PARAMETERS[api_modifier]["maxPage"]: break
        
        # Calculating end_page ________________________________________________
        end_page = ((start_page + REQUEST_PARAMETERS["pageGroup"]) - 1)
        
        if end_page > REQUEST_PARAMETERS[api_modifier]["maxPage"]:
            end_page = REQUEST_PARAMETERS[api_modifier]["maxPage"]        
        
        # Creating file name __________________________________________________
        file_name = "{0}_{1:04d}_{2:04d}.json".format(
            api_modifier.lower(), start_page, end_page)
        
        # Generate JSON with requested data ___________________________________
        print(f"Generating File: {file_name}")
        response = rf.generate_json_files(
            start_page=         start_page,
            end_page=           end_page,
            request_url=        REQUEST_PARAMETERS[api_modifier]["url"],
            request_header=     REQUEST_HEADER
        )
        
        # Checking for errors _________________________________________________
        if response["status"] == "error":
            error_msg = f"Error {response['code']}: {response['message']}"
            print(error_msg)
            return {"statusCode": 500, "body": json.dumps(error_msg)}
        
        # Uploading generated JSON to S3 ______________________________________
        json_key = "Raw/TMDB/JSON/{0}/{1}/{2}".format(
            api_modifier.capitalize(),
            NOW_DATE.strftime("%Y/%m/%d"),
            file_name
        )
                
        s3f.upload_json_to_bucket(
            s3_client=      s3_client,
            bucket_name=    S3_BUCKET_NAME,
            obj_key=        json_key,
            json_dict=      response["json"]
        )
        
        # Updating start_page for next iteration ______________________________
        start_page = response["newCurrentPage"]
        
    PROGRESS_JSON[f"{api_modifier}CurrentPage"] = response["newCurrentPage"]
        
    # Update progress JSON file _______________________________________________
    json_updated = s3f.upload_json_to_bucket(
        s3_client=      s3_client,
        bucket_name=    S3_BUCKET_NAME,
        obj_key=        PROGRESS_OBJ_KEY,
        json_dict=      PROGRESS_JSON
    )
    if not json_updated:
        print(constants.LOG_JSON_PROGRESS_UPDATE_ERROR)
        return {"statusCode": 500, "body": json.dumps(constants.LOG_JSON_PROGRESS_UPDATE_ERROR)}
        
    print(constants.LOG_JSON_PROGRESS_UPDATE_SUCCESS)
    return {"statusCode": 200, "body": json.dumps(PROGRESS_JSON)}