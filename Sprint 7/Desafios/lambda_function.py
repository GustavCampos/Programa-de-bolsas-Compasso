import boto3
import json
import logging
import os
import request_functions as rf
import s3_functions as s3f
import utils

# Creating logger obj
logger = logging.getLogger()

def lambda_handler(event, context) -> dict:
    LOCAL = os.path.dirname(os.path.realpath(__file__))
    JSON_PATH = os.path.join(LOCAL, "config.json")
    
    # Checking enviroment variables ___________________________________________
    logger.info("Checking Environment Variables")
    if not utils.check_env_vars():
        error_msg = "Missing Environment Variables"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps(error_msg)}
    
    TMDB_SESSION_TOKEN = os.getenv("TMDB_SESSION_TOKEN")
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
    S3_BUCKET_REGION = os.getenv("S3_BUCKET_REGION")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
        
    # Checking configuration variables ________________________________________
    try:
        logger.info("Importing Config JSON File")
        REQUEST_PARAMETERS = utils.import_json(JSON_PATH)
    except FileNotFoundError:
        error_msg = "Missing Config JSON File"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps(error_msg)}
    
    # Creating s3 client ______________________________________________________
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
        if PROGRESS_JSON["code"] == "NoSuchKey":
            PROGRESS_JSON = {"movieCurrentPage": 1, "tvCurrentPage": 1}
        else:
            logger.error(PROGRESS_JSON)
            return {"statusCode": 500, "body": json.dumps(PROGRESS_JSON)}
        
    
    # Creating request header _________________________________________________
    REQUEST_HEADER = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_SESSION_TOKEN}"
    }
    
    # Check API modifier ______________________________________________________
    api_modifier = rf.get_api_modifier(REQUEST_PARAMETERS, PROGRESS_JSON)
    
    if api_modifier is None:
        logger.info("No More Data to Request")
        return {"statusCode": 200, "body": json.dumps(PROGRESS_JSON)}
    
    # Requesting data for movies/series _______________________________________
    start_page = PROGRESS_JSON[f"{api_modifier}CurrentPage"]
    for _ in range(REQUEST_PARAMETERS["filesPerCall"]):
        end_page = start_page + REQUEST_PARAMETERS["pageGroup"]
        file_name = f"movies_{start_page:04d}-{(end_page - 1):04d}.json"
        
        logger.info(f"Generating File: {file_name}")
        response = rf.generate_json_files(
            start_page=         start_page,
            max_page=           REQUEST_PARAMETERS[api_modifier]["maxPage"],
            pages_per_file=     REQUEST_PARAMETERS["pageGroup"],
            request_url=        REQUEST_PARAMETERS[api_modifier]["url"],
            request_header=     REQUEST_HEADER
        )
        
        s3f.upload_json_to_bucket()
        
    PROGRESS_JSON[f"{api_modifier}CurrentPage"] = response["newCurrentPage"]
        
    # Update progress JSON file _______________________________________________
    json_updated = s3f.upload_json_to_bucket(
        s3_client=      s3_client,
        bucket_name=    S3_BUCKET_NAME,
        obj_key=        PROGRESS_OBJ_KEY,
        json_dict=      PROGRESS_JSON
    )
    if not json_updated:
        error_msg = "Error Updating Progress JSON File"
        logger.error(error_msg)
        return {"statusCode": 500, "body": json.dumps(error_msg)}
        
    logger.info("Config JSON File Updated")
    return {"statusCode": 200, "body": json.dumps(PROGRESS_JSON)}