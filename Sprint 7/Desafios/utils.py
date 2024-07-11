import os
import json

def check_env_vars() -> bool:
    VAR_LIST = [
        # API variables ___________________________________________________
        "TMDB_SESSION_TOKEN", 
        
        # S3 variables ____________________________________________________
        "S3_BUCKET_NAME",
        "S3_BUCKET_REGION",
        
        # AWS variables ___________________________________________________
        "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"
    ]
    
    for var in VAR_LIST:        
        print(f"Checking {var} variable...")
        if var not in os.environ: 
            print(f"Missing env variable: {var}.")
            return False
    
    return True 

def import_json(local_path: str) -> dict:
    with open(local_path) as file:
        return json.load(file)