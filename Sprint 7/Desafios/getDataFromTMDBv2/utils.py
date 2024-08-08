import os
import json
import constants

def check_env_vars() -> bool:
    for var in constants.NEEDED_ENV_VARIABLES:
        print(f"Checking {var} variable...")
        if var not in os.environ: 
            print(f"Missing env variable: {var}.")
            return False
    
    return True 

def import_json(local_path: str) -> dict:
    with open(local_path) as file:
        return json.load(file)