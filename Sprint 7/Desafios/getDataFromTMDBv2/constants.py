# Enviroment constants _________________________________________________________
NEEDED_ENV_VARIABLES = [
    # API variables ___________________________________________________
    "TMDB_SESSION_TOKEN", 
    
    # S3 variables ____________________________________________________
    "S3_BUCKET_NAME",
    "S3_BUCKET_REGION",
    
    # AWS variables ___________________________________________________
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"
]

# API constants _______________________________________________________________
API_MOVIE_MODIFIER = "movies"
API_SERIES_MODIFIER = "series"
ID_PLACEHOLDER = "<id_placeholder>"

# S3 constants ________________________________________________________________
S3_OUTPUT_KEY = "Raw/TMDB/JSON"
CREATE_PROGRESS_JSON_ERROR_CODE = "NoSuchKey"

# Log Messages ________________________________________________________________
# Enviroment
LOG_ENV_VAR_CHECK = "Checking Environment Variables..."
LOG_ENV_VAR_MISSING = "Missing Environment Variables."

# Configuration JSON
LOG_JSON_CONFIG_IMPORT = "Importing Config JSON File..."
LOG_JSON_CONFIG_MISSING = "Missing Config JSON File."

# Boto3
LOG_JSON_PROGRESS_CREATE = "Progress JSON File Not Found in Specified Key, Creating New One..."
LOG_JSON_PROGRESS_UPDATE_ERROR = "Error Updating Progress JSON File."
LOG_JSON_PROGRESS_UPDATE_SUCCESS = "Progress JSON File Updated."
LOG_CREATE_S3_CLIENT = "Creating s3 Client..."


# TMDB Request
LOG_REQUEST_HEADER_CREATE = "Creating Request Header..."
LOG_REQUEST_CHECK_PROGRESS = "Checking Data Retrieve Progress..."
LOG_REQUEST_PROGRESS_COMPLETE = "No More Data to Request."