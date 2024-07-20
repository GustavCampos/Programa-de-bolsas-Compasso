
# Python Standard Libs
import sys
import json
import requests
from os.path import dirname, realpath, join as path_join
from re import search as re_search

# Boto3
import boto3

# AWS Glue Libs
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as spk_func
from pyspark.sql.types import StringType, ArrayType


# Custom Functions ================================================================================
# S3 key manipulation functions _______________________________________________
def uri_to_bucket_name(uri: str) -> str:
    return uri.split("/")[2]

def uri_to_s3_key(uri: str) -> str:
    return "/".join(uri.split("/")[3:])

def s3_key_to_uri(obj_key: str, bucket_name=str) -> str:
    return f's3://{bucket_name}/{obj_key}'

def s3_key_to_date(obj_key: str) -> str:
    rule = r"\d{4}/\d{2}/\d{2}/[\w|.]+$"
    date = re_search(rule, obj_key).group()
    return '-'.join(date.split("/")[:-1])

# Request Functions __________________________________________________________
def get_genre_map(url: str, header: dict) -> dict:
    response = requests.get(url=url, headers=header)
    genres_list =response.json().get("genres")
    return {genre["id"]: genre["name"] for genre in genres_list}

# Spark Functions _____________________________________________________________
def map_array_elements(array: list|None, mapping_dict: dict) -> list | None:
    if array is None: return None
    return [mapping_dict.get(element) for element in array]

def rename_columns(spark_df: DataFrame, mapping: list) -> DataFrame:
    return_df = spark_df
    
    for old_name, new_name in mapping:
        return_df = return_df.withColumnRenamed(old_name, new_name)
    
    return return_df

# Glue Job Functions _________________________________________________________
def load_args(arg_list: list=None, file_path: str=None) -> dict:
    try:
        return getResolvedOptions(sys.argv, arg_list)
    except GlueArgumentError:
        local = dirname(realpath(__file__))
        with open(path_join(local, file_path)) as file:
            return json.load(file)

def generate_unified_df(glue_context: GlueContext, s3_client: boto3.client, s3_path: str,  
                        file_format: str, format_options: dict) -> DataFrame:
    print(f'Getting Object List In {s3_path}')
    bucket_name = uri_to_bucket_name(s3_path)
    
    response = s3_client.list_objects(
        Bucket=bucket_name, 
        Prefix=uri_to_s3_key(s3_path)    
    )

    wanted_objects = list(filter(lambda n: n["Key"].endswith(f".{file_format}"), response["Contents"]))
    wanted_keys_size = len(wanted_objects)
    print(f"{wanted_keys_size} Objects Found In {s3_path}!")

    # Importing Found Objects _________________________________________________ 
    print("Loading Found Files...")
    
    unified_df = None
    for obj in wanted_objects:
        obj_key = obj["Key"]
        obj_date = s3_key_to_date(obj_key)

        print(f"Loading {obj_key}")

        obj_dyf = glue_context.create_dynamic_frame.from_options(
            connection_type=        "s3",
            connection_options=     {"paths": [s3_key_to_uri(obj_key, bucket_name)]},
            format=                 file_format,
            format_options=         format_options
        )

        obj_df = obj_dyf.toDF()
        with_date_df = obj_df.withColumn("IngestionDate", spk_func.lit(obj_date).cast("date"))
        
        if unified_df is None:
            unified_df = with_date_df
        else:
            unified_df = unified_df.union(with_date_df)

    return unified_df

    
# Main Function ================================================================================
def main():
    # Loading Job Parameters __________________________________________________
    print("Loading Job Parameters...")

    ARGS_LIST = [
        "JOB_NAME",
        "S3_MOVIE_INPUT_PATH", 
        "S3_SERIES_INPUT_PATH", 
        "S3_TARGET_PATH",
        "TMDB_TOKEN"
    ]

    ## @params: [JOB_NAME, S3_MOVIE_INPUT_PATH, S3_SERIES_INPUT_PATH, S3_TARGET_PATH, TMDB_TOKEN]
    args = load_args(ARGS_LIST, "createTrustedDataTMDB_parameters.json")
    
    # Creating Job Context ____________________________________________________
    print("Creating Job Context...")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args[ARGS_LIST[0]], args)
    
    # Custom Code ===============================================================================
    # Setting Constants _______________________________________________________
    # S3 Paths
    S3_MOVIE_INPUT_PATH = args[ARGS_LIST[1]]
    S3_SERIES_INPUT_PATH = args[ARGS_LIST[2]]
    S3_TARGET_PATH = args[ARGS_LIST[3]]
    
    # Glue Dynamic Frame Options
    FILE_FORMAT = "json"
    FORMAT_OPTIONS = {"jsonPath": "$.results.*"}
    
    # TMDB Requests
    TMDB_TOKEN = args[ARGS_LIST[4]]
    TMDB_HEADER = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_TOKEN}"
    }
    
    TMDB_GENRE_URL = "https://api.themoviedb.org/3/genre/<type>/list"
    MOVIE_GENRE_URL = TMDB_GENRE_URL.replace("<type>", "movie")
    SERIES_GENRE_URL = TMDB_GENRE_URL.replace("<type>", "tv")
    
    print("Requesting TMDB Genre Maps...")
    MOVIE_GENRES = get_genre_map(MOVIE_GENRE_URL, TMDB_HEADER)    
    SERIES_GENRES = get_genre_map(SERIES_GENRE_URL, TMDB_HEADER)
    
    # Creating S3 Client ______________________________________________________
    print('Creating S3 Client...')
    s3_client = boto3.client("s3")

    # Movie Data Treatment ____________________________________________________
    print("Importing Movies Data...")
    movies_df = generate_unified_df(
        glue_context=   glueContext,
        s3_client=      s3_client,
        s3_path=        S3_MOVIE_INPUT_PATH,
        file_format=    FILE_FORMAT,
        format_options= FORMAT_OPTIONS
    )
    print("Movie Data Import Complete!")
    movies_df.printSchema()

    print("Movie Data: Dropping Irrelevant Columns...")
    columns_to_remove = ["adult", "backdrop_path", "poster_path", "video"]
    dropped_movies_df = movies_df.drop(*columns_to_remove)
    
    print(f"Movie Data: Mapping {dropped_movies_df.count()} Rows...")
    map_movie_genres_udf = spk_func.udf(
        lambda id_list:
            [MOVIE_GENRES.get(g_id) for g_id in id_list], 
        ArrayType(StringType())
    )
    mapped_genres_movies_df = dropped_movies_df.withColumn(
        colName="genre_ids", 
        col=map_movie_genres_udf(spk_func.col("genre_ids"))
    )
    
    renamed_movies_df = rename_columns(
        mapped_genres_movies_df, 
        [("genre_ids", "genres"), ("IngestionDate", "ingestion_date")]
    )
    
    print("Movie Data Mapped!")
    renamed_movies_df.printSchema()
    
    print(f"Writing Movie Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}TMDB/Movies/"
    renamed_movies_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(s3_path)
    print(f"Movie Data Write Complete!")
    
    # Series Data Treatment ____________________________________________________
    print("Importing Series Data...")
    series_df = generate_unified_df(
        glue_context=   glueContext,
        s3_client=      s3_client,
        s3_path=        S3_SERIES_INPUT_PATH,
        file_format=    FILE_FORMAT,
        format_options= FORMAT_OPTIONS
    )
    print("Series Data Import Complete!")
    series_df.printSchema()
    
    print("Series Data: Dropping Irrelevant Columns...")
    columns_to_remove = ["adult", "backdrop_path", "poster_path"]
    dropped_series_df = series_df.drop(*columns_to_remove)
    
    print(f"Series Data: Mapping {dropped_series_df.count()} Rows...")
    map_series_genres_udf = spk_func.udf(
        lambda id_list:
            [SERIES_GENRES.get(g_id) for g_id in id_list], 
        ArrayType(StringType())
    )
    mapped_genres_series_df = dropped_series_df.withColumn(
        colName="genre_ids", 
        col=map_series_genres_udf(spk_func.col("genre_ids"))
    )
    
    renamed_series_df = rename_columns(mapped_genres_series_df, [
        ("name",            "title"),
        ("original_name",   "original_title"),
        ("genre_ids",       "genres"),
        ("IngestionDate",   "ingestion_date"),
    ])
    
    print("Series Data Mapped!")
    renamed_series_df.printSchema()
    
    print(f"Writing Series Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}TMDB/Series/"
    renamed_series_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(s3_path)
    print(f"Series Data Write Complete!")
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
