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
from awsglue.utils import getResolvedOptions
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
def rename_columns(spark_df: DataFrame, mapping: list) -> DataFrame:
    return_df = spark_df
    
    for old_name, new_name in mapping:
        return_df = return_df.withColumnRenamed(old_name, new_name)
    
    return return_df

# Glue Job Functions _________________________________________________________
def load_args(arg_list: list=None, file_path: str=None) -> dict:
    try:
        local = dirname(realpath(__file__))
        with open(path_join(local, file_path)) as file:
            return json.load(file)
    except FileNotFoundError:
        return getResolvedOptions(sys.argv, arg_list)

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
        with_date_df = obj_df.withColumn("ingestion_date", spk_func.lit(obj_date).cast("date"))
        
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
        "S3_TARGET_PATH"
    ]

    ## @params: [JOB_NAME, S3_MOVIE_INPUT_PATH, S3_SERIES_INPUT_PATH, S3_TARGET_PATH, TMDB_TOKEN]
    args = load_args(ARGS_LIST, "job_params.json")
    
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
        
    # Data Mapping
    COLUMN_GENRES = "genres"
    COLUMN_INGESTION_DATE = "ingestion_date"
    
    
    COLUMNS_TO_REMOVE = [
        "adult", "backdrop_path", "poster_path", 
        "homepage", "spoken_languages", "tagline",
        "production_companies", "production_countries"
    ]
    
    COLUMNS_TO_REMOVE_MOVIES = [*COLUMNS_TO_REMOVE, "belongs_to_collection", "video"]
    
    COLUMNS_TO_REMOVE_SERIES = [*COLUMNS_TO_REMOVE,
        "episode_run_time", "last_episode_to_air", 
        "next_episode_to_air", "networks", "seasons",
    ]
    
    # UDF Functions
    MAP_GENRES_UDF = spk_func.udf(
        lambda genre_list: [genre_dict["name"] for genre_dict in genre_list], 
        ArrayType(StringType())
    )
        
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
    dropped_movies_df = movies_df.drop(*COLUMNS_TO_REMOVE_MOVIES)
    
    print(f"Movie Data: Mapping {dropped_movies_df.count()} Rows...")
    mapped_movies_df = dropped_movies_df.withColumn(COLUMN_GENRES,
        MAP_GENRES_UDF(spk_func.col(COLUMN_GENRES))
    )
    
    print("Movie Data Mapped!")
    mapped_movies_df.printSchema()
    
    print(f"Writing Movie Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}TMDB/DetailedMovies/"
    mapped_movies_df.write.mode("overwrite").partitionBy(COLUMN_INGESTION_DATE).parquet(s3_path)
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
    dropped_series_df = series_df.drop(*COLUMNS_TO_REMOVE_SERIES)
    
    print(f"Series Data: Mapping {dropped_series_df.count()} Rows...")
    renamed_series_df = rename_columns(dropped_series_df, [
        ("name",            "title"), 
        ("original_name",   "original_title"), 
        ("first_air_date",  "release_date"), 
        ("last_air_date",   "end_date")
    ])
    
    mapped_series_df = renamed_series_df.withColumn(COLUMN_GENRES,
        MAP_GENRES_UDF(spk_func.col(COLUMN_GENRES))
    )
    
    print("Series Data Mapped!")
    mapped_series_df.printSchema()
    
    print(f"Writing Series Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}TMDB/DetailedSeries/"
    mapped_series_df.write.mode("overwrite").partitionBy(COLUMN_INGESTION_DATE).parquet(s3_path)
    print(f"Series Data Write Complete!")
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
