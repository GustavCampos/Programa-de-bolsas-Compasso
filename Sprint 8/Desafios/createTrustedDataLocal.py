# Python Standard Libs
import sys
import json
from os.path import dirname, realpath, join as path_join
from re import search as re_search

# Boto3
import boto3
from botocore.exceptions import ClientError

# AWS Glue Libs
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions, GlueArgumentError
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as spk_func


# Custom Functions ================================================================================
# S3 key manipulation functions _______________________________________________
def url_to_bucket_name(url: str) -> str:
    return url.split("/")[2]

def url_to_s3_key(url: str) -> str:
    return "/".join(url.split("/")[3:])

def s3_key_to_url(obj_key: str, bucket_name=str) -> str:
    return f's3://{bucket_name}/{obj_key}'

def s3_key_to_date(obj_key: str) -> str:
    rule = r"\d{4}/\d{2}/\d{2}/[\w|.]+$"
    date = re_search(rule, obj_key).group()
    return '-'.join(date.split("/")[:-1])

# Spark Functions _____________________________________________________________
def map_columns_df(spark_df: DataFrame, mapping: list) -> DataFrame:
    return_df = spark_df
    for name, new_name, col_type in mapping:
        return_df = return_df.withColumn(new_name,
            spk_func.when(spk_func.col(name) == "\\N", None)
                .otherwise(spk_func.col(name))
                .cast(col_type)
        )
        
        return_df = return_df.drop(name)
        
    return return_df

# Glue Job Functions __________________________________________________________
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
    bucket_name = url_to_bucket_name(s3_path)
    
    response = s3_client.list_objects(
        Bucket=bucket_name, 
        Prefix=url_to_s3_key(s3_path)    
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
            connection_options=     {"paths": [s3_key_to_url(obj_key, bucket_name)]},
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
        "S3_TARGET_PATH"
    ]

    ## @params: [JOB_NAME, S3_MOVIE_INPUT_PATH, S3_SERIES_INPUT_PATH, S3_TARGET_PATH]
    args = load_args(ARGS_LIST, "createTrustedDataLocal_parameters.json")
    
    # Creating Job Context ____________________________________________________
    print("Creating Job Context...")
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args[ARGS_LIST[0]], args)
    
    # # Custom Code ===============================================================================
    # Setting Constants _______________________________________________________ 
    S3_MOVIE_INPUT_PATH = args[ARGS_LIST[1]]
    S3_SERIES_INPUT_PATH = args[ARGS_LIST[2]]
    S3_TARGET_PATH = args[ARGS_LIST[3]]
    
    # Creating S3 Client ______________________________________________________
    print('Creating S3 Client...')
    s3_client = boto3.client("s3")

    # Movie Data Treatment ____________________________________________________
    print("Importing Movies Data...")
    movies_df = generate_unified_df(
        glue_context=   glueContext,
        s3_client=      s3_client,
        s3_path=        S3_MOVIE_INPUT_PATH,
        file_format=    "csv",
        format_options= {"withHeader": True, "separator": "|"}
    )
    print("Movie Data Import Complete!")
    movies_df.printSchema()
    
    print(f"Movie Data: Mapping {movies_df.count()} Rows...")
    mapped_movies_df = map_columns_df(movies_df, [
        ("id", "id", "STRING"),
        ("tituloPincipal", "title", "STRING"),
        ("tituloOriginal", "original_title", "STRING"),
        ("anoLancamento", "release_year", "INT"), 
        ("tempoMinutos", "minute_duration", "INT"),
        ("genero", "genre", "STRING"),
        ("notaMedia", "vote_average", "FLOAT"),
        ("numeroVotos", "vote_count", "INT"),
        ("personagem", "character", "STRING"),
        ("nomeArtista", "artist_name", "STRING"),
        ("generoArtista", "artist_genre", "STRING"),
        ("anoNascimento", "birth_year", "INT"),
        ("anoFalecimento", "death_year", "INT"),
        ("profissao", "occupation", "STRING"),
        ("titulosMaisConhecidos", "most_known_titles", "STRING"),
        ("IngestionDate", "ingestion_date", "DATE")
    ])
    
    print("Movie Data Mapped!")
    mapped_movies_df.printSchema()
    
    print(f"Writing Movie Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}Local/Movies/"
    mapped_movies_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(s3_path)
    print(f"Movie Data Write Complete!")
    
    # Series Data Treatment ___________________________________________________
    print("Importing Series Data...")
    series_df = generate_unified_df(
        s3_client=      s3_client,
        glue_context=   glueContext,
        s3_path=        S3_SERIES_INPUT_PATH,
        file_format=    "csv",
        format_options= {"withHeader": True, "separator": "|"}
    )
    print("Series Data Import Complete!")
    series_df.printSchema()
    
    print(f"Series Data: Mapping {series_df.count()} Rows...")
    mapped_series_df = map_columns_df(series_df, [
        ("id", "id", "INT"),
        ("tituloPincipal", "title", "STRING"),
        ("tituloOriginal", "original_title", "STRING"),
        ("anoLancamento", "release_year", "INT"),
        ("anoTermino", "end_year", "STRING"),
        ("tempoMinutos", "minute_duration", "INT"),
        ("genero", "genre", "STRING"),
        ("notaMedia", "vote_average", "FLOAT"),
        ("numeroVotos", "vote_count", "INT"),
        ("generoArtista", "artist_genre", "STRING"),
        ("personagem", "character", "STRING"),
        ("nomeArtista", "artist_name", "STRING"),
        ("anoNascimento", "birth_year", "INT"),
        ("anoFalecimento", "death_year", "INT"),
        ("profissao", "occupation", "STRING"),
        ("titulosMaisConhecidos", "most_known_titles", "STRING"),
        ("IngestionDate", "ingestion_date", "DATE")
    ])
    
    print("Series Data Mapped!")
    mapped_movies_df.printSchema()
    
    print(f"Writing Series Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}Local/Series/"
    mapped_series_df.write.mode("overwrite").partitionBy("ingestion_date").parquet(s3_path)
    print(f"Series Data Write Complete!")
    
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
