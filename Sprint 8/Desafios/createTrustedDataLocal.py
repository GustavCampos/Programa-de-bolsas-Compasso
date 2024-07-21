# Python Standard Libs
import sys
import json
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

# Custom Classes That Serve as Constants ==========================================================
class COLUMNS:
    ARTIST_GENRE = "artist_genre"
    ARTIST_NAME = "artist_name"
    BIRTH_YEAR = "birth_year"
    CHARACTER = "character"
    DEATH_YEAR = "death_year"
    END_YEAR = "end_year"
    GENRE = "genre"
    ID = "id"
    INGESTION_DATE = "ingestion_date"
    MINUTE_DURATION = "minute_duration"
    MOST_KNOWN_TITLES = "most_known_titles"
    OCCUPATION = "occupation"
    ORIGINAL_TITLE = "original_title"
    RELEASE_YEAR = "release_year"
    TITLE = "title"
    VOTE_AVERAGE = "vote_average"
    VOTE_COUNT = "vote_count"

    class OLD:
        ARTIST_GENRE = "generoArtista"
        ARTIST_NAME = "nomeArtista"
        BIRTH_YEAR = "anoNascimento"
        CHARACTER = "personagem"
        DEATH_YEAR = "anoFalecimento"
        END_YEAR = "anoTermino"
        GENRE = "genero"
        MINUTE_DURATION = "tempoMinutos"
        MOST_KNOWN_TITLES = "titulosMaisConhecidos"
        OCCUPATION = "profissao"
        ORIGINAL_TITLE = "tituloOriginal"
        RELEASE_YEAR = "anoLancamento"
        TITLE = "tituloPincipal"
        VOTE_AVERAGE = "notaMedia"
        VOTE_COUNT = "numeroVotos"

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

# Spark Functions _____________________________________________________________
def map_columns_df(spark_df: DataFrame, mapping: list, null_symbol: str="None") -> DataFrame:        
    # Return StringType() value
    evaluate_null = spk_func.udf(
        lambda string: ((string, None)[string == null_symbol])
    )
    
    return_df = spark_df
    for name, new_name, col_type in mapping:
        cast_to_array = col_type.upper().startswith("ARRAY")
        
        create_column_df = return_df.withColumn(new_name, evaluate_null(spk_func.col(name)))
        
        if cast_to_array:
            create_column_df = create_column_df.withColumn(
                colName=new_name, 
                col=spk_func.split(spk_func.col(new_name), ",")
            )

        cast_df = create_column_df.withColumn(new_name, spk_func.col(new_name).cast(col_type))
        
        return_df = cast_df.drop(name)
        
    return return_df

# Glue Job Functions __________________________________________________________
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
        with_date_df = obj_df.withColumn(COLUMNS.INGESTION_DATE, spk_func.lit(obj_date))
        
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
    # S3 Paths
    S3_MOVIE_INPUT_PATH = args[ARGS_LIST[1]]
    S3_SERIES_INPUT_PATH = args[ARGS_LIST[2]]
    S3_TARGET_PATH = args[ARGS_LIST[3]]
    
    # Glue Dynamic Frame Import
    FORMAT_FILE = "csv"
    FORMAT_OPTIONS = {"withHeader": True, "separator": "|"}
    
    # Data Processing
    NULL_SYMBOL = f"\\N"
    
    # Data Mapping
    MOVIE_MAPPING = [
        (COLUMNS.OLD.TITLE,             COLUMNS.TITLE,              "STRING"),
        (COLUMNS.OLD.ORIGINAL_TITLE,    COLUMNS.ORIGINAL_TITLE,     "STRING"),
        (COLUMNS.OLD.RELEASE_YEAR,      COLUMNS.RELEASE_YEAR,       "INT"), 
        (COLUMNS.OLD.MINUTE_DURATION,   COLUMNS.MINUTE_DURATION,    "INT"),
        (COLUMNS.OLD.GENRE,             COLUMNS.GENRE,              "ARRAY<STRING>"),
        (COLUMNS.OLD.VOTE_AVERAGE,      COLUMNS.VOTE_AVERAGE,       "FLOAT"),
        (COLUMNS.OLD.VOTE_COUNT,        COLUMNS.VOTE_COUNT,         "INT"),
        (COLUMNS.OLD.CHARACTER,         COLUMNS.CHARACTER,          "STRING"),
        (COLUMNS.OLD.ARTIST_NAME,       COLUMNS.ARTIST_NAME,        "STRING"),
        (COLUMNS.OLD.ARTIST_GENRE,      COLUMNS.ARTIST_GENRE,       "STRING"),
        (COLUMNS.OLD.DEATH_YEAR,        COLUMNS.DEATH_YEAR,         "INT"),
        (COLUMNS.OLD.BIRTH_YEAR,        COLUMNS.BIRTH_YEAR,         "INT"),
        (COLUMNS.OLD.OCCUPATION,        COLUMNS.OCCUPATION,         "ARRAY<STRING>"),
        (COLUMNS.OLD.MOST_KNOWN_TITLES, COLUMNS.MOST_KNOWN_TITLES,  "ARRAY<STRING>"),
    ]
    
    SERIES_MAPPING = [
        *MOVIE_MAPPING,
        (COLUMNS.OLD.END_YEAR, COLUMNS.END_YEAR, "INT")
    ]
    
    UDF_MAP_ARTIST_GENRE = spk_func.udf(
        lambda genre: {"actor": "M", "actress": "F"}.get(genre, None) 
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
        file_format=    FORMAT_FILE,
        format_options= FORMAT_OPTIONS
    )
    print("Movie Data Import Complete!")
    movies_df.printSchema()
    
    print(f"Movie Data: Mapping {movies_df.count()} Rows...")
    map_artist_genre_df = movies_df.withColumn(COLUMNS.OLD.ARTIST_GENRE,
        UDF_MAP_ARTIST_GENRE(spk_func.col(COLUMNS.OLD.ARTIST_GENRE))
    )
    
    mapped_movies_df = map_columns_df(map_artist_genre_df, MOVIE_MAPPING, NULL_SYMBOL)
    
    print("Movie Data Mapped!")
    mapped_movies_df.printSchema()
        
    print(f"Writing Movie Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}Local/Movies/"
    mapped_movies_df.write.mode("overwrite").partitionBy(COLUMNS.INGESTION_DATE).parquet(s3_path)
    print(f"Movie Data Write Complete!")
    
    # Series Data Treatment ___________________________________________________
    print("Importing Series Data...")
    series_df = generate_unified_df(
        s3_client=      s3_client,
        glue_context=   glueContext,
        s3_path=        S3_SERIES_INPUT_PATH,
        file_format=    FORMAT_FILE,
        format_options= FORMAT_OPTIONS
    )
    print("Series Data Import Complete!")
    series_df.printSchema()
    
    print(f"Series Data: Mapping {series_df.count()} Rows...")
    map_artist_genre_df = series_df.withColumn(COLUMNS.OLD.ARTIST_GENRE,
        UDF_MAP_ARTIST_GENRE(spk_func.col(COLUMNS.OLD.ARTIST_GENRE))
    )
    
    mapped_series_df = map_columns_df(map_artist_genre_df, SERIES_MAPPING, NULL_SYMBOL)
    
    print("Series Data Mapped!")
    mapped_series_df.printSchema()
    
    print(f"Writing Series Data On {S3_TARGET_PATH}")
    s3_path = f"{S3_TARGET_PATH}Local/Series/"
    mapped_series_df.write.mode("overwrite").partitionBy(COLUMNS.INGESTION_DATE).parquet(s3_path)
    print(f"Series Data Write Complete!")
    
    # Custom Code End =============================================================================
    job.commit()
        
if __name__ == "__main__":
    main()
