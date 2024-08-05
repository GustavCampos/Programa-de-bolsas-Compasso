# Python Standart Libs
import os
import sys
import json

# AWS Glue Libs
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame, functions as spk_func, Window
from pyspark.sql.types import *

# Glue Job Functions __________________________________________________________
def load_args(arg_list: list=None, file_path: str=None) -> dict:
    try:
        local = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(local, file_path)) as file:
            return json.load(file)
    except FileNotFoundError:
        return getResolvedOptions(sys.argv, arg_list)


# Pyspark functions ___________________________________________________________
def create_model_tables(spark: SparkSession) -> list[DataFrame]:
    dim_artist = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("tmdb_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("genre", StringType(), True),
            StructField("birth_year", IntegerType(), True),
            StructField("death_year", IntegerType(), True),
            StructField("occupation", ArrayType(StringType()), True),
            StructField("popularity", DoubleType(), True),
        ])                                
    )
    
    dim_date = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("complete_date", StringType(), True),
            StructField("decade", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("week", IntegerType(), True),
            StructField("day", IntegerType(), True)
        ])
    )
    
    dim_media = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("tmdb_id", LongType(), True),
            StructField("imdb_id", LongType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True),
            StructField("origin_country", ArrayType(StringType()), True),
            StructField("genres", ArrayType(StringType()), True)
        ])
    )
    
    fact_media_evaluation = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("media_id", LongType(), True),
            StructField("artist_id", LongType(), True),
            StructField("ingestion_date", LongType(), True),
            StructField("release_date", LongType(), True),
            StructField("end_date", LongType(), True),
            StructField("minute_duration", IntegerType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("budget", LongType(), True),
            StructField("revenue", LongType(), True),
            StructField("popularity", DoubleType(), True),
        ])
    )
    
    return [dim_artist, dim_date, dim_media, fact_media_evaluation]
   
def to_dim_date(spark_df: DataFrame, date_column: str, only_year: bool=False) -> DataFrame:
    if only_year:
        return spark_df.withColumn(
            "decade",
            spk_func.expr(f"FLOOR({date_column} / 10) * 10")
        ).select(
            spk_func
                .when(spk_func.col(date_column).isNull(), None)
                .when(spk_func.col(date_column) == "", None)
                .otherwise(
                    spk_func.concat_ws(" ", 
                        spk_func.lit("year only"), 
                        spk_func.col(date_column)
                    )    
                ).alias("complete_date"),
            spk_func.col("decade"),
            spk_func.col(date_column).alias("year"),
            spk_func.lit(None).alias("month"),
            spk_func.lit(None).alias("quarter"),
            spk_func.lit(None).alias("week"),
            spk_func.lit(None).alias("day")
        )

    return spark_df.withColumn(
        "decade",           
        spk_func.expr(f"FLOOR(YEAR({date_column}) / 10) * 10")
    ).select(
        spk_func
            .when(spk_func.col(date_column).isNull(), None)
            .when(spk_func.col(date_column) == "", None)
            .otherwise(spk_func.col(date_column))
            .alias("complete_date"),
        spk_func.col("decade"),
        spk_func.year(date_column).alias("year"),
        spk_func.month(date_column).alias("month"),
        spk_func.quarter(date_column).alias("quarter"),
        spk_func.weekofyear(date_column).alias("week"),
        spk_func.dayofmonth(date_column).alias("day")
    )

def main():
    # Loading Job Parameters __________________________________________________
    print("Loading Job Parameters...")
    
    ARGS_LIST = [
        "JOB_NAME", 
        "LOCAL_MOVIE_DATA_PATH", "LOCAL_SERIES_DATA_PATH",
        "TMDB_MOVIE_DATA_PATH", "TMDB_SERIES_DATA_PATH"
    ]
    
    ## @params: [JOB_NAME,LOCAL_MOVIE_DATA_PATH, LOCAL_SERIES_DATA_PATH, TMDB_MOVIE_DATA_PATH, TMDB_SERIES_DATA_PATH]
    args = load_args(arg_list=ARGS_LIST, file_path='job_params.json')
    
    # Creating Job Context ____________________________________________________
    print("Creating Job Context...")
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args[ARGS_LIST[0]], args)
    
    # Custom Code Start =======================================================
    # Setting Contants ________________________________________________________
    print("Setting Up Constants...")
    
    LOCAL_MOVIE_DATA_PATH = args[ARGS_LIST[1]]
    LOCAL_SERIES_DATA_PATH = args[ARGS_LIST[2]]
    TMDB_MOVIE_DATA_PATH = args[ARGS_LIST[3]]
    TMDB_SERIES_DATA_PATH = args[ARGS_LIST[4]]
    
    COLUMNS_TO_REMOVE = {
        "LOCAL": ["original_title", "character", "most_known_titles"],
        "TMDB": {
            "MOVIE": ["original_language", "original_title", "overview", "status"],
            "SERIES": [
                "created_by",
                "in_production",
                "languages",
                "number_of_episodes",
                "number_of_seasons",
                "original_language",
                "original_title",
                "status",
                "type"
            ]
        }
    }
    
    # Creating Dimensional Model Tables _______________________________________
    print("Generating Dimensional Model Tables...")
    dim_artist, dim_date, dim_media, fact_evaluation = create_model_tables(spark)
    
    # Import Data _____________________________________________________________
    print("Import Local Movie Data...")
    local_movie_df = spark.read.parquet(LOCAL_MOVIE_DATA_PATH)
    
    print("Import Local Series Data...")
    local_series_df = spark.read.parquet(LOCAL_SERIES_DATA_PATH)
    
    print("Import TMDB Movie Data...")
    tmdb_movie_df = spark.read.parquet(TMDB_MOVIE_DATA_PATH)
    
    print("Import TMDB Series Data...")
    tmdb_series_df = spark.read.parquet(TMDB_SERIES_DATA_PATH)
    
    # Cleaning Data ____________________________________________________________
    print("Cleaning Unuseful Columns...")
    dropped_local_movie_df = local_movie_df.drop(*COLUMNS_TO_REMOVE["LOCAL"])
    dropped_local_series_df = local_series_df.drop(*COLUMNS_TO_REMOVE["LOCAL"])
    dropped_tmdb_movie_df = tmdb_movie_df.drop(*COLUMNS_TO_REMOVE["TMDB"]["MOVIE"])
    dropped_tmdb_series_df = tmdb_series_df.drop(*COLUMNS_TO_REMOVE["TMDB"]["SERIES"])
    
    # Creating dim_date _______________________________________________________
    print("Extracting Data for dim_date...")
    
    # For Local Movies
    local_movie_dim_date = to_dim_date(dropped_local_movie_df, "ingestion_date")
    local_movie_dim_date = local_movie_dim_date.union(to_dim_date(dropped_local_movie_df, "release_year", True))
    
    # For Local Series
    local_series_dim_date = to_dim_date(dropped_local_series_df, "ingestion_date")
    local_series_dim_date = local_series_dim_date.union(to_dim_date(dropped_local_series_df, "release_year", True))
    local_series_dim_date = local_series_dim_date.union(to_dim_date(dropped_local_series_df, "end_year", True))
    
    # For TMDB Movies
    tmdb_movie_dim_date = to_dim_date(dropped_tmdb_movie_df, "ingestion_date")
    tmdb_movie_dim_date = tmdb_movie_dim_date.union(to_dim_date(dropped_tmdb_movie_df, "release_date"))
    
    # For TMDB Series
    tmdb_series_dim_date = to_dim_date(dropped_tmdb_series_df, "ingestion_date")
    tmdb_series_dim_date = tmdb_series_dim_date.union(to_dim_date(dropped_tmdb_series_df, "release_date"))
    tmdb_series_dim_date = tmdb_series_dim_date.union(to_dim_date(dropped_tmdb_series_df, "end_date"))
        

    print("Adding data to dim_date...")

    # Union all dim_date
    dim_date = dim_date.union(
        local_movie_dim_date
            .union(local_series_dim_date)
            .union(tmdb_series_dim_date)
            .union(tmdb_movie_dim_date)
            .drop_duplicates()
            .orderBy(
                spk_func.when(spk_func.col("year").isNull(), 1).otherwise(0),
                spk_func.when(spk_func.col("month").isNull(), 1).otherwise(0),
                spk_func.when(spk_func.col("day").isNull(), 1).otherwise(0),
                spk_func.col("year"),
                spk_func.col("month"),
                spk_func.col("day")
            ).select(
                spk_func.monotonically_increasing_id().alias("id"),
                spk_func.col("*")
            )
    )
    
    print("Date Dimension Complete!")

    # Creating dim_artist _____________________________________________________
    local_movie_dim_artist = dropped_local_movie_df.select(
        spk_func.lit(None).alias("tmdb_id"),
        spk_func.lower(spk_func.col("artist_name")).alias("name"),
        spk_func
            .when(spk_func.col("artist_genre") == "M", "male")
            .when(spk_func.col("artist_genre") == "F", "female")
            .otherwise(None).alias("genre"),
        spk_func.col("birth_year").alias("birth_year"),
        spk_func.col("death_year").alias("death_year"),
        spk_func.col("occupation").alias("occupation"),
        spk_func.lit(None).alias("popularity")
    )
    
    local_series_dim_artist = dropped_local_series_df.select(
        spk_func.lit(None).alias("tmdb_id"),
        spk_func.lower(spk_func.col("artist_name")).alias("name"),
        spk_func
            .when(spk_func.col("artist_genre") == "M", "male")
            .when(spk_func.col("artist_genre") == "F", "female")
            .otherwise(None).alias("genre"),
        spk_func.col("birth_year").alias("birth_year"),
        spk_func.col("death_year").alias("death_year"),
        spk_func.col("occupation").alias("occupation"),
        spk_func.lit(None).alias("popularity")
    )
    
    # tmdb_movie_dim_date = dropped_tmdb_movie_df.select(
        
    # )
    
    dropped_tmdb_movie_df.select(spk_func.explode(spk_func.col("credits.cast"))).show()

    # # Creating dim_media ______________________________________________________
    # local_movie_dim_media = dropped_local_movie_df.select(
    #     spk_func.lit(None).alias("tmdb_id"),
    #     spk_func.col("id").alias("imdb_id"),
    #     spk_func.lit("movie").alias("type"),
    #     spk_func.lower(spk_func.col("title")).alias("title"),
    #     spk_func.lit(None).alias("origin_country"),
    #     spk_func.col("genre").alias("genre")
    # ).drop_duplicates()
    
    # Custom Code End =========================================================
    job.commit()

if __name__ == "__main__":
    main()