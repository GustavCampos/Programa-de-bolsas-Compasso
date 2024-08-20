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
    dim_occupation = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True)
        ])
    )
    
    dim_country = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("code", StringType(), True),
        ])
    )
    
    dim_genre = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
        ])
    )
    
    dim_media = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("tmdb_id", LongType(), True),
            StructField("imdb_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("title", StringType(), True)
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
    
    dim_people = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("tmdb_id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("birth_year", IntegerType(), True),
            StructField("death_year", IntegerType(), True),
            StructField("occupation", StringType(), True)
        ])
    )
    
    fme = spark.createDataFrame(spark.sparkContext.emptyRDD(),
        StructType([
            StructField("id", LongType(), False),
            StructField("ingestion_date", LongType(), True),
            StructField("release_date", LongType(), True),
            StructField("end_date", LongType(), True),
            StructField("media_id", LongType(), True),
            StructField("genre_id", LongType(), True),
            StructField("origin_country_id", LongType(), True),
            StructField("people_id", LongType(), True),
            StructField("minute_duration", IntegerType(), True),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("budget", IntegerType(), True),
            StructField("revenue", IntegerType(), True),
            StructField("popularity", DoubleType(), True)
        ])
    )
    
    return (fme, dim_people, dim_date, dim_media, dim_genre, dim_country, dim_occupation)
   
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
        "TMDB_MOVIE_DATA_PATH", "TMDB_SERIES_DATA_PATH",
        "S3_TARGET_PATH"
    ]
    
    ## @params: [JOB_NAME,LOCAL_MOVIE_DATA_PATH, LOCAL_SERIES_DATA_PATH, TMDB_MOVIE_DATA_PATH, TMDB_SERIES_DATA_PATH, S3_TARGET_PATH]
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
    S3_TARGET_PATH = args[ARGS_LIST[5]]
    
    # S3 Paths
    RESULT_FOLDER_PATH = f"{S3_TARGET_PATH}movies_and_series_dw/"
    DIM_MEDIA_PATH = f"{RESULT_FOLDER_PATH}dim_media/"
    DIM_PEOPLE_PATH = f"{RESULT_FOLDER_PATH}dim_people/"
    DIM_DATE_PATH = f"{RESULT_FOLDER_PATH}dim_date/"
    FACT_MEDIA_EVALUATION_PATH = f"{RESULT_FOLDER_PATH}fact_media_evaluation/"
    
    # Columns constants
    COL_RELEASE_DATE    = "release_date"
    COL_END_DATE        = "end_date"
    COL_INGESTION_DATE  = "ingestion_date"
    COL_TMDB_ID         = "tmdb_id"
    COL_IMDB_ID         = "imdb_id"
    COL_TYPE            = "type"
    COL_TITLE           = "title"
    COL_GENRE           = "genre"
    COL_OG_COUNTRY      = "origin_country"
    COL_P_TMDB_ID       = "people_tmdb_id"
    COL_P_NAME          = "people_name"
    COL_P_GENDER        = "people_gender"
    COL_BIRTH_YEAR      = "birth_year"
    COL_DEATH_YEAR      = "death_year"
    COL_OCCUPATION      = "occupation"
    COL_MINUTE_DURATION = "minute_duration"
    COL_VOTE_AVERAGE    = "vote_average"
    COL_VOTE_COUNT      = "vote_count"
    COL_BUDGET          = "budget"
    COL_REVENUE         = "revenue"
    COL_POPULARITY      = "popularity"
    
    
    # Media Type
    MEDIA_TYPE_MOVIE = "movie"
    MEDIA_TYPE_SERIE = "series"
    
    # Creating Dimensional Model Tables _______________________________________
    print("Generating Dimensional Model Tables...")
    data_frames = create_model_tables(spark)
    fact_media_evaluation   = data_frames[0] 
    dim_people              = data_frames[1] 
    dim_date                = data_frames[2] 
    dim_media               = data_frames[3] 
    dim_genre               = data_frames[4] 
    dim_country             = data_frames[5]
    dim_occupation          = data_frames[6]
    
    # Processing Local Data _____________________________________________________________
    print("Import Local Data...")
    print(f"Getting data from {LOCAL_MOVIE_DATA_PATH}")
    local_movie_df = spark.read.parquet(LOCAL_MOVIE_DATA_PATH)
    
    print(f"Getting data from {LOCAL_SERIES_DATA_PATH}")
    local_series_df = spark.read.parquet(LOCAL_SERIES_DATA_PATH)
    
    print("Unifying Movie and Series Local Data...")
    local_df = (
        local_movie_df.select(
            spk_func.col("id").alias(COL_IMDB_ID),
            spk_func.col(COL_TITLE),
            spk_func.col("release_year"),
            spk_func.col(COL_MINUTE_DURATION),
            spk_func.col(COL_GENRE),
            spk_func.col(COL_VOTE_AVERAGE),
            spk_func.col(COL_VOTE_COUNT),
            spk_func.col("artist_name").alias(COL_P_NAME),
            spk_func.col("artist_genre").alias(COL_P_GENDER),
            spk_func.col(COL_DEATH_YEAR),
            spk_func.col(COL_BIRTH_YEAR),
            spk_func.col(COL_OCCUPATION),
            spk_func.col(COL_INGESTION_DATE),
            spk_func.lit(MEDIA_TYPE_MOVIE).alias(COL_TYPE)
        ).unionByName(local_series_df.select(
            spk_func.col("id").alias(COL_IMDB_ID),
            spk_func.col(COL_TITLE),
            spk_func.col("release_year"),
            spk_func.col(COL_MINUTE_DURATION),
            spk_func.col(COL_GENRE),
            spk_func.col(COL_VOTE_AVERAGE),
            spk_func.col(COL_VOTE_COUNT),
            spk_func.col("artist_name").alias(COL_P_NAME),
            spk_func.col("artist_genre").alias(COL_P_GENDER),
            spk_func.col(COL_DEATH_YEAR),
            spk_func.col(COL_BIRTH_YEAR),
            spk_func.col(COL_OCCUPATION),
            spk_func.col("end_year"),
            spk_func.col(COL_INGESTION_DATE),
            spk_func.lit(MEDIA_TYPE_SERIE).alias(COL_TYPE)
        ), allowMissingColumns=True)
    )
    
    print("Local data unified!")
    local_df.printSchema()
    
    print("Exploding Local Data...")
    exploded_local_df = (
        local_df
            .withColumn(COL_VOTE_AVERAGE, spk_func.round(spk_func.col(COL_VOTE_AVERAGE), 2))
            
            .withColumn(COL_TITLE, spk_func.lower(COL_TITLE))
            
            .withColumn(COL_P_NAME, spk_func.lower(COL_P_NAME))
            
            .withColumn(COL_P_GENDER, spk_func
                .when(spk_func.col(COL_P_GENDER) == "M", "male")
                .when(spk_func.col(COL_P_GENDER) == "F", "female")
                .otherwise(None)
            )
            
            .withColumn(COL_GENRE, spk_func.explode_outer(COL_GENRE))
            .withColumn(COL_GENRE, spk_func.lower(COL_GENRE))
            
            .withColumn(COL_OCCUPATION, spk_func.explode_outer(COL_OCCUPATION))
            .withColumn(COL_OCCUPATION, spk_func
                .when(spk_func.col(COL_OCCUPATION) == "", None)
                .otherwise(spk_func.col(COL_OCCUPATION))
            ).withColumn(COL_OCCUPATION, 
                spk_func.regexp_replace(spk_func.col(COL_OCCUPATION), "_", " ")
            )
            
            .withColumnRenamed("release_year", COL_RELEASE_DATE)
            .withColumnRenamed("end_year", COL_END_DATE)
    )
    
    exploded_local_df.show(5)

    # Processing TMDB Data ____________________________________________________
    print("Import TMDB Data...")
    print(f"Getting data from {TMDB_MOVIE_DATA_PATH}")
    tmdb_movie_df = spark.read.parquet(TMDB_MOVIE_DATA_PATH)
    
    print(f"Getting data from {TMDB_SERIES_DATA_PATH}")
    tmdb_series_df = spark.read.parquet(TMDB_SERIES_DATA_PATH)
    
    tmdb_series_df.printSchema()
    
    print("Unifying Movie and Series TMDB Data...")
    tmdb_df = (
        tmdb_movie_df
            .withColumn("credits", spk_func.explode("credits.cast"))
            .select("*", 
                spk_func.col("credits.gender").alias(COL_P_GENDER),
                spk_func.col("credits.id").alias(COL_P_TMDB_ID),
                spk_func.col("credits.name").alias(COL_P_NAME),
                spk_func.lit("actor").alias(COL_OCCUPATION),
            ).unionByName(tmdb_movie_df
                .withColumn("credits", spk_func.explode("credits.crew"))
                .select("*",
                    spk_func.col("credits.gender").alias(COL_P_GENDER),
                    spk_func.col("credits.id").alias(COL_P_TMDB_ID),
                    spk_func.col("credits.name").alias(COL_P_NAME),
                    spk_func.col("credits.job").alias(COL_OCCUPATION),
                )
            ).unionByName(
                tmdb_series_df.withColumn("credits", spk_func.explode("credits.cast"))
                    .select("*", 
                    spk_func.col("credits.gender").alias(COL_P_GENDER),
                    spk_func.col("credits.id").alias(COL_P_TMDB_ID),
                    spk_func.col("credits.name").alias(COL_P_NAME),
                    spk_func.lit("actor").alias(COL_OCCUPATION),
                ).unionByName(tmdb_series_df
                    .withColumn("credits", spk_func.explode("credits.crew"))
                    .select("*",
                        spk_func.col("credits.gender").alias(COL_P_GENDER),
                        spk_func.col("credits.id").alias(COL_P_TMDB_ID),
                        spk_func.col("credits.name").alias(COL_P_NAME),
                        spk_func.col("credits.job").alias(COL_OCCUPATION),
                    )
                )

        )
        # ).select(
        #     spk_func.col(COL_BUDGET),
        #     spk_func.col("genres").alias(COL_GENRE),
        #     spk_func.col("id").alias(COL_TMDB_ID),
        #     spk_func.col(COL_IMDB_ID),
        #     spk_func.col(COL_OG_COUNTRY),
        #     spk_func.col(COL_POPULARITY),
        #     spk_func.col(COL_RELEASE_DATE),
        #     spk_func.col(COL_REVENUE),
        #     spk_func.col("runtime").alias(COL_MINUTE_DURATION),
        #     spk_func.col(COL_TITLE),
        #     spk_func.col(COL_VOTE_AVERAGE),
        #     spk_func.col(COL_VOTE_COUNT),
        #     spk_func.col("credits"),
        #     spk_func.col(COL_INGESTION_DATE),
        #     spk_func.lit(MEDIA_TYPE_MOVIE).alias(COL_TYPE)
        # ).unionByName(tmdb_series_df.select(
        #     spk_func.col("genres").alias(COL_GENRE),
        #     spk_func.col("id").alias(COL_TMDB_ID),
        #     spk_func.col(COL_OG_COUNTRY),
        #     spk_func.col(COL_POPULARITY),
        #     spk_func.col(COL_RELEASE_DATE),
        #     spk_func.col(COL_END_DATE),
        #     spk_func.col(COL_TITLE),
        #     spk_func.col(COL_VOTE_AVERAGE),
        #     spk_func.col(COL_VOTE_COUNT),
        #     spk_func.col("credits"),
        #     spk_func.col(COL_INGESTION_DATE),
        #     spk_func.lit(MEDIA_TYPE_SERIE).alias(COL_TYPE)
        # ), allowMissingColumns=True)
    )
    
    tmdb_df.show()
    
    # Cleaning Data ____________________________________________________________
    print("Cleaning Unuseful Columns...")
    dropped_local_movie_df = local_movie_df.drop(*COLUMNS_TO_REMOVE["LOCAL"])
    dropped_local_series_df = local_series_df.drop(*COLUMNS_TO_REMOVE["LOCAL"])
    dropped_tmdb_movie_df = tmdb_movie_df.drop(*COLUMNS_TO_REMOVE["TMDB"]["MOVIE"])
    dropped_tmdb_series_df = tmdb_series_df.drop(*COLUMNS_TO_REMOVE["TMDB"]["SERIES"])
    
    # Creating dim_date _______________________________________________________
    print("Extracting Data for dim_date...")
    
    # For Local Movies
    local_movie_dim_date = to_dim_date(dropped_local_movie_df, COL_INGESTION_DATE)
    local_movie_dim_date = local_movie_dim_date.union(to_dim_date(dropped_local_movie_df, "release_year", True))
    
    # For Local Series
    local_series_dim_date = to_dim_date(dropped_local_series_df, COL_INGESTION_DATE)
    local_series_dim_date = local_series_dim_date.union(to_dim_date(dropped_local_series_df, "release_year", True))
    local_series_dim_date = local_series_dim_date.union(to_dim_date(dropped_local_series_df, "end_year", True))
    
    # For TMDB Movies
    tmdb_movie_dim_date = to_dim_date(dropped_tmdb_movie_df, COL_INGESTION_DATE)
    tmdb_movie_dim_date = tmdb_movie_dim_date.union(to_dim_date(dropped_tmdb_movie_df, COL_RELEASE_DATE))
    
    # For TMDB Series
    tmdb_series_dim_date = to_dim_date(dropped_tmdb_series_df, COL_INGESTION_DATE)
    tmdb_series_dim_date = tmdb_series_dim_date.union(to_dim_date(dropped_tmdb_series_df, COL_RELEASE_DATE))
    tmdb_series_dim_date = tmdb_series_dim_date.union(to_dim_date(dropped_tmdb_series_df, COL_END_DATE))
        

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
                spk_func.col("complete_date"),
                spk_func.col("decade"),
                spk_func.col("year"),
                spk_func.col("month"),
                spk_func.col("quarter"),
                spk_func.col("week"),
                spk_func.col("day")
            )
    )
    
    print("Date Dimension Complete!")

    # Creating dim_people _____________________________________________________
    print("Extracting Data for dim_people...")
    
    # Local Data
    local_movie_dim_people = local_extract_people(dropped_local_movie_df)
    local_series_dim_people = local_extract_people(dropped_local_series_df) 
    
    # TMDB Data
    tmdb_movie_dim_people = tmdb_extract_people(dropped_tmdb_movie_df)
    tmdb_series_dim_people = tmdb_extract_people(dropped_tmdb_series_df)

    print("Adding data to dim_people...")
    
    # Union all dim_people
    dim_people = dim_people.union(local_movie_dim_people
        .union(local_series_dim_people)
        .union(tmdb_movie_dim_people)
        .union(tmdb_series_dim_people)
        .groupBy("name").agg(
            spk_func.first(COL_TMDB_ID).alias(COL_TMDB_ID),
            spk_func.first("gender").alias("gender"),
            spk_func.first(COL_BIRTH_YEAR).alias(COL_BIRTH_YEAR),
            spk_func.first(COL_DEATH_YEAR).alias(COL_DEATH_YEAR),
            spk_func.flatten(spk_func.collect_set(COL_OCCUPATION)).alias(COL_OCCUPATION),
            spk_func.first(COL_POPULARITY).alias(COL_POPULARITY)
        ).drop_duplicates()
        .orderBy(
            spk_func.when(spk_func.col(COL_TMDB_ID).isNull(), 1).otherwise(0),
            spk_func.col(COL_TMDB_ID),
            spk_func.col("name")
        )
        .select(
            spk_func.monotonically_increasing_id().alias("id"),
            spk_func.col(COL_TMDB_ID),
            spk_func.col("name"),
            spk_func.col("gender"),
            spk_func.col(COL_BIRTH_YEAR),
            spk_func.col(COL_DEATH_YEAR),
            spk_func.col(COL_OCCUPATION),
            spk_func.col(COL_POPULARITY)
        )
    )
    
    print("People Dimension Complete!")
    
    # Creating dim_media ______________________________________________________
    print("Extracting Data for dim_media...")
    
    # Local Data
    local_movie_dim_media = local_extract_media(dropped_local_movie_df, MEDIA_TYPE_MOVIE)
    local_series_dim_media = local_extract_media(dropped_local_series_df, MEDIA_TYPE_SERIE)
    
    # TMDB Data
    tmdb_movie_dim_media = tmdb_extract_media(dropped_tmdb_movie_df, MEDIA_TYPE_MOVIE)
    tmdb_series_dim_media = tmdb_extract_media(dropped_tmdb_series_df, MEDIA_TYPE_SERIE, False)
    
    # Union all dim_media
    print("Adding data to dim_media...")
    
    dim_media = dim_media.union(
        local_movie_dim_media
        .union(local_series_dim_media)
        .union(tmdb_movie_dim_media)
        .union(tmdb_series_dim_media)
        .drop_duplicates()
        .orderBy(
            spk_func.when(spk_func.col(COL_TITLE).isNull(), 1).otherwise(0),
            spk_func.col(COL_TITLE)
        )
        .select(
            spk_func.monotonically_increasing_id().alias("id"),
            spk_func.col(COL_TMDB_ID),
            spk_func.col(COL_IMDB_ID),
            spk_func.col(COL_TYPE),
            spk_func.col(COL_TITLE),
            spk_func.col(COL_OG_COUNTRY),
            spk_func.col("genres")
        )
    )
    
    print("Media Dimension Complete!")
    
    # Creating fact_media_evaluation __________________________________________
    print("Extracting Data for fact_media_evaluation...")
    
    # Local Data
    local_movie_fact_evaluation = local_extract_evaluation(
        spark_df=   dropped_local_movie_df.withColumn("end_year", spk_func.lit(None)),
        dim_media=  dim_media, 
        dim_people= dim_people,
        dim_date=   dim_date
    )
    local_series_fact_evaluation = local_extract_evaluation(
        spark_df=   dropped_local_series_df, 
        dim_media=  dim_media, 
        dim_people= dim_people, 
        dim_date=   dim_date
    )
    
    # TMDB Data
    tmdb_movie_fact_evaluation = tmdb_extract_evaluation(
        spark_df=   dropped_tmdb_movie_df.withColumn(COL_END_DATE, spk_func.lit(None)),
        dim_media=  dim_media, 
        dim_people= dim_people, 
        dim_date=   dim_date,
        media_type= MEDIA_TYPE_MOVIE
    )
    
    tmdb_series_fact_evaluation = tmdb_extract_evaluation(
        spark_df= (dropped_tmdb_series_df
            .withColumn("runtime", spk_func.lit(None))
            .withColumn(COL_BUDGET, spk_func.lit(None))
            .withColumn(COL_REVENUE, spk_func.lit(None))
        ),
        dim_media=  dim_media, 
        dim_people= dim_people, 
        dim_date=   dim_date,
        media_type= MEDIA_TYPE_SERIE
    )
    
    # Union all fact_media_evaluation
    print("Adding data to fact_media_evaluation...")
    
    fact_media_evaluation = fact_media_evaluation.union(
        local_movie_fact_evaluation
        .union(local_series_fact_evaluation)
        .union(tmdb_movie_fact_evaluation)
        .union(tmdb_series_fact_evaluation)
        .orderBy(
            spk_func.when(spk_func.col("media_id").isNull(), 1).otherwise(0),
            spk_func.col("media_id"),
            spk_func.when(spk_func.col("people_id").isNull(), 1).otherwise(0),
            spk_func.col("people_id")
        ).select(
            spk_func.monotonically_increasing_id().alias("id"),
            spk_func.col("media_id"),
            spk_func.col("people_id"),
            spk_func.col(COL_INGESTION_DATE),
            spk_func.col(COL_RELEASE_DATE),
            spk_func.col(COL_END_DATE),
            spk_func.col(COL_MINUTE_DURATION),
            spk_func.col(COL_VOTE_AVERAGE),
            spk_func.col(COL_VOTE_COUNT),
            spk_func.col(COL_BUDGET),
            spk_func.col(COL_REVENUE),
            spk_func.col(COL_POPULARITY)
        )
    )
        
    print("Media Evaluation Fact Complete!")
    
    # Writing Data ____________________________________________________________
    print(f"Writing Data on {S3_TARGET_PATH}...")
    
    fact_media_evaluation.write.mode("overwrite").parquet(FACT_MEDIA_EVALUATION_PATH)
    dim_media.write.mode("overwrite").parquet(DIM_MEDIA_PATH)
    dim_people.write.mode("overwrite").parquet(DIM_PEOPLE_PATH)
    dim_date.write.mode("overwrite").parquet(DIM_DATE_PATH)
    
    print("Data Write Complete!")
    # Custom Code End =========================================================
    job.commit()

if __name__ == "__main__": 
    main()