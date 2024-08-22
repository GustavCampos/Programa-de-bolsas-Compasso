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
from pyspark.sql import DataFrame, functions as spk_func, Window
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
def create_dimension(spark_df: DataFrame, order_by: str | tuple[str], *select: str) -> DataFrame:    
    if len(order_by) == 0:
        raise ValueError("'order_by' needs to have at least one 'str' inside or be a valid string.")
    
    is_tuple = isinstance(order_by, tuple)
    norm_order_by = order_by if is_tuple else tuple([order_by])
        
    return (
        spark_df
            .select(*norm_order_by, *select)
            .where(spk_func.coalesce(*norm_order_by).isNotNull())
            .distinct()
            .withColumn("id", spk_func
                .row_number()
                .over(Window.orderBy(*norm_order_by))
            )
            .orderBy(*norm_order_by)
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
    COL_ID              = "id"
    COL_RELEASE_DATE    = "release_date"
    COL_END_DATE        = "end_date"
    COL_INGESTION_DATE  = "ingestion_date"
    COL_COMPLETE_DATE   = "complete_date"
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
    COL_CHARACTER       = "character"
    
    
    # Media Type
    MEDIA_TYPE_MOVIE = "movie"
    MEDIA_TYPE_SERIE = "series"
    
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
            spk_func.lit(MEDIA_TYPE_MOVIE).alias(COL_TYPE),
            spk_func.col(COL_CHARACTER)
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
            spk_func.col("end_year").alias(COL_END_DATE),
            spk_func.col(COL_INGESTION_DATE),
            spk_func.lit(MEDIA_TYPE_SERIE).alias(COL_TYPE),
            spk_func.col(COL_CHARACTER)
        ), allowMissingColumns=True)
    )
    
    print("Local data unified!")
    local_df.printSchema()
    
    print("Exploding Local Data...")
    exploded_local_df = (
        local_df
            # Filter non animations and adult animations
            .filter(spk_func.array_contains(spk_func.col(COL_GENRE), "Animation"))
            .filter(~spk_func.array_contains(spk_func.col(COL_GENRE), "Adult"))
            # Adjust columns
            .withColumnRenamed("release_year", COL_RELEASE_DATE)
            .withColumnRenamed("end_year", COL_END_DATE)
            .withColumn(COL_RELEASE_DATE, spk_func.col(COL_RELEASE_DATE).cast(StringType()))
            .withColumn(COL_END_DATE, spk_func.col(COL_END_DATE).cast(StringType()))
            .withColumn(COL_VOTE_AVERAGE, 
                spk_func.round(spk_func.col(COL_VOTE_AVERAGE).cast(DoubleType()), 2)
            )
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
    )
    
    print(f"Record count before explode: {local_df.count()}")
    print(f"Record count after explode: {exploded_local_df.count()}")
    exploded_local_df.show(5)

    # Processing TMDB Data ____________________________________________________
    print("Import TMDB Data...")
    print(f"Getting data from {TMDB_MOVIE_DATA_PATH}")
    tmdb_movie_df = spark.read.parquet(TMDB_MOVIE_DATA_PATH)
    
    print(f"Getting data from {TMDB_SERIES_DATA_PATH}")
    tmdb_series_df = spark.read.parquet(TMDB_SERIES_DATA_PATH)
    
    print("Unifying Movie and Series TMDB Data...")
    c_gender = "credits.gender"
    c_id = "credits.id"
    c_name = "credits.name"
    explode_local_movie_credits_df = (tmdb_movie_df
        .withColumn("credits", spk_func.explode("credits.cast"))
        .select("*", 
            spk_func.col(c_gender).alias(COL_P_GENDER),
            spk_func.col(c_id).alias(COL_P_TMDB_ID),
            spk_func.col(c_name).alias(COL_P_NAME),
            spk_func.lit("actor").alias(COL_OCCUPATION),
            spk_func.col(f"credits.{COL_CHARACTER}").alias(COL_CHARACTER),
        )
        .drop("credits")
        .unionByName(tmdb_movie_df
            .withColumn("credits", spk_func.explode("credits.crew"))
            .select("*",
                spk_func.col(c_gender).alias(COL_P_GENDER),
                spk_func.col(c_id).alias(COL_P_TMDB_ID),
                spk_func.col(c_name).alias(COL_P_NAME),
                spk_func.col("credits.job").alias(COL_OCCUPATION),
                spk_func.col("credits.credit_id").cast(StringType()).alias(COL_CHARACTER),
            )
            .drop("credits")
        )
        .withColumn(COL_TYPE, spk_func.lit(MEDIA_TYPE_MOVIE))
    )
    
    explode_local_series_credits_df = (tmdb_series_df
        .withColumn("credits", spk_func.explode("credits.cast"))
        .select("*", 
            spk_func.col(c_gender).alias(COL_P_GENDER),
            spk_func.col(c_id).alias(COL_P_TMDB_ID),
            spk_func.col(c_name).alias(COL_P_NAME),
            spk_func.lit("actor").alias(COL_OCCUPATION),
            spk_func.col(f"credits.{COL_CHARACTER}").alias(COL_CHARACTER),
        )
        .drop("credits")
        .unionByName(tmdb_series_df
            .withColumn("credits", spk_func.explode("credits.crew"))
            .select("*",
                spk_func.col(c_gender).alias(COL_P_GENDER),
                spk_func.col(c_id).alias(COL_P_TMDB_ID),
                spk_func.col(c_name).alias(COL_P_NAME),
                spk_func.col("credits.job").alias(COL_OCCUPATION),
                spk_func.col("credits.credit_id").cast(StringType()).alias(COL_CHARACTER),
            )
            .drop("credits")
        )
        .withColumn(COL_TYPE, spk_func.lit(MEDIA_TYPE_SERIE))
    )
    
    tmdb_df = (
        explode_local_movie_credits_df
        .unionByName(
            explode_local_series_credits_df, 
            allowMissingColumns=True
        )
        .select(
            spk_func.col(COL_BUDGET),
            spk_func.col("genres").alias(COL_GENRE),
            spk_func.col("id").alias(COL_TMDB_ID),
            spk_func.col(COL_IMDB_ID),
            spk_func.col(COL_OG_COUNTRY),
            spk_func.col(COL_POPULARITY),
            spk_func.col(COL_RELEASE_DATE),
            spk_func.col(COL_REVENUE),
            spk_func.col("runtime").alias(COL_MINUTE_DURATION),
            spk_func.col(COL_TITLE),
            spk_func.col(COL_VOTE_AVERAGE),
            spk_func.col(COL_VOTE_COUNT),
            spk_func.col(COL_INGESTION_DATE),
            spk_func.col(COL_P_GENDER),
            spk_func.col(COL_P_TMDB_ID),
            spk_func.col(COL_P_NAME),
            spk_func.col(COL_OCCUPATION),
            spk_func.col(COL_END_DATE),
            spk_func.col(COL_TYPE),
            spk_func.col(COL_CHARACTER)
        )
    )
    
    print("TMDB data unified!")
    tmdb_df.printSchema()
    
    print("Exploding Local Data...")
    exploded_tmdb_df = (
        tmdb_df
        .withColumn(COL_TITLE, spk_func.lower(COL_TITLE))
        .withColumn(COL_P_NAME, spk_func.lower(COL_P_NAME))
        .withColumn(COL_OCCUPATION, spk_func.lower(COL_OCCUPATION))
        .withColumn(COL_P_GENDER, spk_func
            .when(spk_func.col(COL_P_GENDER) == 1, "female")
            .when(spk_func.col(COL_P_GENDER) == 2, "male")
            .when(spk_func.col(COL_P_GENDER) == 3, "non-binary")
            .otherwise(None)
        )
        .withColumn(COL_GENRE, spk_func.explode_outer(COL_GENRE))
        .withColumn(COL_GENRE, spk_func.lower(COL_GENRE))
        .withColumn(COL_OG_COUNTRY, spk_func.explode(COL_OG_COUNTRY))
        .withColumn(COL_OG_COUNTRY, spk_func.upper(COL_OG_COUNTRY))
    )
    
    print(f"Record count before explode: {tmdb_df.count()}")
    print(f"Record count after explode: {exploded_tmdb_df.count()}")
    exploded_tmdb_df.show(5)
    
    # Evaluating data union beetwen local and TMDB ____________________________
    print("Evaluating Local/TMDB data to unify DataFrames...")
    complete_df = (
        exploded_local_df.alias("i").join(
            how="full_outer",
            other=exploded_tmdb_df.alias("t"),
            on=(
                (spk_func.col(f"i.{COL_TITLE}") == spk_func.col(f"t.{COL_TITLE}")) & 
                (spk_func.col(f"i.{COL_GENRE}") == spk_func.col(f"t.{COL_GENRE}")) & 
                (spk_func.col(f"i.{COL_P_NAME}") == spk_func.col(f"t.{COL_P_NAME}")) &
                (spk_func.col(f"i.{COL_CHARACTER}") == spk_func.col(f"t.{COL_CHARACTER}")) &
                (spk_func.col(f"i.{COL_OCCUPATION}") == spk_func.col(f"t.{COL_OCCUPATION}"))
            )
        ).select(
            spk_func.coalesce(spk_func.col(f"t.{COL_RELEASE_DATE}"), spk_func.col(f"i.{COL_RELEASE_DATE}")).alias(COL_RELEASE_DATE),
            spk_func.coalesce(spk_func.col(f"t.{COL_END_DATE}"), spk_func.col(f"i.{COL_END_DATE}")).alias(COL_END_DATE),
            spk_func.coalesce(spk_func.col(f"t.{COL_INGESTION_DATE}"), spk_func.col(f"i.{COL_INGESTION_DATE}")).alias(COL_INGESTION_DATE),
            spk_func.col(f"t.{COL_TMDB_ID}"),
            spk_func.coalesce(spk_func.col(f"t.{COL_IMDB_ID}"), spk_func.col(f"i.{COL_IMDB_ID}")).alias(COL_IMDB_ID),
            spk_func.col(f"t.{COL_TYPE}"),
            spk_func.col(f"t.{COL_TITLE}"),
            spk_func.col(f"t.{COL_GENRE}"),
            spk_func.col(f"t.{COL_OG_COUNTRY}"),
            spk_func.col(f"t.{COL_P_TMDB_ID}"),
            spk_func.coalesce(spk_func.col(f"t.{COL_P_NAME}"), spk_func.col(f"i.{COL_P_NAME}")).alias(COL_P_NAME),
            spk_func.coalesce(spk_func.col(f"t.{COL_P_GENDER}"), spk_func.col(f"i.{COL_P_GENDER}")).alias(COL_P_GENDER),
            spk_func.col(f"i.{COL_BIRTH_YEAR}"),
            spk_func.col(f"i.{COL_DEATH_YEAR}"),
            spk_func.col(f"t.{COL_OCCUPATION}"),
            spk_func.coalesce(spk_func.col(f"t.{COL_MINUTE_DURATION}"), spk_func.col(f"i.{COL_MINUTE_DURATION}")).alias(COL_MINUTE_DURATION),
            spk_func.coalesce(spk_func.col(f"t.{COL_VOTE_AVERAGE}"), spk_func.col(f"i.{COL_VOTE_AVERAGE}")).alias(COL_VOTE_AVERAGE),
            spk_func.coalesce(spk_func.col(f"t.{COL_VOTE_COUNT}"), spk_func.col(f"i.{COL_VOTE_COUNT}")).alias(COL_VOTE_COUNT),
            spk_func.col(f"t.{COL_BUDGET}"),
            spk_func.col(f"t.{COL_REVENUE}"),
            spk_func.col(f"t.{COL_POPULARITY}"),
        )
    )
    
    a = exploded_local_df.count()
    b = exploded_tmdb_df.count()
    c = complete_df.count() 
    print(f"Local Records: {a}\nTMDB Records: {b}\nUnited Records: {c}\nReduced Records: {abs((a + b) - c)}")
     
    complete_df.show(5)
    
    # Creating dimensions _____________________________________________________
    print("Creating Dimensions...")
    
    # Dim occupation
    print("Creating Occupation Dimension...")
    occupation_df = create_dimension(complete_df, COL_OCCUPATION)
    occupation_df.printSchema()
    
    # Creating country dimension
    print("Creating Country Dimension")
    country_df = create_dimension(complete_df, COL_OG_COUNTRY)    
    country_df.printSchema()
    
    # Creating genre dimension
    print("Creating Genre Dimension")
    genre_df = create_dimension(complete_df, COL_GENRE)
    genre_df.printSchema()
    
    # Creating media dimension
    print("Creating Media Dimension")
    media_df_order_by = (COL_TITLE, COL_TMDB_ID, COL_IMDB_ID)
    media_df = create_dimension(complete_df, media_df_order_by, COL_TYPE)
    media_df.printSchema()
    
    # Creating date dimension
    print("Creating Date Dimension")
    date_union_df = (complete_df
        .select(spk_func.col(COL_RELEASE_DATE).alias(COL_COMPLETE_DATE))
        .union(complete_df.select(spk_func.col(COL_END_DATE).alias(COL_COMPLETE_DATE)))
        .union(complete_df.select(spk_func.col(COL_INGESTION_DATE).alias(COL_COMPLETE_DATE)))
    )
    
    date_df = (
        create_dimension(date_union_df, (COL_COMPLETE_DATE))
        .withColumn("year", spk_func.year(COL_COMPLETE_DATE))
        .withColumn("month", spk_func.month(COL_COMPLETE_DATE))
        .withColumn("day", spk_func.dayofmonth(COL_COMPLETE_DATE))
        .withColumn("quarter", spk_func.quarter(COL_COMPLETE_DATE))
        .withColumn("week", spk_func.weekofyear(COL_COMPLETE_DATE))
        .withColumn(
            "decade", 
            (spk_func.col("year") / 10).cast(IntegerType()) * 10
        )
    )
    date_df.printSchema()
        
    # Creating people dimension
    print("Creating People Dimension")
    people_df_order_by = (COL_P_TMDB_ID, COL_P_NAME)
    people_df = create_dimension(
        complete_df, 
        people_df_order_by,
        COL_P_GENDER, COL_BIRTH_YEAR, COL_DEATH_YEAR
    )
    people_df.printSchema()
    
    # Creating fact and checking data _________________________________________
    print("Checking data correctness...")
    
    join_complete_df = (
        complete_df.alias("c")
        # Occupation join
        .orderBy(COL_OCCUPATION)
        .join(
            how="left",
            other=occupation_df.alias("o"), 
            on=spk_func.col(f"c.{COL_OCCUPATION}") == spk_func.col(f"o.{COL_OCCUPATION}"), 
        )
        # Country join
        .orderBy(COL_OG_COUNTRY)
        .join(
            how="left",
            other=country_df.alias("co"), 
            on=spk_func.col(f"c.{COL_OG_COUNTRY}") == spk_func.col(f"co.{COL_OG_COUNTRY}"),
        )
        # Genre join
        .orderBy(COL_GENRE)
        .join(
            how="left",
            other=genre_df.alias("g"), 
            on=spk_func.col(f"c.{COL_GENRE}") == spk_func.col(f"g.{COL_GENRE}"),
        )
        # Media join
        .orderBy(*media_df_order_by)
        .join(
            how="left",
            other=media_df.alias("m"),
            on=(
                (spk_func.col(f"c.{COL_TITLE}") == spk_func.col(f"m.{COL_TITLE}")) &
                (spk_func.col(f"c.{COL_TMDB_ID}") == spk_func.col(f"m.{COL_TMDB_ID}")) &
                (spk_func.col(f"c.{COL_IMDB_ID}") == spk_func.col(f"m.{COL_IMDB_ID}")) 
            ),
        )
        # Date join
        .orderBy(COL_RELEASE_DATE)
        .join(
            how="left",
            other=date_df.alias("rld"),
            on=spk_func.col(f"c.{COL_RELEASE_DATE}") == spk_func.col(f"rld.{COL_COMPLETE_DATE}"),
        )
        .orderBy(COL_END_DATE)
        .join(
            how="left",
            other=date_df.alias("edd"),
            on=spk_func.col(f"c.{COL_RELEASE_DATE}") == spk_func.col(f"edd.{COL_COMPLETE_DATE}"),
        )
        .orderBy(COL_INGESTION_DATE)
        .join(
            how="left",
            other=date_df.alias("igd"),
            on=spk_func.col(f"c.{COL_RELEASE_DATE}") == spk_func.col(f"igd.{COL_COMPLETE_DATE}"),
        )
        # People join
        .orderBy(*people_df_order_by)
        .join(
            how="left",
            other=people_df.alias("p"),
            on=(
                (spk_func.col(f"c.{COL_P_TMDB_ID}") == spk_func.col(f"p.{COL_P_TMDB_ID}")) &
                (spk_func.col(f"c.{COL_P_NAME}") == spk_func.col(f"p.{COL_P_NAME}"))
            ),
        )
    )
    
    complete_df.select(COL_TYPE).distinct().show()
    join_complete_df.show()
    
    # Writing Data ____________________________________________________________
    # print(f"Writing Data on {S3_TARGET_PATH}...")
    
    # fact_media_evaluation.write.mode("overwrite").parquet(FACT_MEDIA_EVALUATION_PATH)
    # dim_media.write.mode("overwrite").parquet(DIM_MEDIA_PATH)
    # dim_people.write.mode("overwrite").parquet(DIM_PEOPLE_PATH)
    # dim_date.write.mode("overwrite").parquet(DIM_DATE_PATH)
    
    # print("Data Write Complete!")
    # Custom Code End =========================================================
    job.commit()

if __name__ == "__main__": 
    main()