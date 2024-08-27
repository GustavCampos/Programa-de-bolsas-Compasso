# Python Standart Libs
import os
import sys
import json
import timeit

# AWS Glue Libs
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, functions as spkf, Window
from pyspark.sql.types import StringType, DoubleType, IntegerType

# String functions ____________________________________________________________
def compare_by_hash(cols: tuple, alias: tuple = ("a", "b")) -> bool:
    return (
        spkf.concat(spkf.lit("#"), *[f"{alias[0]}.{col}" for col in cols]) 
        == 
        spkf.concat(spkf.lit("#"), *[f"{alias[1]}.{col}" for col in cols])
    )

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
            .where(spkf.coalesce(*norm_order_by).isNotNull())
            .distinct()
            .withColumn("id", spkf
                .row_number()
                .over(Window.orderBy(*norm_order_by))
            )
            .orderBy(*norm_order_by)
    )
    
def get_condition(col_a: str, col_b: str = None,
                  aliases: tuple = ("a", "b"), nullable: bool=True) -> str:
    col_bb = col_a if col_b is None else col_b
    a_alias, b_alias = aliases
    
    cond_1 = (spkf.col(f"{a_alias}.{col_a}") == spkf.col(f"{b_alias}.{col_bb}"))
    
    if not nullable: return cond_1
    
    return ((cond_1) |
        (spkf.col(f"{a_alias}.{col_a}").isNull() & spkf.col(f"{b_alias}.{col_bb}").isNull())
    )
    
def main():
    start = timeit.default_timer()
    
    # Loading Job Parameters __________________________________________________
    print("Loading Job Parameters...")
    
    ARGS_LIST = [
        "JOB_NAME", 
        "LOCAL_MOVIE_DATA_PATH", "LOCAL_SERIES_DATA_PATH",
        "TMDB_MOVIE_DATA_PATH", "TMDB_SERIES_DATA_PATH",
        "S3_TARGET_PATH"
    ]
    
    ## @params: [JOB_NAME,LOCAL_MOVIE_DATA_PATH, LOCAL_SERIES_DATA_PATH, TMDB_MOVIE_DATA_PATH, TMDB_SERIES_DATA_PATH, S3_TARGET_PATH]
    args = load_args(arg_list=ARGS_LIST, file_path='main_params.json')
    
    # Creating Job Context ____________________________________________________
    print("Creating Job Context...")
    
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
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
    DIM_GENRE_PATH = f"{RESULT_FOLDER_PATH}dim_genre/"
    DIM_COUNTRY_PATH = f"{RESULT_FOLDER_PATH}dim_country/"
    DIM_OCCUPATION_PATH = f"{RESULT_FOLDER_PATH}dim_occupation/"
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
    print("Importing and unifying Movie and Series Local Data...")
    local_df = (
        spark.read.parquet(LOCAL_MOVIE_DATA_PATH).select(
            spkf.col("id").alias(COL_IMDB_ID),
            spkf.col(COL_TITLE),
            spkf.col("release_year"),
            spkf.col(COL_MINUTE_DURATION),
            spkf.col(COL_GENRE),
            spkf.col(COL_VOTE_AVERAGE),
            spkf.col(COL_VOTE_COUNT),
            spkf.col("artist_name").alias(COL_P_NAME),
            spkf.col("artist_genre").alias(COL_P_GENDER),
            spkf.col(COL_DEATH_YEAR),
            spkf.col(COL_BIRTH_YEAR),
            spkf.col(COL_OCCUPATION),
            spkf.col(COL_INGESTION_DATE),
            spkf.lit(MEDIA_TYPE_MOVIE).alias(COL_TYPE),
            spkf.col(COL_CHARACTER)
        ).unionByName(spark.read.parquet(LOCAL_SERIES_DATA_PATH).select(
            spkf.col("id").alias(COL_IMDB_ID),
            spkf.col(COL_TITLE),
            spkf.col("release_year"),
            spkf.col(COL_MINUTE_DURATION),
            spkf.col(COL_GENRE),
            spkf.col(COL_VOTE_AVERAGE),
            spkf.col(COL_VOTE_COUNT),
            spkf.col("artist_name").alias(COL_P_NAME),
            spkf.col("artist_genre").alias(COL_P_GENDER),
            spkf.col(COL_DEATH_YEAR),
            spkf.col(COL_BIRTH_YEAR),
            spkf.col(COL_OCCUPATION),
            spkf.col("end_year").alias(COL_END_DATE),
            spkf.col(COL_INGESTION_DATE),
            spkf.lit(MEDIA_TYPE_SERIE).alias(COL_TYPE),
            spkf.col(COL_CHARACTER)
        ), allowMissingColumns=True)
    )
    local_df_before_explode_count = local_df.count()
    
    print("Local data unified!")
    local_df.printSchema()
        
    print("Exploding Local Data...")
    local_df = (
        local_df
            # Filter defective rows
            .filter(spkf.col(COL_IMDB_ID).isNotNull())
            .filter(spkf.col(COL_TITLE).isNotNull())
            .filter(spkf.col(COL_GENRE).isNotNull())
            .filter(spkf.col(COL_P_NAME).isNotNull())
            .drop_duplicates()
            # Filter non animations and adult animations
            .filter(spkf.array_contains(spkf.col(COL_GENRE), "Animation"))
            .filter(~spkf.array_contains(spkf.col(COL_GENRE), "Adult"))
            # Adjust columns
            .withColumnRenamed("release_year", COL_RELEASE_DATE)
            .withColumnRenamed("end_year", COL_END_DATE)
            .withColumn(COL_RELEASE_DATE, spkf.col(COL_RELEASE_DATE).cast(StringType()))
            .withColumn(COL_END_DATE, spkf.col(COL_END_DATE).cast(StringType()))
            .withColumn(COL_VOTE_AVERAGE, 
                spkf.round(spkf.col(COL_VOTE_AVERAGE).cast(DoubleType()), 2)
            )
            .withColumn(COL_TITLE, spkf.lower(COL_TITLE))
            .withColumn(COL_P_NAME, spkf.lower(COL_P_NAME))
            .withColumn(COL_P_GENDER, spkf
                .when(spkf.col(COL_P_GENDER) == "M", "male")
                .when(spkf.col(COL_P_GENDER) == "F", "female")
                .otherwise(None)
            )
            .withColumn(COL_GENRE, spkf.explode(COL_GENRE))
            .withColumn(COL_GENRE, spkf.lower(COL_GENRE))
            .withColumn(COL_OCCUPATION, spkf.explode_outer(COL_OCCUPATION))
            .withColumn(COL_OCCUPATION, spkf
                .when(spkf.col(COL_OCCUPATION) == "", None)
                .otherwise(spkf.col(COL_OCCUPATION))
            ).withColumn(COL_OCCUPATION, 
                spkf.regexp_replace(spkf.col(COL_OCCUPATION), "_", " ")
            )
    )
    
    local_df_after_explode_count = local_df.count()
    print(f"Record count before explode: {local_df_before_explode_count}")
    print(f"Record count after explode: {local_df_after_explode_count}")
    local_df.show(5)

    # Processing TMDB Data ____________________________________________________
    print("Importing and unifying Movie and Series TMDB Data...")
    c_gender = "credits.gender"
    c_id = "credits.id"
    c_name = "credits.name"
    
    explode_local_movie_credits_df = (spark.read.parquet(TMDB_MOVIE_DATA_PATH)
        .withColumn("credits", spkf.explode("credits.cast"))
        .select("*", 
            spkf.col(c_gender).alias(COL_P_GENDER),
            spkf.col(c_id).alias(COL_P_TMDB_ID),
            spkf.col(c_name).alias(COL_P_NAME),
            spkf.lit("actor").alias(COL_OCCUPATION),
            spkf.col(f"credits.{COL_CHARACTER}").alias(COL_CHARACTER),
        )
        .drop("credits")
        .unionByName(spark.read.parquet(TMDB_MOVIE_DATA_PATH)
            .withColumn("credits", spkf.explode("credits.crew"))
            .select("*",
                spkf.col(c_gender).alias(COL_P_GENDER),
                spkf.col(c_id).alias(COL_P_TMDB_ID),
                spkf.col(c_name).alias(COL_P_NAME),
                spkf.col("credits.job").alias(COL_OCCUPATION),
                spkf.col("credits.credit_id").cast(StringType()).alias(COL_CHARACTER),
            )
            .drop("credits")
        )
        .withColumn(COL_TYPE, spkf.lit(MEDIA_TYPE_MOVIE))
    )
    
    explode_local_series_credits_df = (spark.read.parquet(TMDB_SERIES_DATA_PATH)
        .withColumn("credits", spkf.explode("credits.cast"))
        .select("*", 
            spkf.col(c_gender).alias(COL_P_GENDER),
            spkf.col(c_id).alias(COL_P_TMDB_ID),
            spkf.col(c_name).alias(COL_P_NAME),
            spkf.lit("actor").alias(COL_OCCUPATION),
            spkf.col(f"credits.{COL_CHARACTER}").alias(COL_CHARACTER),
        )
        .drop("credits")
        .unionByName(spark.read.parquet(TMDB_SERIES_DATA_PATH)
            .withColumn("credits", spkf.explode("credits.crew"))
            .select("*",
                spkf.col(c_gender).alias(COL_P_GENDER),
                spkf.col(c_id).alias(COL_P_TMDB_ID),
                spkf.col(c_name).alias(COL_P_NAME),
                spkf.col("credits.job").alias(COL_OCCUPATION),
                spkf.col("credits.credit_id").cast(StringType()).alias(COL_CHARACTER),
            )
            .drop("credits")
        )
        .withColumn(COL_TYPE, spkf.lit(MEDIA_TYPE_SERIE))
    )
    
    tmdb_df = (
        explode_local_movie_credits_df
        .unionByName(
            explode_local_series_credits_df, 
            allowMissingColumns=True
        )
        .select(
            spkf.col(COL_BUDGET),
            spkf.col("genres").alias(COL_GENRE),
            spkf.col("id").alias(COL_TMDB_ID),
            spkf.col(COL_IMDB_ID),
            spkf.col(COL_OG_COUNTRY),
            spkf.col(COL_POPULARITY),
            spkf.col(COL_RELEASE_DATE),
            spkf.col(COL_REVENUE),
            spkf.col("runtime").alias(COL_MINUTE_DURATION),
            spkf.col(COL_TITLE),
            spkf.col(COL_VOTE_AVERAGE),
            spkf.col(COL_VOTE_COUNT),
            spkf.col(COL_INGESTION_DATE),
            spkf.col(COL_P_GENDER),
            spkf.col(COL_P_TMDB_ID),
            spkf.col(COL_P_NAME),
            spkf.col(COL_OCCUPATION),
            spkf.col(COL_END_DATE),
            spkf.col(COL_TYPE),
            spkf.col(COL_CHARACTER)
        )
        .drop_duplicates()
    )
    tmdb_df_before_explode_count = tmdb_df.count()
    
    print("TMDB data unified!")
    tmdb_df.printSchema()
    
    print("Exploding Local Data...")
    tmdb_df = (
        tmdb_df
        .withColumn(COL_TITLE, spkf.lower(COL_TITLE))
        .withColumn(COL_P_NAME, spkf.lower(COL_P_NAME))
        .withColumn(COL_OCCUPATION, spkf.lower(COL_OCCUPATION))
        .withColumn(COL_P_GENDER, spkf
            .when(spkf.col(COL_P_GENDER) == 1, "female")
            .when(spkf.col(COL_P_GENDER) == 2, "male")
            .when(spkf.col(COL_P_GENDER) == 3, "non-binary")
            .otherwise(None)
        )
        .withColumn(COL_GENRE, spkf.explode_outer(COL_GENRE))
        .withColumn(COL_GENRE, spkf.lower(COL_GENRE))
        .withColumn(COL_OG_COUNTRY, spkf.explode(COL_OG_COUNTRY))
        .withColumn(COL_OG_COUNTRY, spkf.upper(COL_OG_COUNTRY))
    )
    
    tmdb_df_after_explode_count = tmdb_df.count()
    print(f"Record count before explode: {tmdb_df_before_explode_count}")
    print(f"Record count after explode: {tmdb_df_after_explode_count}")
    tmdb_df.show(5)
    
    # Evaluating data union beetwen local and TMDB ____________________________
    print("Evaluating Local/TMDB data to unify DataFrames...")
    complete_df = (
        local_df.alias("i").join(
            how="full_outer",
            other=tmdb_df.alias("t"),
            on=(
                (spkf.col(f"i.{COL_TITLE}") == spkf.col(f"t.{COL_TITLE}")) & 
                (spkf.col(f"i.{COL_GENRE}") == spkf.col(f"t.{COL_GENRE}")) & 
                (spkf.col(f"i.{COL_P_NAME}") == spkf.col(f"t.{COL_P_NAME}")) &
                (spkf.col(f"i.{COL_CHARACTER}") == spkf.col(f"t.{COL_CHARACTER}")) &
                (spkf.col(f"i.{COL_OCCUPATION}") == spkf.col(f"t.{COL_OCCUPATION}"))
            )
        ).select(
            spkf.coalesce(spkf.col(f"t.{COL_RELEASE_DATE}"), spkf.col(f"i.{COL_RELEASE_DATE}"))         .alias(COL_RELEASE_DATE),
            spkf.coalesce(spkf.col(f"t.{COL_END_DATE}"), spkf.col(f"i.{COL_END_DATE}"))                 .alias(COL_END_DATE),
            spkf.coalesce(spkf.col(f"t.{COL_INGESTION_DATE}"), spkf.col(f"i.{COL_INGESTION_DATE}"))     .alias(COL_INGESTION_DATE),
            spkf.col(f"t.{COL_TMDB_ID}")                                                                .alias(COL_TMDB_ID),
            spkf.coalesce(spkf.col(f"t.{COL_IMDB_ID}"), spkf.col(f"i.{COL_IMDB_ID}"))                   .alias(COL_IMDB_ID),
            spkf.coalesce(spkf.col(f"t.{COL_TYPE}"), spkf.col(f"i.{COL_TYPE}"))                         .alias(COL_TYPE),
            spkf.coalesce(spkf.col(f"t.{COL_TITLE}"), spkf.col(f"i.{COL_TITLE}"))                       .alias(COL_TITLE),
            spkf.coalesce(spkf.col(f"t.{COL_GENRE}"), spkf.col(f"i.{COL_GENRE}"))                       .alias(COL_GENRE),
            spkf.col(f"t.{COL_OG_COUNTRY}")                                                             .alias(COL_OG_COUNTRY),
            spkf.col(f"t.{COL_P_TMDB_ID}")                                                              .alias(COL_P_TMDB_ID),
            spkf.coalesce(spkf.col(f"t.{COL_P_NAME}"), spkf.col(f"i.{COL_P_NAME}"))                     .alias(COL_P_NAME),
            spkf.coalesce(spkf.col(f"t.{COL_P_GENDER}"), spkf.col(f"i.{COL_P_GENDER}"))                 .alias(COL_P_GENDER),
            spkf.col(f"i.{COL_BIRTH_YEAR}")                                                             .alias(COL_BIRTH_YEAR),
            spkf.col(f"i.{COL_DEATH_YEAR}")                                                             .alias(COL_DEATH_YEAR),
            spkf.coalesce(spkf.col(f"t.{COL_OCCUPATION}"), spkf.col(f"i.{COL_OCCUPATION}"))             .alias(COL_OCCUPATION),
            spkf.coalesce(spkf.col(f"t.{COL_MINUTE_DURATION}"), spkf.col(f"i.{COL_MINUTE_DURATION}"))   .alias(COL_MINUTE_DURATION),
            spkf.coalesce(spkf.col(f"t.{COL_VOTE_AVERAGE}"), spkf.col(f"i.{COL_VOTE_AVERAGE}"))         .alias(COL_VOTE_AVERAGE),
            spkf.coalesce(spkf.col(f"t.{COL_VOTE_COUNT}"), spkf.col(f"i.{COL_VOTE_COUNT}"))             .alias(COL_VOTE_COUNT),
            spkf.col(f"t.{COL_BUDGET}")                                                                 .alias(COL_BUDGET),
            spkf.col(f"t.{COL_REVENUE}")                                                                .alias(COL_REVENUE),
            spkf.col(f"t.{COL_POPULARITY}")                                                             .alias(COL_POPULARITY),
        )
        .withColumn(COL_ID, spkf.row_number().over(Window.orderBy(COL_RELEASE_DATE, COL_TITLE, COL_GENRE, COL_P_NAME)))
        .orderBy(COL_ID)
    )
    
    a = local_df.count()
    b = tmdb_df.count()
    c = complete_df.count()
    print(
        f"Local Records:    {a}\n"
        f"TMDB Records:     {b}\n"
        f"Reduced Records:  {abs((a + b) - c)}\n"
        f"Complete Records:   {c}"
    )
    
    # Avoid heap memory error
    del local_df, tmdb_df
     
    complete_df.printSchema()
    complete_df.show(5)
    
    # Creating dimensions _____________________________________________________
    print("Creating Dimensions...")    
    # Dim occupation
    print("Creating Occupation Dimension...")
    occupation_df = create_dimension(complete_df, COL_OCCUPATION)
    occupation_df.printSchema()
    
    # Creating country dimension
    print("Creating Country Dimension...")
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
        .select(spkf.col(COL_RELEASE_DATE).alias(COL_COMPLETE_DATE))
        .union(complete_df.select(spkf.col(COL_END_DATE).alias(COL_COMPLETE_DATE)))
        .union(complete_df.select(spkf.col(COL_INGESTION_DATE).alias(COL_COMPLETE_DATE)))
    )
    
    date_df = (
        create_dimension(date_union_df, (COL_COMPLETE_DATE))
        .withColumn("year", spkf.year(COL_COMPLETE_DATE))
        .withColumn("month", spkf.month(COL_COMPLETE_DATE))
        .withColumn("day", spkf.dayofmonth(COL_COMPLETE_DATE))
        .withColumn("quarter", spkf.quarter(COL_COMPLETE_DATE))
        .withColumn("week", spkf.weekofyear(COL_COMPLETE_DATE))
        .withColumn(
            "decade", 
            (spkf.col("year") / 10).cast(IntegerType()) * 10
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
    
    # Creating fact ___________________________________________________________
    print("Creating Fact...")
    
    complete_df = (
        complete_df.orderBy(COL_ID).alias("f")
        .join(occupation_df.alias("o"), COL_OCCUPATION, "left")
        .join(country_df.alias("c"), COL_OG_COUNTRY, "left")
        .join(genre_df.alias("g"), COL_GENRE, "left")
        .join(date_df.alias("r"), spkf.expr(f"f.{COL_RELEASE_DATE} = r.{COL_COMPLETE_DATE}"), "left")
        .join(date_df.alias("e"), spkf.expr(f"f.{COL_END_DATE} = e.{COL_COMPLETE_DATE}"), "left")
        .join(date_df.alias("i"), spkf.expr(f"f.{COL_INGESTION_DATE} = i.{COL_COMPLETE_DATE}"), "left")
        .join(
            how="left",
            other=media_df.alias("m"),
            on=[
                get_condition(media_df_order_by[0], aliases=("f", "m"), nullable=False),
                get_condition(media_df_order_by[1], aliases=("f", "m")),
                get_condition(media_df_order_by[2], aliases=("f", "m")),
            ]
        )
        .join(
            how="left",
            other=people_df.alias("p"),
            on=[
                get_condition(people_df_order_by[0], aliases=("f", "p")),
                get_condition(people_df_order_by[1], aliases=("f", "p"),nullable=False),
            ]
        )
        .select(
            spkf.col(f"f.{COL_ID}").alias(COL_ID),
            spkf.col(f"r.{COL_ID}").alias(COL_RELEASE_DATE),
            spkf.col(f"e.{COL_ID}").alias(COL_END_DATE),
            spkf.col(f"i.{COL_ID}").alias(COL_INGESTION_DATE),
            spkf.col(f"m.{COL_ID}").alias("media_id"),
            spkf.col(f"g.{COL_ID}").alias("genre_id"),
            spkf.col(f"c.{COL_ID}").alias(COL_OG_COUNTRY),
            spkf.col(f"p.{COL_ID}").alias("people_id"),
            spkf.col(f"o.{COL_ID}").alias("occupation_id"),
            spkf.col(f"f.{COL_MINUTE_DURATION}").alias(COL_MINUTE_DURATION),
            spkf.col(f"f.{COL_VOTE_AVERAGE}").alias(COL_VOTE_AVERAGE),
            spkf.col(f"f.{COL_VOTE_COUNT}").alias(COL_VOTE_COUNT),
            spkf.col(f"f.{COL_BUDGET}").alias(COL_BUDGET),
            spkf.col(f"f.{COL_REVENUE}").alias(COL_REVENUE),
            spkf.col(f"f.{COL_POPULARITY}").alias(COL_POPULARITY),
        )
    )
    
    print("Fact created!")
    complete_df.printSchema()
    complete_df.sample(fraction=.5).show(10)
    
    # Writing Data ____________________________________________________________
    stop = timeit.default_timer()
    print('Time to generate DataFrames: ', stop - start)
    print(f"Writing Data on {S3_TARGET_PATH}...")
    
    complete_df.write.mode("overwrite").parquet(FACT_MEDIA_EVALUATION_PATH)
    occupation_df.write.mode("overwrite").parquet(DIM_OCCUPATION_PATH)
    country_df.write.mode("overwrite").parquet(DIM_COUNTRY_PATH)
    genre_df.write.mode("overwrite").parquet(DIM_GENRE_PATH)
    media_df.write.mode("overwrite").parquet(DIM_MEDIA_PATH)
    date_df.write.mode("overwrite").parquet(DIM_DATE_PATH)
    people_df.write.mode("overwrite").parquet(DIM_PEOPLE_PATH)
    
    print("Data Write Complete!")
    # Custom Code End =========================================================
    job.commit()

if __name__ == "__main__": 
    main()