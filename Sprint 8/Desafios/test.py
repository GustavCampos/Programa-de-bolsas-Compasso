'''
This script is used to test AWS Glue Configuration for an aws glue docker container with public data on a S3 bucket.
This script is used in this tutorial https://www.youtube.com/watch?v=-4ZnJkM-QDk by Adriano Nicolucci and is not meant
to be used in production.
'''



import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import requests

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
inputDF = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options = {"paths": ["s3://gustavcampos-pb-data-lake/Temp/progress.json"]}, format = "json"
)

url = "https://api.themoviedb.org/3/genre/movie/list?language=en"

headers = {
    "accept": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyZTNjZjU1YzQ0MWI5NTJhMzhkZmQ4MWRhNWVjNjM0MyIsIm5iZiI6MTcyMTA3ODM1NC42Mjc4NDcsInN1YiI6IjY2OGQ4YzVkODQyZjlhYTkyM2IyZTM0MSIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.WB0fpTGiX9qSs7hys1-_Rgurfg9-5nGrwabWXvG6lbA"
}

response = requests.get(url, headers=headers)

print(response.text)

inputDF.show(5)
df = inputDF.toDF()
df_pd = df.toPandas()
print('test script has successfully ran')