# src/mov_dynamic/parse.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer
import sys

APP_NAME = sys.argv[1]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# read json
SAVE_PATH = '/home/nishtala/data/mov_dynamic'
date = "2023"
json_df = spark.read.option("multiline", "true").json(f"{SAVE_PATH}/movie_data_{date}.json")

exploded_df = json_df.withColumn("company", explode_outer("companys")) 
exploded_df = exploded_df.withColumn("director", explode_outer("directors"))

exploded_df.write.parquet(f"{SAVE_PATH}/movie_data_parsed_{date}.parquet")

spark.stop()
