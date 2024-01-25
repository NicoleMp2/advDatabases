from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, DoubleType
import pandas as pd

# Δημιουργία του SparkSession
# spark = SparkSession.builder \
#     .master("spark://master:7077") \
#     .config("spark.dynamicAllocation.enabled", "false") \
#     .config("spark.executor.instances", "2") \
#     .appName("CrimeAnalysis") \
#     .getOrCreate()
# print("SparkSession created")

hdfs_path = "hdfs://master/data/"

# crime_data = spark.read.csv(hdfs_path + "crime_data.csv", header=True, inferSchema=True)





