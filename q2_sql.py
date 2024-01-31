import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc , sum

# Create a Spark session
spark = SparkSession.builder.appName("Query2SQL").config("spark.executor.instances", "4").getOrCreate()
sys.stdout = open("outputs/Query2SQL.txt", "w")

path = "hdfs://master:9000/user/user/data/"

CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()

CrimeData.createOrReplaceTempView("CrimeDataTable")

TimeOfDaySQL = """
    SELECT *,
           CASE
               WHEN `TIME OCC` BETWEEN 500 AND 1159 THEN "Morning"
               WHEN `TIME OCC` BETWEEN 1200 AND 1659 THEN "Afternoon"
               WHEN `TIME OCC` BETWEEN 1700 AND 2059 THEN "Evening"
               WHEN `TIME OCC` < 460 OR `TIME OCC` >= 2100 THEN "Night"
               ELSE NULL
           END AS `Time of Day`
    FROM CrimeDataTable
    WHERE `Premis Desc` = "STREET"
"""
ResultSQL = """
    SELECT `Time of Day`, COUNT(*) AS CrimeCount
    FROM ({})
    GROUP BY `Time of Day`
    ORDER BY CrimeCount DESC
""".format(TimeOfDaySQL)

Result = spark.sql(ResultSQL)

totalTime = time.time() - startTime

print("Query 2 SQL Execution Time: " + str(totalTime) + "\n")
print("===== Query 2 SQL Result =====")
Result.show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__

#TODO
spark.stop()