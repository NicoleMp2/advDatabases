import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc , sum

spark = SparkSession.builder.appName("Query2Dataframe").config("spark.executor.instances", "4").getOrCreate()
sys.stdout = open("outputs/Query2DF.txt", "w")

path = "hdfs://master:9000/user/user/"

CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()


#Filter the data for crimes that occurred on the "STREET",categorize and group the crimes by time of day, and order the result in descending order of count
Result = CrimeData.filter(col("Premis Desc") == "STREET").withColumn("Time of Day",
                   when((col("TIME OCC") >= 500) & (col("TIME OCC") <= 1159), "Morning")
                   .when((col("TIME OCC") >= 1200) & (col("TIME OCC") <= 1659), "Afternoon")
                   .when((col("TIME OCC") >= 1700) & (col("TIME OCC") <= 2059), "Evening")
                   .when((col("TIME OCC") >= 2100) | (col("TIME OCC") <= 459), "Night")
                   .otherwise(None)
                  )

Result = Result.groupBy("Time of Day").count().orderBy(desc("count"))



print("Query 2 Dataframe Execution Time: " + str(time.time() - startTime) + "\n")
print("===== Query 2 Dataframe Result =====")
Result.show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__

#TODO
spark.stop()