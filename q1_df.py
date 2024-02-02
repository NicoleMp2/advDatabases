import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, desc , col ,dense_rank
from pyspark.sql.window import Window

# Create a Spark session
path = "hdfs://master:9000/user/user/"
sys.stdout = open("outputs/Query1DF.txt", "w")

spark = SparkSession.builder.appName("Query1Dataframe").config("spark.executor.instances", 4).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()

# Extract year and month from the "DATE OCC" column
CrimeDataYYMM = CrimeData.withColumn("Year", year("DATE OCC")).withColumn("Month", month("DATE OCC"))
MonthlyCrimeCount = CrimeDataYYMM.groupBy("Year", "Month").count()
# Define a Window specification to rank months within each year based on the crime count
windowSpec = Window.partitionBy("Year").orderBy(desc(col("count")))

# Calculate the crime count for each month within each year
Result = (MonthlyCrimeCount.withColumn("Rank", dense_rank().over(windowSpec))).filter(col("Rank") <= 3).orderBy("Year", "Rank")




print("Query 1 Dataframe Execution Time: " + str(time.time() - startTime) + "\n")
print("===== Query 1 Dataframe Result =====")
Result.show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__

spark.stop()
