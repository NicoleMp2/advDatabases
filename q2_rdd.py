from pyspark.sql import Row
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, desc , sum

# Create a Spark session
spark = SparkSession.builder.appName("Query2RDD").config("spark.executor.instances", "4").getOrCreate()
sys.stdout = open("outputs/Query2RDD.txt", "w")

#TODO
CrimeData = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()

CrimeDataRDD = CrimeData.rdd.filter(lambda x: x['Premis Desc'] == "STREET")

def TimeSegments(hour):
    if 500 <= hour <= 1159:
        return "Morning"
    elif 1200 <= hour <= 1659:
        return "Afternoon"
    elif 1700 <= hour <= 2059:
        return "Evening"
    else:
        return "Night"



Result = CrimeDataRDD.map(lambda row: (TimeSegments(int(row['TIME OCC'])), 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)


totalTime = time.time() - startTime

print("Query 2 RDD Execution Time: " + str(totalTime) + "\n")
print("===== Query 2 RDD Result =====")
Result.toDF(["Time of Day","CrimeCount"]).show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__

#TODO
spark.stop()