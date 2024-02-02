from pyspark.sql import Row
import sys
import time
from pyspark.sql import SparkSession


def TimeSegments(hour):
    if 500 <= hour <= 1159:
        return "Morning"
    elif 1200 <= hour <= 1659:
        return "Afternoon"
    elif 1700 <= hour <= 2059:
        return "Evening"
    else:
        return "Night"

path = "hdfs://master:9000/user/user/"
sys.stdout = open("outputs/Query2RDD.txt", "w")

spark = SparkSession.builder.appName("Query2RDD").config("spark.executor.instances", "4").getOrCreate()


CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()

#Filter the data for crimes that occurred on the "STREET",categorize and group the crimes by time of day, and order the result in descending order of count
# In the RDD version, we use map and reduceByKey to achieve the same result as the dataframe version
CrimeDataRDD = CrimeData.rdd.filter(lambda x: x['Premis Desc'] == "STREET")
Result = CrimeDataRDD.map(lambda row: (TimeSegments(int(row['TIME OCC'])), 1)).reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1], ascending=False)



print("Query 2 RDD Execution Time: " + str(time.time() - startTime) + "\n")
print("===== Query 2 RDD Result =====")
Result.toDF(["Time of Day","CrimeCount"]).show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__
spark.stop()