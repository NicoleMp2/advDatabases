import sys
import time
from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder.appName("Query1SQL").config("spark.executor.instances", "4").getOrCreate()
sys.stdout = open("outputs/Query1SQL.txt", "a")

path = "hdfs://master:9000/user/user/data/"
CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()


CrimeData.createOrReplaceTempView("CrimeDataTable")

# Group by year and month and count the number of crimes
MonthlyCrimeCountSQL = """
SELECT YEAR(`DATE OCC`) AS Year, MONTH(`DATE OCC`) AS Month, COUNT(*) AS CrimeCount
FROM CrimeDataTable
GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)
"""

# Rank months within each year based on the crime count
Top3MonthsSQL = """
SELECT Year, Month, CrimeCount,
       DENSE_RANK() OVER (PARTITION BY Year ORDER BY CrimeCount DESC) AS Rank
FROM ({}) MonthlyCrimeCount
""".format(MonthlyCrimeCountSQL)

# Filter the top 3 months for each year
ResultSQL = """
SELECT Year, Month, CrimeCount
FROM ({}) Top3Months
WHERE Rank <= 3
ORDER BY Year, Rank
""".format(Top3MonthsSQL)

# Execute the SQL query
Result = spark.sql(ResultSQL)


totalTime = time.time() - startTime


print("Query 1 SQL Execution Time: " + str(totalTime) + "\n")
print("===== Query 1 SQL Result =====")
Result.show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__
# Stop the Spark session
spark.stop()
