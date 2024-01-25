import sys
import time
from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder.appName("Query1SQL").config("spark.executor.instances", "4").getOrCreate()
sys.stdout = open("outputs/Query1SQL.txt", "a")

#TODO
CrimeData = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
startTime = time.time()


# Create a temporary SQL table for the CrimeData DataFrame
CrimeData.createOrReplaceTempView("CrimeDataTable")

# Write the SQL query for Query 1
sql_query = """
    SELECT Year, Month, CrimeCount, Rank
    FROM (
        SELECT Year, Month, COUNT(*) as CrimeCount,
               DENSE_RANK() OVER (PARTITION BY Year ORDER BY COUNT(*) DESC) as Rank
        FROM CrimeDataTable
        GROUP BY Year, Month
    ) tmp
    WHERE Rank <= 3
"""

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
result_sql = """
SELECT Year, Month, CrimeCount
FROM ({}) Top3Months
WHERE Rank <= 3
ORDER BY Year, Rank
""".format(Top3MonthsSQL)

# Execute the SQL query
Result = spark.sql(result_sql)


totalTime = time.time() - startTime


print("Query 1 Dataframe Execution Time: " + str(totalTime) + "\n")
print("===== Query 1 Dataframe Result =====")
Result.show(Result.count(), truncate=False)

sys.stdout.close()
sys.stdout = sys.__stdout__
# Stop the Spark session
spark.stop()
