from pyspark.sql import SparkSession
from pyspark.sql.functions import dense_rank
from 
# Create a Spark session
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()

# Assuming you have loaded your crime_data DataFrame

# Create a temporary SQL table for the crime_data DataFrame
crime_data.createOrReplaceTempView("crime_data_table")

# Write the SQL query for Query 1
sql_query = """
    SELECT Year, Month, CrimeCount, Rank
    FROM (
        SELECT Year, Month, COUNT(*) as CrimeCount,
               DENSE_RANK() OVER (PARTITION BY Year ORDER BY COUNT(*) DESC) as Rank
        FROM crime_data_table
        GROUP BY Year, Month
    ) tmp
    WHERE Rank <= 3
"""

# Execute the SQL query
result_sql_df = spark.sql(sql_query)

# Print the result
result_sql_df.show()

# Stop the Spark session
spark.stop()
