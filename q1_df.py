from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, desc ,rank, col , sum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# Create a Spark session
spark = SparkSession.builder.appName("Query1Dataframe").config("spark.executor.instances", "4").getOrCreate()

crime_data = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
# Assuming you have loaded your crime_data DataFrame

# Extract year and month from the "Date Rptd" column
crime_data = crime_data.withColumn("Year", year("DATE OCC")).withColumn("Month", month("DATE OCC"))

monthly_crime_count = crime_data.groupBy("Year", "Month").count()

# Define a Window specification to rank months within each year based on the crime count
windowSpec = Window.partitionBy("Year").orderBy(desc(col("count")))

# Calculate the crime count for each month within each year
result_df = (monthly_crime_count.withColumn("Rank", rank().over(windowSpec))).filter(col("Rank") <= 3).orderBy("Year", "Rank")

# Print the result
result_df.show()

# Stop the Spark session
spark.stop()
