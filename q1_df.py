from pyparsing import col
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, desc , count , dense_rank
from pyspark.sql.window import Window


# Create a Spark session
spark = SparkSession.builder.appName("CrimeAnalysis").getOrCreate()
crime_data = spark.read.csv("crime_data.csv",header=True, inferSchema=True)
# Assuming you have loaded your crime_data DataFrame

window_spec = Window.partitionBy(year(col('DATE OCC'))).orderBy(col('DATE OCC').desc())

# Add a row number for each month within each year
df_with_row_number = df.withColumn('row_number', F.row_number().over(window_spec))

# Find the top 3 months for each year
top_3_months_per_year = df_with_row_number.filter(col('row_number') <= 3) \
    .groupBy(year(col('DATE OCC')).alias('Year'), month(col('DATE OCC')).alias('Month')) \
    .agg({'DATE OCC': 'count'}) \
    .orderBy('Year')

# Display the results
top_3_months_per_year.show(100, False)
# Κλείσιμο του Spark Session
spark.stop()