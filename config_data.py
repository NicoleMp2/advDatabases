from pandas import to_datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, DoubleType
from pyspark.sql.functions import to_date


# spark = SparkSession.builder.appName("PrepareData").getOrCreate()
spark = SparkSession.builder.appName("PrepareData")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()
crime_data_2010_to_2019 = spark.read.csv("Crime_Data_from_2010_to_2019.csv",header=True, inferSchema=True)
crime_data_2020_to_present = spark.read.csv("Crime_Data_from_2020_to_Present.csv",header=True, inferSchema=True)


crime_data = crime_data_2010_to_2019.union(crime_data_2020_to_present)

# Adjust data types using select and cast

crime_data = crime_data.withColumn('Date Rptd', to_date(col('Date Rptd'), 'MM/dd/yyyy').cast(DateType()))
crime_data = crime_data.withColumn('DATE OCC', to_date(col('DATE OCC'), 'MM/dd/yyyy').cast(DateType()))
crime_data = crime_data.withColumn('Vict Age', col('Vict Age').cast(IntegerType()))
crime_data = crime_data.withColumn('LAT', col('LAT').cast(DoubleType()))
crime_data = crime_data.withColumn('LON', col('LON').cast(DoubleType()))
print("Total Rows:", crime_data.count())
crime_data.printSchema()


# crime_data.coalesce(1).write.format("csv").save("crime_data.csv", header=True)
# Write the DataFrame to a CSV file
crime_data.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save("crime_data.csv")

spark.stop()