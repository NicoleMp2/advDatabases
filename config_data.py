import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , to_date , regexp_replace
from pyspark.sql.types import DateType, IntegerType, DoubleType


spark = SparkSession.builder.appName("PrepareData")\
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
    .getOrCreate()
sys.stdout = open("outputs/ConfigData.txt", "w")

# Read the CSV files, format where needed and combine datasets
CrimeData2010To2019 = spark.read.csv("data/Crime_Data_from_2010_to_2019.csv",header=True, inferSchema=True)
CrimeData2020ToPresent = spark.read.csv("data/Crime_Data_from_2020_to_Present.csv",header=True, inferSchema=True)
RevGeoCoding = spark.read.csv("data/revgecoding.csv",header=True, inferSchema=True)
IncomeData2015 = spark.read.csv("data/income/LA_income_2015.csv",header=True, inferSchema=True)

CrimeData2010ToPresent = CrimeData2010To2019.unionByName(CrimeData2020ToPresent, allowMissingColumns=True)

CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('Date Rptd', to_date(col('Date Rptd'), 'MM/dd/yyyy').cast(DateType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('DATE OCC', to_date(col('DATE OCC'), 'MM/dd/yyyy').cast(DateType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('Vict Age', col('Vict Age').cast(IntegerType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('LAT', col('LAT').cast(DoubleType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('LON', col('LON').cast(DoubleType()))

CrimeDataWithZIP = CrimeData2010ToPresent.join(RevGeoCoding, ['LAT', 'LON'], how='left')
CrimeDataWithIncome = CrimeDataWithZIP.join(IncomeData2015, CrimeDataWithZIP['ZIPcode'] == IncomeData2015['Zip Code'], how='left')

CrimeData = CrimeDataWithIncome.select(CrimeData2010ToPresent['*'],col('ZIPcode'), col('Community'),col('Estimated Median Income'))
CrimeData = CrimeData.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"),'[$,]','') ).withColumn('Estimated Median Income', col('Estimated Median Income').cast(IntegerType())) #i will use this column for q4


# Write the DataFrame to a CSV file
print("Total Rows:", CrimeData.count())
CrimeData.printSchema()
CrimeData.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save("CrimeData.csv")
sys.stdout.close()
sys.stdout = sys.__stdout__
spark.stop()