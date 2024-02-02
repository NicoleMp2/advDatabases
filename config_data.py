import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , to_date , regexp_replace, year
from pyspark.sql.types import DateType, IntegerType, DoubleType

path = "hdfs://master:9000/user/user/"
sys.stdout = open("outputs/ConfigData.txt", "w")

spark = SparkSession.builder.appName("PrepareData").config("spark.sql.legacy.timeParserPolicy", "LEGACY").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the CSV files, format where needed and combine datasets
CrimeData2010To2019 = spark.read.csv(path+"data/Crime_Data_from_2010_to_2019.csv",header=True, inferSchema=True)
CrimeData2020ToPresent = spark.read.csv(path+"data/Crime_Data_from_2020_to_Present.csv",header=True, inferSchema=True)
RevGeoCoding = spark.read.csv(path+"data/revgecoding.csv",header=True, inferSchema=True)
IncomeData2015 = spark.read.csv(path+"data/income/LA_income_2015.csv",header=True, inferSchema=True)

#Concate the crime data sets
CrimeData2010ToPresent = CrimeData2010To2019.unionByName(CrimeData2020ToPresent, allowMissingColumns=True)

#Format the Types
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('Date Rptd', to_date(col('Date Rptd'), 'MM/dd/yyyy').cast(DateType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('DATE OCC', to_date(col('DATE OCC'), 'MM/dd/yyyy').cast(DateType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('Vict Age', col('Vict Age').cast(IntegerType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('LAT', col('LAT').cast(DoubleType()))
CrimeData2010ToPresent = CrimeData2010ToPresent.withColumn('LON', col('LON').cast(DoubleType()))
IncomeData2015 = IncomeData2015.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"),'[$,]','') ).withColumn('Estimated Median Income', col('Estimated Median Income').cast(IntegerType()))

#Join the datasets
CrimeDataWithZIP = CrimeData2010ToPresent.join(RevGeoCoding, ['LAT', 'LON'], how='left')

CrimeDataWithIncome = CrimeDataWithZIP.join(IncomeData2015, (CrimeDataWithZIP['ZIPcode'] == IncomeData2015['Zip Code']) & (year(CrimeDataWithZIP['DATE OCC']) == 2015) , how='left')

CrimeData = CrimeDataWithIncome.select(CrimeData2010ToPresent['*'],col('ZIPcode'), col('Community'),col('Estimated Median Income'))




# Write the DataFrame to a CSV file
print("Total Rows:", CrimeData.count())
CrimeData.printSchema()
CrimeData.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(path+"CrimeData.csv")
IncomeData2015.coalesce(1).write.format("csv").mode("overwrite").option("header", True).save(path+"IncomeData2015.csv")
sys.stdout.close()
sys.stdout = sys.__stdout__
spark.stop()