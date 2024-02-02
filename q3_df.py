import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc , year, create_map, lit , asc, regexp_replace
from itertools import chain
from pyspark.sql.types import IntegerType

path = "hdfs://master:9000/user/user/"
sys.stdout = open("outputs/Query3DF.txt", "w")

DescentMapping = {
    'A' : 'Other Asian', 'B' : 'Black', 'C' : 'Chinese', 'D' : 'Cambodian',
    'F' : 'Filipino', 'G' : 'Guamanian', 'H' : 'Hispanic/Latin/Mexican',
    'I' : 'American Indian/Alaskan Native', 'J' : 'Japanese', 'K' : 'Korean',
    'L' : 'Laotian', 'O' : 'Other', 'P' : 'Pacific Islander', 
    'S' : 'Samoan', 'U' : 'Hawaiian', 'V' : 'Vietnamese', 'W' : 'White',
    'X' : 'Unknown', 'Z' : 'Asian Indian'
    }


for executor in [2,3,4]:
    spark = SparkSession.builder.appName("Query3Dataframe"+str(executor)+"Executors").config("spark.executor.instances", executor).getOrCreate()

    CrimeData = spark.read.csv(path+"CrimeData.csv",header=True, inferSchema=True)
    Income2015 = spark.read.csv(path+"data/LA_income_2015.csv",header=True, inferSchema=True)
    startTime = time.time()

    #Filter the data for crimes that occurred in 2015, and remove rows with null values in the "Vict Descent", "ZIPcode", and "Estimated Median Income" columns
    CrimeData2015 = CrimeData.filter((year(col("DATE OCC")) == 2015) & col("Vict Descent").isNotNull() & col("ZIPcode").isNotNull() & col("Estimated Median Income").isNotNull()).select("Vict Descent", "ZIPcode", "Estimated Median Income")

    # to find the top 3 and bottom 3 zipcodes by median income
    TopZIPS = Income2015.orderBy(desc("Estimated Median Income")).select("Zip Code").limit(3)
    BottomZIPS = Income2015.orderBy(asc("Estimated Median Income")).select("Zip Code").limit(3)
    ZipsToJoin = TopZIPS.union(BottomZIPS)


    Result = CrimeData2015.join(ZipsToJoin, on=CrimeData2015["ZIPcode"] == ZipsToJoin["Zip Code"], how="inner").groupBy("Vict Descent").count().orderBy(desc("count"))


    # to map the victim descent to the actual name
    MappingExpr = create_map([lit(x) for x in chain(*DescentMapping.items())])
    Result = Result.withColumn("Vict Descent", MappingExpr.getItem(col("Vict Descent")))

    print("Query 3 Dataframe Execution Time: " + str(time.time() - startTime) + "with " + str(executor) + " executors" + "\n")
    if executor == 4:
        print("===== Query 3 Dataframe Result =====")
        Result.show()

    spark.stop()

sys.stdout.close()
sys.stdout = sys.__stdout__
