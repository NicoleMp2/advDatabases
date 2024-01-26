import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc , year, create_map, lit
from itertools import chain

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

#TODO
    CrimeData = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
    startTime = time.time()

    CrimeData2015 = CrimeData.filter(year(col("DATE OCC")) == 2015).select("Vict Descent", "ZIPcode", "Estimated Median Income")
    # to find the top 3 and bottom 3 zipcodes by median income
    MedianIncomes = CrimeData2015.filter(col("Vict Descent").isNotNull() & col("ZIPcode").isNotNull() & col("Estimated Median Income").isNotNull()).select("ZIPcode", "Estimated Median Income").dropDuplicates(["ZIPcode"]).orderBy(desc("Estimated Median Income"))
    
    TopZIPS = MedianIncomes.limit(3)
    BottomZIPS = MedianIncomes.orderBy(asc("Estimated Median Income")).limit(3)
    Total = TopZIPS.union(BottomZIPS)

    Result = CrimeData2015.join(Total.select(col("ZIPcode")), "ZIPcode").groupBy("Vict Descent").count().orderBy(desc("count"))
    


    MappingExpr = create_map([lit(x) for x in chain(*DescentMapping.items())])

    Result = Result.withColumn("Vict Descent", MappingExpr.getItem(col("Vict Descent"))).filter(col("Vict Descent").isNotNull())


    totalTime = time.time() - startTime

    print("Query 3 Dataframe Execution Time: " + str(totalTime) + "with " + str(executor) + " executors" + "\n")
    if executor == 4:
        print("===== Query 3 Dataframe Result =====")
        Result.show()

    
#TODO
    spark.stop()

sys.stdout.close()
sys.stdout = sys.__stdout__
