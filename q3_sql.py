import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, year , regexp_replace
from itertools import chain
from pyspark.sql.types import IntegerType

# Create a Spark session
DescentMapping = {
    'A' : 'Other Asian', 'B' : 'Black', 'C' : 'Chinese', 'D' : 'Cambodian',
    'F' : 'Filipino', 'G' : 'Guamanian', 'H' : 'Hispanic/Latin/Mexican',
    'I' : 'American Indian/Alaskan Native', 'J' : 'Japanese', 'K' : 'Korean',
    'L' : 'Laotian', 'O' : 'Other', 'P' : 'Pacific Islander', 
    'S' : 'Samoan', 'U' : 'Hawaiian', 'V' : 'Vietnamese', 'W' : 'White',
    'X' : 'Unknown', 'Z' : 'Asian Indian'
    }


sys.stdout = open("outputs/Query3SQL.txt", "w")
for executor in [2,3,4]:
    spark = SparkSession.builder.appName("Query3SQL"+str(executor)+"Executors").config("spark.executor.instances", executor).getOrCreate()
#TODO
    startTime = time.time()
    CrimeData = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
    Income2015 = spark.read.csv("data/income/LA_income_2015.csv",header=True, inferSchema=True)

    CrimeData.createOrReplaceTempView("CrimeData")
    Income2015.createOrReplaceTempView("Income2015")

    spark.sql("""
        SELECT `Zip Code`, CAST(regexp_replace(`Estimated Median Income`, '[$,]', '') AS INT) AS `Estimated Median Income`
        FROM Income2015
        WHERE `Estimated Median Income` IS NOT NULL
    """).createOrReplaceTempView("Income2015Filtered")


    spark.sql("""
        SELECT `Vict Descent`, `ZIPcode`, `Estimated Median Income`
        FROM CrimeData
        WHERE YEAR(`DATE OCC`) = 2015 AND `Vict Descent` IS NOT NULL AND `ZIPcode` IS NOT NULL AND `Estimated Median Income` IS NOT NULL
    """).createOrReplaceTempView("CrimeData2015")

    spark.sql("""
        SELECT `Zip Code`
        FROM Income2015Filtered
        ORDER BY `Estimated Median Income` DESC
        LIMIT 3
    """).union(
        spark.sql("""
            SELECT `Zip Code`
            FROM Income2015Filtered
            ORDER BY `Estimated Median Income` ASC
            LIMIT 3
        """)).createOrReplaceTempView("ZipsToJoin")

    Result = spark.sql("""
        SELECT `Vict Descent`, COUNT(*) AS `Count`
        FROM CrimeData2015
        JOIN ZipsToJoin ON CrimeData2015.`ZIPcode` = ZipsToJoin.`Zip Code`
        GROUP BY `Vict Descent`
        ORDER BY `Count` DESC
    """)


    MappingExpr = create_map([lit(x) for x in chain(*DescentMapping.items())])
    Result = Result.withColumn("Vict Descent", MappingExpr.getItem(col("Vict Descent")))

    totalTime = time.time() - startTime

    print("Query 3 SQL Execution Time: " + str(totalTime) + "with " + str(executor) + " executors" + "\n")
    if executor == 4:
        print("===== Query 3 SQL Result =====")
        Result.show()

#TODO
    spark.stop()
sys.stdout.close()
sys.stdout = sys.__stdout__
