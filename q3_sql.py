import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, create_map, lit, year
from itertools import chain

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
    CrimeData = spark.read.csv("CrimeData.csv",header=True, inferSchema=True)
    startTime = time.time()
    CrimeData2015 = CrimeData.filter(year(col("DATE OCC")) == 2015).createOrReplaceTempView("CrimeData2015")
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW MedianIncomes AS
        SELECT ZIPcode, "Estimated Median Income"
        FROM CrimeData2015
        WHERE `Vict Descent` IS NOT NULL AND ZIPcode IS NOT NULL AND "Estimated Median Income" IS NOT NULL
        GROUP BY ZIPcode
        ORDER BY "Estimated Median Income" DESC
    """).createOrReplaceTempView("MedianIncomes")

    TopZIPS = spark.sql("""SELECT * FROM MedianIncomes WHERE row_number() OVER (ORDER BY "Estimated Median Income" DESC) <= 3""")
    BottomZIPS = spark.sql("""SELECT * FROM MedianIncomes WHERE row_number() OVER (ORDER BY "Estimated Median Income" ASC) <= 3""")
    Total = TopZIPS.union(BottomZIPS)

    Result = spark.sql("""
        SELECT "Vict Descent", COUNT(*) as count
        FROM CrimeData2015
        JOIN Total
        ON CrimeData2015.ZIPcode = Total.ZIPcode
        WHERE "Vict Descent" IS NOT NULL
        GROUP BY "Vict Descent"
        ORDER BY count DESC
    """)

    MappingExpr = create_map([lit(x) for x in chain(*DescentMapping.items())])
    Result = Result.withColumn("Vict Descent", MappingExpr.getItem(col("Vict Descent"))).filter(col("Vict Descent").isNotNull())

    totalTime = time.time() - startTime

    print("Query 3 SQL Execution Time: " + str(totalTime) + "with " + str(executor) + " executors" + "\n")
    if executor == 4:
        print("===== Query 3 SQL Result =====")
        Result.show()

#TODO
    spark.stop()
sys.stdout.close()
sys.stdout = sys.__stdout__
