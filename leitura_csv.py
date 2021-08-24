from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col, mean, count
import pandas as pd

if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[3]")
             .appName("leitura_nordil_csv")
             .getOrCreate())

    dfn = (spark
           .read
           .format("csv")
           .option("header",True)
           .option("inferSchema", True)
           .option("delimiter",",")
           .load("/home/lucksslima/Downloads/csv/nordilmov_2018-2021.csv"))
    dfn.show(10)
    dfn.printSchema()



