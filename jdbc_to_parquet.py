from pyspark import SparkConf
from pyspark.sql import SparkSession


if __name__ == "__main__":
    
    spark = (SparkSession.builder
             .master("local[3]")
             .appName("jdbc_to_parquet")
             .getOrCreate())
    spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite","LEGACY")

    
    query = "(SELECT * FROM NORDIL.PCMOV p2  WHERE DTMOV = TO_DATE('20/08/2021','DD/MM/YYYY'))"

    df = (spark
          .read
          .format("jdbc")
          .option("url", "jdbc:oracle:thin:@192.168.8.2:1521:WTNDJP")
          .option("dbtable",query)
          .option("user","biviews")
          .option("password","BI7789x3")
          .option("driver","oracle.jdbc.driver.OracleDriver")).load()

    
    (df
        .write
        .mode("append")
        .format("parquet")
        .option("header", True)
        .save("/home/lucksslima/Downloads/csv")
    )
