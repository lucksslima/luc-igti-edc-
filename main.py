from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[3]")
             .appName("estudando-pyspark-ojdbc")
             .getOrCreate())

    query = "(SELECT * FROM NORDIL.PCMOV p2  WHERE DTMOV > TO_DATE('01/01/2018','DD/MM/YYYY'))"

    df = (spark
          .read
          .format("jdbc")
          .option("url", "jdbc:oracle:thin:@192.168.8.2:1521:WTNDJP")
          .option("dbtable",query)
          .option("user","biviews")
          .option("password","BI7789x3")
          .option("driver","oracle.jdbc.driver.OracleDriver")).load()

    df.show()
    df.printSchema()

    (
        df
        .write
        .mode("overwrite")
        .format("csv")
        .save("/home/lucksslima/Downloads/csv")
    )