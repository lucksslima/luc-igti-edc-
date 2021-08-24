from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (SparkSession.builder
             .master("local[3]")
             .appName("estudando-pyspark-csv")
             .getOrCreate())

    df = (spark
          .read
          .format("csv")
          .option("delimiter",";")
          .option("inferSchema", True)
          .option("header", True)
          .load("/home/lucksslima/Downloads/data/microdados_enem_2019/DADOS/MICRODADOS_ENEM_2019.csv"))

    df.show()
    df.printSchema()

    #comentário só pra modificar o arquivo no terraform
    