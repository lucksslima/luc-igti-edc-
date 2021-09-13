from pyspark.sql import SparkSession
import pyspark.sql.functions as f 

if __name__ == "__main__":
      spark = (SparkSession.builder
             .master("local[3]")
             .appName("estudando-pyspark-csv")
             .getOrCreate())

      spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
      spark.conf.set("spark.sql.repl.eagerEval.enabled",True) 

    
      df = (spark
          .read
          .format("csv")
          .option("delimiter",";")
          .option("inferSchema", True)
          .option("header", True)
          .load("/home/lucksslima/Downloads/data/microdados_enem_2019/DADOS/MICRODADOS_ENEM_2019.csv"))

#comentário só pra modificar o arquivo no terraform

df.select(f.col("NU_INSCRICAO"),f.col("NU_IDADE"),f.col("NU_NOTA_REDACAO")).show(5)

     