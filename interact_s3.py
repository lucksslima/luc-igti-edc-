import boto3
import pandas as pd

# vamos criar um cliente para interagir com o AWS S3
s3_client = boto3.client("s3")

s3_client.download_file("datalake-igti-luc",
                        "/data/ITENS_PROVA_2019.csv",
                        "/data/ITENS_PROVA_2019.csv")

df = pd.read_csv("/data/ITENS_PROVA_2019.csv")
print(df)