import boto3
import pandas as pd

# vamos criar um cliente para interagir com o AWS S3
s3client = boto3.client("s3")
s3 = boto3.resource('s3')

s3client.download_file("datalake-igti-luc",
                        "/data/dadospnad.csv",
                        "/data/dadospnad.csv")

