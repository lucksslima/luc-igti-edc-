resource "aws_s3_bucket_object" "spark_job" {
    bucket = aws_s3_bucket.dl.id
    key = "emr-code/pyspark/pyspark_teste_csv_from_tf.py"
    acl = "private"
    source = "../pyspark_teste_csv.py"
    etag = filemd5("../pyspark_teste_csv.py")
}