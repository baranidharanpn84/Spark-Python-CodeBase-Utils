# %%spark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from pyspark import SQLContext
from datetime import date,datetime,timedelta
from pytz import timezone
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3
import sys
import uuid

##########################################################################################################

def get_recent_dir (prefix_input):
    while True:
        result = client.list_objects(Bucket=bucket, Prefix=prefix_input, Delimiter='/',MaxKeys=1500)
        if result.get('CommonPrefixes') == None:
            last_dir = prefix_input
            return last_dir
        else:
            last_dir = sorted([prefix.get('Prefix') for prefix in result.get('CommonPrefixes')])[-1]
            prefix_input = last_dir
    
    return prefix_input

##########################################################################################################

def blank_as_null(x):
    return when(sf.col(x) != "", sf.col(x)).otherwise(None)

##########################################################################################################

# bucket = ""
# access_key = ""
# secret_key = ""
# vendor = ""
# date = "1900-01-01"

bucket = sys.argv[1]
access_key = sys.argv[2]
secret_key = sys.argv[3]
vendor = sys.argv[4]
date = sys.argv[5]
enriched_path = sys.argv[6]
appNameSuffix = vendor + "_Spark_DataModels"

year = date.split('-',1)[0]
month = date.split('-',2)[1]
day = date.split('-',3)[2]

# If you run in pyspark, ignore sc = SparkContext(). Else if you run via spark-submit, uncomment this.
sc = SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", "" + access_key + "")
sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", ""+ secret_key + "")
sc._jsc.hadoopConfiguration().set("hadoop.tmp.dir", "/mnt/var/lib/hadoop/tmp/"+str(uuid.uuid4()))

sparkSession = (SparkSession
                .builder
                .appName('SparkApp_' + appNameSuffix)
                # .config("spark.hadoop.fs.s3.enableServerSideEncryption", "true")
                # .config("spark.hadoop.fs.s3.serverSideEncryptionAlgorithm", "aws:kms")
                # .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
                .config("spark.sql.parquet.filterPushdown", "true")
                .config("spark.sql.parquet.mergeSchema", "true")
                .config("spark.sql.caseSensitive","true")
                .config("spark.sql.shuffle.partitions","5")
                .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                .getOrCreate())

client = boto3.client('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-west-2")

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathBD = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/baseData/"

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquetSrc.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0]) \
          # .withColumn("srcNewID",sf.coalesce(blank_as_null("loanApplicationId"),sf.coalesce(blank_as_null("applicantId"),blank_as_null("clientID")), sf.lit("^"))) \
          # .withColumn("createdDatePT",sf.to_timestamp(udf_TZConversion(sf.regexp_replace(dfparquetSrc.createdDate,"T"," ").cast("string"),sf.lit("UTC"),sf.lit("US/Pacific")),"yyyy-MM-dd HH:mm:ss"))

dfbaseData = df.select([col for col in df.columns])

dfbaseData.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathBD)