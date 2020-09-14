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
appNameSuffix = vendor + "Spark_JSON_Parquet"

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

tgtfilePathFIDP9 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/fraud_IDScoreIDP9/"
tgtfilePathFA_D3 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/fraud_FA_D3/"
tgtfilePathFC360CM1 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/fraud_Comply360CM1/"
tgtfilePathFAttr = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/fraudAttributes/"
tgtfilePathRAttr = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskAttributes/"

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquetSrc.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0]) \
          # .withColumn("srcNewID",sf.coalesce(blank_as_null("applicantId"),sf.coalesce(blank_as_null("clientID"),blank_as_null("loanApplicationId")), sf.lit("^"))) \
          # .withColumn("srcNewID",sf.coalesce(blank_as_null("loanApplicationId"),sf.coalesce(blank_as_null("applicantId"),blank_as_null("clientID")), sf.lit("^"))) \
          # .withColumn("createdDatePT",sf.to_timestamp(udf_TZConversion(sf.regexp_replace(dfparquetSrc.createdDate,"T"," ").cast("string"),sf.lit("UTC"),sf.lit("US/Pacific")),"yyyy-MM-dd HH:mm:ss"))

# dfbaseData = df.select([col for col in df.columns])

dffraudIDP9 = df.select([col for col in df.columns if col.startswith("IDScoreIDP9")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dffraudFA_D3 = df.select([col for col in df.columns if col.startswith("FraudAttributesFA_D3")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dffraudComply360CM1 = df.select([col for col in df.columns if col.startswith("Comply360CM1")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dffraudAttributes = df.select([col for col in df.columns if col.startswith("ind_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dfriskAttributes = df.select([col for col in df.columns if col.startswith("var_TT_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dffraudIDP9.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathFIDP9)

dffraudFA_D3.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathFA_D3)

dffraudComply360CM1.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathFC360CM1)

dffraudAttributes.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathFAttr)

dfriskAttributes.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRAttr)