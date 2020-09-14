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

#Create function and register udf
def funcTZConversion (ip_dt_str,from_tz,to_tz):
    fmt = "%Y-%m-%d %H:%M:%S"
    datetime_obj_naive = datetime.strptime(ip_dt_str,fmt)
    datetime_obj_tz = timezone(from_tz).localize(datetime_obj_naive)
    tgt_tz = timezone(to_tz)
    tgt_dtime = datetime_obj_tz.astimezone(tgt_tz)
    return tgt_dtime.strftime(fmt)

udf_TZConversion = udf(funcTZConversion)

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

srcfilePathUIToken = "s3://" + bucket + "/" + enriched_path + vendor + "/JSON/" + year + "/" + month + "/" + day +""
# srcfilePathAccessToken = "s3://" + bucket + "/" + enriched_path + vendor + "/JSON/*/*/*/accesstoken_*"

tgtfilePathUIToken = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/"
# tgtfilePathUIToken = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/uiToken/"
# tgtfilePathAccessToken = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/accessToken/"

dfjsonUIToken = sparkSession.read.format("json").option("multiline", "true").option("inferSchema", "true").load(srcfilePathUIToken)
# dfjsonAccessToken = sparkSession.read.format("json").option("multiline", "true").option("inferSchema", "true").load(srcfilePathAccessToken)

#data = dfjson.select(explode("DATA").alias("data"))
dataUIToken = dfjsonUIToken.withColumn("data", explode("DATA")).select("data.*")
# dataAccessToken = dfjsonAccessToken.withColumn("data", explode("DATA")).select("data.*")

# dfPT = data.withColumn("createdDatePT",sf.to_timestamp(udf_TZConversion(sf.regexp_replace(data.createdDate,"T"," ").cast("string"),sf.lit("UTC"),sf.lit("US/Pacific")),"yyyy-MM-dd HH:mm:ss"))
dfPTUIToken = dataUIToken.withColumn("createdDatePT",sf.from_utc_timestamp(sf.regexp_replace(dataUIToken.createdDate,"T"," "),"US/Pacific"))
# dfPTAccessToken = dataAccessToken.withColumn("createdDatePT",sf.from_utc_timestamp(sf.regexp_replace(dataAccessToken.createdDate,"T"," "),"US/Pacific"))

dfUIToken = dfPTUIToken.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfAccessToken = dfPTAccessToken.withColumn("year",sf.split("createdDate","\-")[0]) \
          # .withColumn("month",sf.split("createdDate","\-")[1]) \
          # .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseDataUIToken = dfUIToken.select([col for col in dfUIToken.columns])

# dfbaseDataAccessToken = dfAccessToken.select([col for col in dfAccessToken.columns])

#dfbaseData.show(10,False)

# dfrankedId = dfbaseData.withColumn("row_num", sf.row_number().over(Window.partitionBy("id").orderBy(sf.asc("updatedAt")))) \
                    # .where(sf.col("row_num") == 1) \
                    # .select(dfbaseData["*"])

dfbaseDataUIToken.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathUIToken)

# dfbaseDataAccessToken.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathAccessToken)