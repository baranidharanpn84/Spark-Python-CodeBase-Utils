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

concat_udf = sf.udf(lambda cols: "".join([x if x is not None else "" for x in cols]), StringType())

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
appNameSuffix = vendor + "Spark_DataModels"

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
                .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

#dfbaseData.show(10,False)

dfbaseData = dfbaseData.withColumn("conn_auto_updates", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_auto_updates")]))) \
                    .withColumn("conn_config_instructions", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_config_instructions")]))) \
                    .withColumn("conn_created", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_created")]))) \
                    .withColumn("conn_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("connection_id") or col.endswith("connections_id")]))) \
                    .withColumn("conn_institution_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_institution_id")]))) \
                    .withColumn("conn_institution_name", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_institution_name")]))) \
                    .withColumn("conn_is_disabled", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_is_disabled")]))) \
                    .withColumn("conn_is_oauth", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_is_oauth")]))) \
                    .withColumn("conn_last_good_sync", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_last_good_sync")]))) \
                    .withColumn("conn_last_sync", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_last_sync")]))) \
                    .withColumn("conn_status", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_status")]))) \
                    .withColumn("conn_user_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_user_id")]))) \
                    .withColumn("conn_username", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_username")]))) \
                    .withColumn("conn_value", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_value")]))) \

dfbaseDataFinal = dfbaseData.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("applicantId"),sf.col("applicationSource") \
                                   ,sf.col("clientID"),sf.col("createdDate"),sf.col("loanApplicationId"),sf.col("mvpApplicantId"),sf.col("mvpClientID") \
                                   ,sf.col("mvpLoanApplicationId"),sf.col("noHit"),sf.col("successful"),sf.col("timestamp"),sf.col("updatedAt") \
                                   ,sf.col("userId"),sf.col("createdDatePT"),sf.col("conn_auto_updates"),sf.col("conn_config_instructions") \
                                   ,sf.col("conn_created"),sf.col("conn_id"),sf.col("conn_institution_id"),sf.col("conn_institution_name") \
                                   ,sf.col("conn_is_disabled"),sf.col("conn_is_oauth"),sf.col("conn_last_good_sync"),sf.col("conn_last_sync") \
                                   ,sf.col("conn_status"),sf.col("conn_user_id"),sf.col("conn_username"),sf.col("conn_value") \
                                   )

dfbaseDataFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathBD)