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
appNameSuffix = vendor + "_DataModels_syntheticFraud"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/syntheticFraud/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

df00WB1 = df.select(df.colRegex("`00WB1_[a-zA-Z0-9]+_\w*`"))

df00WB1Int = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`00WB1_[0-9]+_\w*`"))

df00WB1Str = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`00WB1_[a-zA-Z_]*`"))

synFraudCnt = sorted([int(col.split("_")[1]) for col in df00WB1Int.columns if col.split("_")[0] == "00WB1" \
                        and col.split("_")[-1] in ["derogatoryAlert","fileInquiriesImpactedScore","results"]])[-1]

for col in df00WB1Int.columns:
    instr = col.find('_', 7, 10)
    for i in range(synFraudCnt):
        rule = "00WB1_"+str(i+1)
        if not "00WB1_"+str(i+1)+"_"+col[instr+1:] in df00WB1Int.columns:
            df00WB1Int = df00WB1Int.withColumn("00WB1_"+str(i+1)+"_"+col[instr+1:], sf.lit(None))

for i in range(synFraudCnt):
    rule = "00WB1_"+str(i+1)
    df00WB1Int = df00WB1Int.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_scoreModel_score_derogatoryAlert"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_scoreModel_score_fileInquiriesImpactedScore"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_scoreModel_score_results"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

df00WB1Int = df00WB1Int.withColumn( "00WB1Array", sf.array([col for col in df00WB1Int.columns if col.split("_")[0] == "new00WB1"]) )

dfexplode = df00WB1Int.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("00WB1Array").alias("00WB1") \
            )

dfsplitCol = dfexplode.withColumn("00WB1_derogatoryAlert",sf.split("00WB1","\^")[0]) \
                     .withColumn("00WB1_fileInquiriesImpactedScore",sf.split("00WB1","\^")[1]) \
                     .withColumn("00WB1_results",sf.split("00WB1","\^")[2]) \
                     .drop("00WB1")

df00WB1Str = df00WB1Str.withColumn('00WB1_derogatoryAlert', sf.lit(None).cast(StringType())) \
                    .withColumn('00WB1_fileInquiriesImpactedScore', sf.lit(None).cast(StringType())) \
                    .withColumn('00WB1_results', sf.lit(None).cast(StringType())) \

df00WB1StrCol = df00WB1Str.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("00WB1_derogatoryAlert")
                            ,sf.col("00WB1_fileInquiriesImpactedScore")
                            ,sf.col("00WB1_results")
                            )

dfsplitColFinal = dfsplitCol.union(df00WB1StrCol)

df00WB1 = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("00WB1_derogatoryAlert")=="$$$",None).otherwise(sf.col("00WB1_derogatoryAlert")).alias("derogatoryAlert"), \
                sf.when(sf.col("00WB1_fileInquiriesImpactedScore")=="$$$",None).otherwise(sf.col("00WB1_fileInquiriesImpactedScore")).alias("fileInquiriesImpactedScore"), \
                sf.when(sf.col("00WB1_results")=="$$$",None).otherwise(sf.col("00WB1_results")).alias("results")) \
                .where(sf.col("derogatoryAlert").isNotNull() | \
                sf.col("fileInquiriesImpactedScore").isNotNull() | \
                sf.col("results").isNotNull() \
                )

df00WB1.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)