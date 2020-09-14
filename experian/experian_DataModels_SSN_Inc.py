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
appNameSuffix = vendor + "_DataModels_SSN"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/SSN/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfcsSSN = df.select(df.colRegex("`csSSN_[a-zA-Z0-9]+_\w*`"))

dfcsSSNInt = df.select(df.colRegex("`csSSN_[0-9]+_\w*`"))

dfcsSSNStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csSSN_[a-zA-Z_]*`"))

SSNCnt = sorted([int(col.split("_")[1]) for col in dfcsSSNInt.columns if col.split("_")[0] == "csSSN" \
                        and col.split("_")[2] in ["Number","VariationIndicator","VariationIndicator_code"]])[-1]

for i in range(SSNCnt):
    rule = "csSSN_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_Number"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_VariationIndicator"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_VariationIndicator_code"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

df = df.withColumn( "csSSNArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsSSN"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csSSNArray").alias("csSSN") \
            )

dfsplitCol = dfexplode.withColumn("csSSN_Number",sf.split("csSSN","\^")[0]) \
                     .withColumn("csSSN_VariationIndicator",sf.lit(None).cast(StringType())) \
                     .withColumn("csSSN_VariationIndicator_code",sf.split("csSSN","\^")[1]) \
                     .drop("csSSN")

dfcsSSNStr = dfcsSSNStr.withColumn('csSSN_VariationIndicator', sf.lit(None).cast(StringType()))

dfcsSSNStrCol = dfcsSSNStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csSSN_Number")
                            ,sf.col("csSSN_VariationIndicator")
                            ,sf.col("csSSN_VariationIndicator_code")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsSSNStrCol)

dfcsSSN = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csSSN_Number")=="$$$",None).otherwise(sf.col("csSSN_Number")).alias("Number"), \
                sf.when(sf.col("csSSN_VariationIndicator")=="$$$",None).otherwise(sf.col("csSSN_VariationIndicator")).alias("VariationIndicator"), \
                sf.when(sf.col("csSSN_VariationIndicator_code")=="$$$",None).otherwise(sf.col("csSSN_VariationIndicator_code")).alias("VariationIndicator_code")) \
                .where(sf.col("Number").isNotNull() | \
                sf.col("VariationIndicator").isNotNull() | \
                sf.col("VariationIndicator_code").isNotNull() \
                )

dfcsSSN.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)