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
appNameSuffix = vendor + "_DataModels_Stage_CC3"

year = date.split('-',1)[0]
month = date.split('-',2)[1]
day = date.split('-',3)[2]

# If you run in pyspark, ignore sc =SparkContext(). Else if you run via spark-submit, uncomment this.
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
                .config("spark.sql.shuffle.partitions","1")
                .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                .getOrCreate())

client = boto3.client('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-west-2")

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/creditorContact_2/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/creditorContact_3/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfcreditorContact = df.select(sf.col("id"),sf.col("createdDate"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`07500_[a-zA-Z0-9_]*`"))

# dfcreditorContact.printSchema()

dfcreditorContactR1 = reduce(lambda dfcreditorContact, idx: dfcreditorContact.withColumnRenamed(list(dfcreditorContact.schema.names)[idx],
                                                 list(dfcreditorContact.schema.names)[idx].replace("_110_","_5_")),
            range(len(list(dfcreditorContact.schema.names))),
            dfcreditorContact)

dfcreditorContactR2 = reduce(lambda dfcreditorContactR1, idx: dfcreditorContactR1.withColumnRenamed(list(dfcreditorContactR1.schema.names)[idx],
                                                 list(dfcreditorContactR1.schema.names)[idx].replace("_111_","_6_")),
            range(len(list(dfcreditorContactR1.schema.names))),
            dfcreditorContactR1)

dfcreditorContactR3 = reduce(lambda dfcreditorContactR2, idx: dfcreditorContactR2.withColumnRenamed(list(dfcreditorContactR2.schema.names)[idx],
                                                 list(dfcreditorContactR2.schema.names)[idx].replace("_112_","_7_")),
            range(len(list(dfcreditorContactR2.schema.names))),
            dfcreditorContactR2)

dfcreditorContactR4 = reduce(lambda dfcreditorContactR3, idx: dfcreditorContactR3.withColumnRenamed(list(dfcreditorContactR3.schema.names)[idx],
                                                 list(dfcreditorContactR3.schema.names)[idx].replace("_210_","_8_")),
            range(len(list(dfcreditorContactR3.schema.names))),
            dfcreditorContactR3)

dfcreditorContactR5 = reduce(lambda dfcreditorContactR4, idx: dfcreditorContactR4.withColumnRenamed(list(dfcreditorContactR4.schema.names)[idx],
                                                 list(dfcreditorContactR4.schema.names)[idx].replace("_211_","_9_")),
            range(len(list(dfcreditorContactR4.schema.names))),
            dfcreditorContactR4)

# dfcreditorContactR5.printSchema()

# dfcreditorContactR5.show(1, False)

# dfcreditorContactR5.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePath)

dfcreditorContactR5.write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)