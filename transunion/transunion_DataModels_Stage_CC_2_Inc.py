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
appNameSuffix = vendor + "_DataModels_Stage_CC2"

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

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/creditorContact_1/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/creditorContact_2/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfcreditorContact = df.select(sf.col("id"),sf.col("createdDate"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`07500_[a-zA-Z0-9_]*`"))

# dfcreditorContact.printSchema()

dfcreditorContactRC1 = reduce(lambda dfcreditorContact, idx: dfcreditorContact.withColumnRenamed(list(dfcreditorContact.schema.names)[idx],
                                                 list(dfcreditorContact.schema.names)[idx].replace("07500_1decodeData","07500_1_decodeData")),
            range(len(list(dfcreditorContact.schema.names))),
            dfcreditorContact)
dfcreditorContactRC2 = reduce(lambda dfcreditorContactRC1, idx: dfcreditorContactRC1.withColumnRenamed(list(dfcreditorContactRC1.schema.names)[idx],
                                                 list(dfcreditorContactRC1.schema.names)[idx].replace("07500_1subscriber_","07500_1_subscriber_")),
            range(len(list(dfcreditorContactRC1.schema.names))),
            dfcreditorContactRC1)

dfcreditorContactRC3 = reduce(lambda dfcreditorContactRC2, idx: dfcreditorContactRC2.withColumnRenamed(list(dfcreditorContactRC2.schema.names)[idx],
                                                 list(dfcreditorContactRC2.schema.names)[idx].replace("07500_2decodeData","07500_2_decodeData")),
            range(len(list(dfcreditorContactRC2.schema.names))),
            dfcreditorContactRC2)
dfcreditorContactR = reduce(lambda dfcreditorContactRC3, idx: dfcreditorContactRC3.withColumnRenamed(list(dfcreditorContactRC3.schema.names)[idx],
                                                 list(dfcreditorContactRC3.schema.names)[idx].replace("07500_2subscriber_","07500_2_subscriber_")),
            range(len(list(dfcreditorContactRC3.schema.names))),
            dfcreditorContactRC3)

# dfcreditorContactR.printSchema()

# dfcreditorContactR.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePath)

dfcreditorContactR.write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)