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
appNameSuffix = vendor + "_DataModels_infoMsg"

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
srcfilePathAllCol = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/*/*/*"

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/infoMsg/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)
dfparquetAllCol = sparkSession.read.format("parquet").load(srcfilePathAllCol)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfcsInfoMsg = df.select(df.colRegex("`csInfoMsg_[a-zA-Z0-9]+_\w*`"))

# dfcsInfoMsg.printSchema()

dfcsInfoMsgInt = df.select(df.colRegex("`csInfoMsg_[0-9]+_\w*`"))

dfcsInfoMsgStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csInfoMsg_[a-zA-Z_]*`"))
dfcsInfoMsgStrAllCol = dfparquetAllCol.select(dfparquetAllCol.colRegex("`csInfoMsg_[a-zA-Z_]*`"))

#dfcsInfoMsgStr.show()

InfoMsgCnt = sorted([int(col.split("_")[1]) for col in dfcsInfoMsgInt.columns if col.split("_")[0] == "csInfoMsg" \
                        and col.split("_")[2] in ["MessageNumber","MessageText"]])[-1]

#print(InfoMsgCnt)

for col in dfcsInfoMsgStrAllCol.columns:
    if not col in dfcsInfoMsgStr.columns:
        dfcsInfoMsgStr = dfcsInfoMsgStr.withColumn(col, sf.lit(None))

for i in range(InfoMsgCnt):
    rule = "csInfoMsg_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_MessageNumber"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_MessageText"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#df.show(2,False)

df = df.withColumn( "csInfoMsgArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsInfoMsg"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csInfoMsgArray").alias("csInfoMsg") \
            )

dfsplitCol = dfexplode.withColumn("csInfoMsg_MessageNumber",sf.split("csInfoMsg","\^")[0]) \
                     .withColumn("csInfoMsg_MessageText",sf.split("csInfoMsg","\^")[1]) \
                     .drop("csInfoMsg")

#dfsplitCol.show(10, False)

# dfcsInfoMsgStr = dfcsInfoMsgStr.withColumn('csInfoMsg_MessageNumber', sf.lit(None).cast(StringType())) \
                            # .withColumn('csInfoMsg_MessageText', sf.lit(None).cast(StringType())) \

dfcsInfoMsgStrCol = dfcsInfoMsgStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csInfoMsg_MessageNumber")
                            ,sf.col("csInfoMsg_MessageText")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsInfoMsgStrCol)

#dfsplitColFinal.show(1, False)

dfcsInfoMsg = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csInfoMsg_MessageNumber")=="$$$",None).otherwise(sf.col("csInfoMsg_MessageNumber")).alias("MessageNumber"), \
                sf.when(sf.col("csInfoMsg_MessageText")=="$$$",None).otherwise(sf.col("csInfoMsg_MessageText")).alias("MessageText")) \
                .where(sf.col("MessageNumber").isNotNull() | \
                sf.col("MessageText").isNotNull() \
                )

dfcsInfoMsg.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)