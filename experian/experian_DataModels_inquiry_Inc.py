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
appNameSuffix = vendor + "_DataModels_inquiry"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/inquiry/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData.show(10,False)

dfcsInquiry = df.select(df.colRegex("`csInquiry_[a-zA-Z0-9]+_\w*`"))

# dfcsInquiry.printSchema()

dfcsInquiryInt = df.select(df.colRegex("`csInquiry_[0-9]+_\w*`"))

dfcsInquiryStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csInquiry_[a-zA-Z_]*`"))

#dfcsInquiryStr.show()

InquiryCnt = sorted([int(col.split("_")[1]) for col in dfcsInquiryInt.columns if col.split("_")[0] == "csInquiry" \
                        and col.split("_")[2] in ["AccountNumber","Amount","Date","KOB_code","subcode","SubscriberDisplayName" \
                                            ,"Terms","Terms_code","Type","Type_code"]])[-1]

#print(InquiryCnt)

for i in range(InquiryCnt):
    rule = "csInquiry_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            # sf.coalesce(sf.col(rule+"_AccountNumber"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_Amount"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_Date"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_KOB"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_KOB_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Subcode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SubscriberDisplayName"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Terms"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Terms_code"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Type_code"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#df.show(2,False)

df = df.withColumn( "csInquiryArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsInquiry"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csInquiryArray").alias("csInquiry") \
            )

dfsplitCol = dfexplode.withColumn("csInquiry_AccountNumber",sf.lit(None).cast(StringType())) \
                     .withColumn("csInquiry_Amount",sf.split("csInquiry","\^")[0]) \
                     .withColumn("csInquiry_Date",sf.split("csInquiry","\^")[1]) \
                     .withColumn("csInquiry_KOB",sf.lit(None).cast(StringType())) \
                     .withColumn("csInquiry_KOB_code",sf.split("csInquiry","\^")[2]) \
                     .withColumn("csInquiry_Subcode",sf.split("csInquiry","\^")[3]) \
                     .withColumn("csInquiry_SubscriberDisplayName",sf.split("csInquiry","\^")[4]) \
                     .withColumn("csInquiry_Terms",sf.lit(None).cast(StringType())) \
                     .withColumn("csInquiry_Terms_code",sf.split("csInquiry","\^")[5]) \
                     .withColumn("csInquiry_Type",sf.lit(None).cast(StringType())) \
                     .withColumn("csInquiry_Type_code",sf.split("csInquiry","\^")[6]) \
                     .drop("csInquiry")

#dfsplitCol.show(10, False)

dfcsInquiryStr = dfcsInquiryStr.withColumn('csInquiry_AccountNumber', sf.lit(None).cast(StringType())) \
                            .withColumn('csInquiry_KOB', sf.lit(None).cast(StringType())) \
                            .withColumn('csInquiry_Terms', sf.lit(None).cast(StringType())) \
                            .withColumn('csInquiry_Type', sf.lit(None).cast(StringType())) \

dfcsInquiryStrCol = dfcsInquiryStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csInquiry_AccountNumber")
                            ,sf.col("csInquiry_Amount")
                            ,sf.col("csInquiry_Date")
                            ,sf.col("csInquiry_KOB")
                            ,sf.col("csInquiry_KOB_code")
                            ,sf.col("csInquiry_Subcode")
                            ,sf.col("csInquiry_SubscriberDisplayName")
                            ,sf.col("csInquiry_Terms")
                            ,sf.col("csInquiry_Terms_code")
                            ,sf.col("csInquiry_Type")
                            ,sf.col("csInquiry_Type_code")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsInquiryStrCol)

#dfsplitColFinal.show(1, False)

dfcsInquiry = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csInquiry_AccountNumber")=="$$$",None).otherwise(sf.col("csInquiry_AccountNumber")).alias("AccountNumber"), \
                sf.when(sf.col("csInquiry_Amount")=="$$$",None).otherwise(sf.col("csInquiry_Amount")).alias("Amount"), \
                sf.when(sf.col("csInquiry_Date")=="$$$",None).otherwise(sf.col("csInquiry_Date")).alias("Date"), \
                sf.when(sf.col("csInquiry_KOB")=="$$$",None).otherwise(sf.col("csInquiry_KOB")).alias("KOB"), \
                sf.when(sf.col("csInquiry_KOB_code")=="$$$",None).otherwise(sf.col("csInquiry_KOB_code")).alias("KOB_code"), \
                sf.when(sf.col("csInquiry_Subcode")=="$$$",None).otherwise(sf.col("csInquiry_Subcode")).alias("Subcode"), \
                sf.when(sf.col("csInquiry_SubscriberDisplayName")=="$$$",None).otherwise(sf.col("csInquiry_SubscriberDisplayName")).alias("SubscriberDisplayName"), \
                sf.when(sf.col("csInquiry_Terms")=="$$$",None).otherwise(sf.col("csInquiry_Terms")).alias("Terms"), \
                sf.when(sf.col("csInquiry_Terms_code")=="$$$",None).otherwise(sf.col("csInquiry_Terms_code")).alias("Terms_code"), \
                sf.when(sf.col("csInquiry_Type")=="$$$",None).otherwise(sf.col("csInquiry_Type")).alias("Type"), \
                sf.when(sf.col("csInquiry_Type_code")=="$$$",None).otherwise(sf.col("csInquiry_Type_code")).alias("Type_code")) \
                .where(sf.col("AccountNumber").isNotNull() | \
                sf.col("Amount").isNotNull() | \
                sf.col("Date").isNotNull() | \
                sf.col("KOB").isNotNull() | \
                sf.col("KOB_code").isNotNull() | \
                sf.col("Subcode").isNotNull() | \
                sf.col("SubscriberDisplayName").isNotNull() | \
                sf.col("Terms").isNotNull() | \
                sf.col("Terms_code").isNotNull() | \
                sf.col("Type").isNotNull() | \
                sf.col("Type_code").isNotNull() \
                )

dfcsInquiry.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)