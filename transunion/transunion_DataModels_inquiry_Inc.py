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

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfccInquiryInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`ccInquiry_[0-9]+_\w*`"))

dfccInquiryStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`ccInquiry_[a-zA-Z_]*`"))

#dfccInquiryStr.show()

InquiryCnt = sorted([int(col.split("_")[1]) for col in dfccInquiryInt.columns if col.split("_")[0] == "ccInquiry" \
                        and col.split("_")[2] in ["ECOADesignator","accountType","date_content","date_estimatedCentury","date_estimatedDay","date_estimatedMonth","date_estimatedYear" \
                        ,"subscriber_industryCode","subscriber_inquirySubscriberPrefixCode","subscriber_memberCode","subscriber_name_unparsed"]])[-1]

#print(InquiryCnt)

for col in dfccInquiryInt.columns:
    instr = col.find('_', 11, 14)
    for i in range(InquiryCnt):
        rule = "ccInquiry_"+str(i+1)
        if not "ccInquiry_"+str(i+1)+"_"+col[instr+1:] in dfccInquiryInt.columns:
            dfccInquiryInt = dfccInquiryInt.withColumn("ccInquiry_"+str(i+1)+"_"+col[instr+1:], sf.lit(None))

for i in range(InquiryCnt):
    rule = "ccInquiry_"+str(i+1)
    dfccInquiryInt = dfccInquiryInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_ECOADesignator"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_accountType"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_date_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_date_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_date_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_date_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_date_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_industryCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_inquirySubscriberPrefixCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_memberCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_name_unparsed"),sf.lit("$$$")), sf.lit("^")
                            ) \
                            )

#dfccInquiryInt.show(2,False)

dfccInquiryInt = dfccInquiryInt.withColumn( "ccInquiryArray", sf.array([col for col in dfccInquiryInt.columns if col.split("_")[0] == "newccInquiry"]) )

dfexplode = dfccInquiryInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("ccInquiryArray").alias("ccInquiry") \
            )

dfsplitCol = dfexplode.withColumn("ccInquiry_ECOADesignator",sf.split("ccInquiry","\^")[0]) \
         .withColumn("ccInquiry_accountType",sf.split("ccInquiry","\^")[1]) \
         .withColumn("ccInquiry_date_content",sf.split("ccInquiry","\^")[2]) \
         .withColumn("ccInquiry_date_estimatedCentury",sf.split("ccInquiry","\^")[3]) \
         .withColumn("ccInquiry_date_estimatedDay",sf.split("ccInquiry","\^")[4]) \
         .withColumn("ccInquiry_date_estimatedMonth",sf.split("ccInquiry","\^")[5]) \
         .withColumn("ccInquiry_date_estimatedYear",sf.split("ccInquiry","\^")[6]) \
         .withColumn("ccInquiry_subscriber_industryCode",sf.split("ccInquiry","\^")[7]) \
         .withColumn("ccInquiry_subscriber_inquirySubscriberPrefixCode",sf.split("ccInquiry","\^")[8]) \
         .withColumn("ccInquiry_subscriber_memberCode",sf.split("ccInquiry","\^")[9]) \
         .withColumn("ccInquiry_subscriber_name_unparsed",sf.split("ccInquiry","\^")[10]) \
         .drop("ccInquiry")

#dfsplitCol.show(10, False)

# dfccInquiryStr = dfccInquiryStr.withColumn("ccInquiry_accountType", sf.lit(None).cast(StringType()))

dfccInquiryStrCol = dfccInquiryStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("ccInquiry_ECOADesignator")
                            ,sf.col("ccInquiry_accountType")
                            ,sf.col("ccInquiry_date_content")
                            ,sf.col("ccInquiry_date_estimatedCentury")
                            ,sf.col("ccInquiry_date_estimatedDay")
                            ,sf.col("ccInquiry_date_estimatedMonth")
                            ,sf.col("ccInquiry_date_estimatedYear")
                            ,sf.col("ccInquiry_subscriber_industryCode")
                            ,sf.col("ccInquiry_subscriber_inquirySubscriberPrefixCode")
                            ,sf.col("ccInquiry_subscriber_memberCode")
                            ,sf.col("ccInquiry_subscriber_name_unparsed"))

dfsplitColFinal = dfsplitCol.union(dfccInquiryStrCol)

#dfsplitColFinal.show(1, False)

dfccInquiry = dfsplitColFinal.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("ccInquiry_ECOADesignator")=="$$$",None).otherwise(sf.col("ccInquiry_ECOADesignator")).alias("ECOADesignator"), \
                sf.when(sf.col("ccInquiry_accountType")=="$$$",None).otherwise(sf.col("ccInquiry_accountType")).alias("accountType"), \
                sf.when(sf.col("ccInquiry_date_content")=="$$$",None).otherwise(sf.col("ccInquiry_date_content")).alias("date_content"), \
                sf.when(sf.col("ccInquiry_date_estimatedCentury")=="$$$",None).otherwise(sf.col("ccInquiry_date_estimatedCentury")).alias("date_estimatedCentury"), \
                sf.when(sf.col("ccInquiry_date_estimatedDay")=="$$$",None).otherwise(sf.col("ccInquiry_date_estimatedDay")).alias("date_estimatedDay"), \
                sf.when(sf.col("ccInquiry_date_estimatedMonth")=="$$$",None).otherwise(sf.col("ccInquiry_date_estimatedMonth")).alias("date_estimatedMonth"), \
                sf.when(sf.col("ccInquiry_date_estimatedYear")=="$$$",None).otherwise(sf.col("ccInquiry_date_estimatedYear")).alias("date_estimatedYear"), \
                sf.when(sf.col("ccInquiry_subscriber_industryCode")=="$$$",None).otherwise(sf.col("ccInquiry_subscriber_industryCode")).alias("subscriber_industryCode"), \
                sf.when(sf.col("ccInquiry_subscriber_inquirySubscriberPrefixCode")=="$$$",None).otherwise(sf.col("ccInquiry_subscriber_inquirySubscriberPrefixCode")).alias("subscriber_inquirySubscriberPrefixCode"), \
                sf.when(sf.col("ccInquiry_subscriber_memberCode")=="$$$",None).otherwise(sf.col("ccInquiry_subscriber_memberCode")).alias("subscriber_memberCode"), \
                sf.when(sf.col("ccInquiry_subscriber_name_unparsed")=="$$$",None).otherwise(sf.col("ccInquiry_subscriber_name_unparsed")).alias("subscriber_name_unparsed")) \
                .where(sf.col("ECOADesignator").isNotNull() | \
                sf.col("accountType").isNotNull() | \
                sf.col("date_content").isNotNull() | \
                sf.col("date_estimatedCentury").isNotNull() | \
                sf.col("date_estimatedDay").isNotNull() | \
                sf.col("date_estimatedMonth").isNotNull() | \
                sf.col("date_estimatedYear").isNotNull() | \
                sf.col("subscriber_industryCode").isNotNull() | \
                sf.col("subscriber_inquirySubscriberPrefixCode").isNotNull() | \
                sf.col("subscriber_memberCode").isNotNull() | \
                sf.col("subscriber_name_unparsed").isNotNull())

dfccInquiry.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)