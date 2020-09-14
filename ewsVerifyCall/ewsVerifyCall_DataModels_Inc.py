# %%spark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import *
from pyspark import SQLContext
from datetime import date,datetime,timedelta
from pytz import timezone
from pyspark.sql.functions import udf
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

appNameSuffix = "ewsVerifyCallSpark_DataModels"

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

prefix_rmi = "gold/fdm/risk/risk_model_input/"
rmi_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rmi).split("/")[:-3]) + "/*/*/*"

prefix_rms = "gold/fdm/risk/risk_model_score/"
rms_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rms).split("/")[:-3]) + "/*/*/*"

dfrmi = sparkSession.read.format("parquet").load(rmi_path)
dfrms = sparkSession.read.format("parquet").load(rms_path)

dfrmsNS1 = dfrms.where(sf.col("shadow_record").isNull())

dfrmsNS2 = dfrms.where(sf.col("shadow_record") == 0)

dfrmsNS = dfrmsNS1.union(dfrmsNS2)

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskrun/"
tgtfilePathHeader = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/header/"
tgtfilePathDataLookup = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/datalookup/"
tgtfilePathResult = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/result/"

dfParquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfParquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

dfjoin = dfbaseData.join(dfrmi,(dfbaseData.loanApplicationId == dfrmi.loan_application_id) & \
                                    (sf.unix_timestamp(dfbaseData.createdDatePT) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    .select(dfbaseData.id \
                    ,dfbaseData.mvpApplicantId \
                    ,dfbaseData.loanApplicationId \
                    ,dfbaseData.mvpLoanApplicationId \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("ewsVerifyCallTimestampUTC") \
                    ,dfbaseData.createdDatePT.alias("ewsVerifyCallTimestampPT") \
                    ,dfrmi.id.alias("rmiId") \
                    ,dfrmi.date_created.alias("riskTimestampPT") \
                    ,dfrmsNS.score_type.alias("scoreType") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","ewsVerifyCallTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                .where(sf.col("row_num") == 1) \
                .select(dfjoin["*"])

dfsplitColHeader = dfbaseData.select(sf.col("id")
                                    ,sf.col("application")
                                    ,sf.col("asid")
                                    ,sf.col("replyTo")
                                    ,sf.col("sgid")
                                    ,sf.col("teid")
                                    ,sf.col("timestamp")
                                    ,sf.col("tsoid")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColDataLookup = dfbaseData.select(sf.col("id")
                                    ,sf.col("callingPartyLineType")
                                    ,sf.col("callingPartyStatus")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColResult = dfbaseData.select(sf.col("id")
                                    ,sf.col("action")
                                    # ,sf.col("dataLookup")
                                    ,sf.col("ewDeviceId")
                                    ,sf.col("status_statusCode").alias("statusCode")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))


dfrankedId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathId)

dfsplitColHeader.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathHeader)
                   
dfsplitColDataLookup.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathDataLookup)

dfsplitColResult.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathResult)