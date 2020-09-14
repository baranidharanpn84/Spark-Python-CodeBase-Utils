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
appNameSuffix = vendor + "Spark_JSON_Parquet"

year = date.split('-',1)[0]
month = date.split('-',2)[1]
day = date.split('-',3)[2]

#If you run in pyspark , ignore  sc =SparkContext(). ELse if you run via spark-submit, uncomment this .
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

tgtfilePathid = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskrun/"
tgtfilePathProduct1320 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/Product1320/"
tgtfilePathProduct1325 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/Product1325/"
tgtfilePathProduct1335 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/Product1335/"
tgtfilePathProduct3260 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/Product3260/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

dfjoin = dfbaseData.join(dfrmi,(dfbaseData.loanApplicationId == dfrmi.loan_application_id) & \
                                    (sf.unix_timestamp(dfbaseData.createdDatePT) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    .select(dfbaseData.id \
                    ,dfbaseData.mvpApplicantId \
                    ,dfbaseData.loanApplicationId \
                    # ,dfbaseData.mvpLoanApplicationId \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("neustarTimestampUTC") \
                    ,dfbaseData.createdDatePT.alias("neustarTimestampPT") \
                    ,dfrmi.id.alias("rmiId") \
                    ,dfrmi.date_created.alias("riskTimestampPT") \
                    ,dfrmsNS.score_type.alias("scoreType") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","neustarTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                .where(sf.col("row_num") == 1) \
                .select(dfjoin["*"])

dfsplitColP1320 = df.select([col for col in df.columns if col.startswith("1320_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dfsplitColP1325 = df.select([col for col in df.columns if col.startswith("1325_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dfsplitColP1335 = df.select([col for col in df.columns if col.startswith("1335_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dfsplitColP3260 = df.select([col for col in df.columns if col.startswith("3260_")
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day")])

dfrankedId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathid)

dfsplitColP1320.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathProduct1320)
                   
dfsplitColP1325.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathProduct1325)

dfsplitColP1335.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathProduct1335)

dfsplitColP3260.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathProduct3260)