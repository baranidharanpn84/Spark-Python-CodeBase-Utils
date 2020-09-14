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

concat_udf = sf.udf(lambda cols: "~".join([x if x is not None else "" for x in cols]), StringType())

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

srcfilePathTrans = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/transactions/year=" + year + "/month=" + month + "/day=" + day +""
srcfilePathSummary = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/summary/year=" + year + "/month=" + month + "/day=" + day +""
srcfilePathStreams = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/streams/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathTrans = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/transactions/"
tgtfilePathSummary = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/summary/"
tgtfilePathStreams = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/streams/"
tgtfilePathMapTbl = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/mappingTable/"

dfparquetSrcTrans = sparkSession.read.format("parquet").load(srcfilePathTrans)
dfparquetSrcSummary = sparkSession.read.format("parquet").load(srcfilePathSummary)
dfparquetSrcStreams = sparkSession.read.format("parquet").load(srcfilePathStreams)

dfTrans = dfparquetSrcTrans.withColumn("year",sf.split("createdDate","\-")[0]) \
                .withColumn("month",sf.split("createdDate","\-")[1]) \
                .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfSummary = dfparquetSrcSummary.withColumn("year",sf.split("createdDate","\-")[0]) \
                .withColumn("month",sf.split("createdDate","\-")[1]) \
                .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfStreams = dfparquetSrcStreams.withColumn("year",sf.split("createdDate","\-")[0]) \
                .withColumn("month",sf.split("createdDate","\-")[1]) \
                .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseDataTrans = dfTrans.select([col for col in dfTrans.columns])
dfbaseDataSummary = dfSummary.select([col for col in dfSummary.columns])
dfbaseDataStreams = dfStreams.select([col for col in dfStreams.columns if not col.startswith("streams_transactions_")])
dfbaseDataMapTblInt = dfStreams.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("streams_id"),dfStreams.colRegex("`streams_transactions_[0-9_]+_id`"))
dfbaseDataMapTblStr = dfStreams.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("streams_id"),dfStreams.colRegex("`streams_transactions_[a-zA-Z_]*`"))

dfbaseDataMapTblInt_1 = dfbaseDataMapTblInt.withColumn("streams_transactions_id", concat_udf(sf.sort_array(sf.array([col for col in dfbaseDataMapTblInt.columns if col.startswith("streams_transactions_")]))))

dfbaseDataMapTblInt_2 = dfbaseDataMapTblInt_1.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("streams_id"),sf.col("streams_transactions_id"))

dfbaseDataMapTblInt_2 = dfbaseDataMapTblInt_2.withColumn("transactions_id",sf.explode(sf.split(sf.trim(sf.regexp_replace("streams_transactions_id","~"," "))," ")))
                                    # .withColumn("replace",sf.trim(sf.regexp_replace("streams_transactions_id","~"," "))) \

# dfbaseDataMapTblInt_2.show(10,False)

dfbaseDataTransFinal = dfbaseDataTrans.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("transactions_id").alias("tran_transactions_id"),sf.col("transactions_value").alias("tran_transactions_value"),sf.col("transactions_date").alias("tran_transactions_date")).distinct()

dfbaseDataSummaryFinal = dfbaseDataSummary.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("summary_end_date"),sf.col("summary_num_transactions"),sf.col("summary_start_date"),sf.col("summary_total_irregular_income"),sf.col("summary_total_regular_income"))

# dfbaseDataStreamsFinal = dfbaseDataStreams.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("applicantId"),sf.col("applicationSource") \
                                   # ,sf.col("clientID"),sf.col("createdDate"),sf.col("loanApplicationId"),sf.col("mvpApplicantId") \
                                   # ,sf.col("noHit"),sf.col("successful"),sf.col("timestamp"),sf.col("updatedAt"),sf.col("createdDatePT") \
                                   # ,sf.col("transactions_id").alias("tran_transactions_id"),sf.col("transactions_value").alias("tran_transactions_value"),sf.col("transactions_date").alias("tran_transactions_date")).distinct()

dfbaseDataMapTblInt_3 = dfbaseDataMapTblInt_2.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("streams_id").alias("mptbl_streams_id"),sf.col("transactions_id").alias("mptbl_transactions_id"))

dfbaseDataMapTblStr_1 = dfbaseDataMapTblStr.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("streams_id").alias("mptbl_streams_id"),sf.col("streams_transactions_id").alias("mptbl_transactions_id"))

dfbaseDataMapTblFinal = dfbaseDataMapTblInt_3.union(dfbaseDataMapTblStr_1)

dfbaseDataTransFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathTrans)

dfbaseDataSummaryFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathSummary)

dfbaseDataStreams.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathStreams)

dfbaseDataMapTblFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathMapTbl)