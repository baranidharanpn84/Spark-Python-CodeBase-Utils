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

#If you run in pyspark, ignore sc = SparkContext(). Else if you run via spark-submit, uncomment this.
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

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-west-2")

prefix_rmi = "gold/fdm/risk/risk_model_input/"
rmi_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rmi).split("/")[:-3]) + "/*/*/*"

prefix_rms = "gold/fdm/risk/risk_model_score/"
rms_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rms).split("/")[:-3]) + "/*/*/*"

prefix_app = "gold/fdm/loan_application/applicant/"
app_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_app).split("/")[:-1]) + "/"

dfrmi = sparkSession.read.format("parquet").load(rmi_path)
dfrms = sparkSession.read.format("parquet").load(rms_path)
dfapplicant = sparkSession.read.format("parquet").load(app_path)

dfrmsNS1 = dfrms.where(sf.col("shadow_record").isNull())

dfrmsNS2 = dfrms.where(sf.col("shadow_record") == 0)

dfrmsNS = dfrmsNS1.union(dfrmsNS2)

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathRules = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/rules/"
tgtfilePathDevice = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/device/"
tgtfilePathRiskRun = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskrun/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

dfparquetApp = dfparquet.join(dfapplicant, dfparquet.applicantId == dfapplicant.applicant_id, 'left_outer') \
            .select(dfparquet["*"],dfapplicant["loan_application_id"])

df = dfparquetApp.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

df_device = df.select([col for col in df.columns if not col.startswith("rule_")])

ruleCnt = sorted([int(col.split("_")[1]) for col in df.columns if col.split("_")[0] == "rule" and col.split("_")[2] in ["reason","score","type"]])[-1]

for i in range(ruleCnt+1):
    rule = "rule_"+str(i)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_reason"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_score"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_type"),sf.lit("$$$")) \
                            ) \
                            )

df = df.withColumn( "ruleArray", sf.array([col for col in df.columns if col.split("_")[0] == "newrule"]) )

dfexplode = df.select(sf.col("id"),sf.col("applicantId"),sf.col("loan_application_id"),sf.col("createdDatePT"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("ruleArray").alias("rule") \
            )

dfsplitCol = dfexplode.withColumn("rule_reason",sf.split("rule","\^")[0]) \
         .withColumn("rule_score",sf.split("rule","\^")[1]) \
         .withColumn("rule_type",sf.split("rule","\^")[2]) 

df_rules = dfsplitCol.select(sf.col("id"),sf.col("applicantId"),sf.col("loan_application_id"),sf.col("createdDatePT"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("rule_reason")=="$$$",None).otherwise(sf.col("rule_reason")).alias("rule_reason"), \
                sf.when(sf.col("rule_score")=="$$$",None).otherwise(sf.col("rule_score")).cast("integer").alias("rule_score"), \
                sf.when(sf.col("rule_type")=="$$$",None).otherwise(sf.col("rule_type")).alias("rule_type")) \
                .where(sf.col("rule_reason").isNotNull() | sf.col("rule_score").isNotNull() | sf.col("rule_type").isNotNull())

df_device.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathDevice)

df_rules.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRules)

dfjoin = df_device.join(dfrmi,(df_device.loan_application_id == dfrmi.loan_application_id) & \
                                    (sf.unix_timestamp(df_device.createdDatePT) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    .select(df_device.id, \
                    df_device.applicantId, \
                    # df_device.mvpApplicantId, \
                    df_device.loan_application_id.alias("loanApplicationId"), \
                    sf.regexp_replace(df_device.createdDate,"T"," ").cast(TimestampType()).alias("iovationTimestampUTC"), \
                    df_device.createdDatePT.alias("iovationTimestampPT"), \
                    df_device.device_bb_timestamp,\
                    dfrmi.id.alias("rmiId"), \
                    dfrmi.date_created.alias("riskTimestampPT"), \
                    dfrmsNS.score_type.alias("scoreType"),\
                    dfrmi.risk_model_scorex_plus_input,\
                    dfrmi.risk_model_vantage_input,\
                    dfrmi.risk_model_vantage3_input,\
                    df_device.year,\
                    df_device.month,\
                    df_device.day)

dfranked =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","iovationTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                .where(sf.col("row_num") == 1) \
                .select(dfjoin["*"])

dfranked.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRiskRun)