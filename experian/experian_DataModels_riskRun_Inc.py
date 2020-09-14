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

# def concat_udf(*cols):
    # return sf.concat(*[sf.coalesce(c, sf.lit("")) for c in cols])

concat_udf = sf.udf(lambda cols: "".join([x if x is not None else "" for x in cols]), StringType())

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
appNameSuffix = vendor + "_DataModels_riskRun"

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
srcfilePathRM = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskModel/*/*/*"
srcfilePathPS = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/profileSummary/*/*/*"

tgtfilePathRR = "s3://" + bucket + "/enriched/experian/DataModels/riskrun/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)
dfparquetRM = sparkSession.read.format("parquet").load(srcfilePathRM)
dfparquetPS = sparkSession.read.format("parquet").load(srcfilePathPS)

dfparquetRM_VM = dfparquetRM.where(sf.col("ModelIndicator_code") == "Q ")
dfparquetRM_VM3 = dfparquetRM.where(sf.col("ModelIndicator_code") == "V3")

# dfparquetRM_VMFinal = dfparquetRM_VM.select(dfparquetRM_VM.id.alias("id_vm"),sf.coalesce(dfparquetRM_VM.Score.cast(IntegerType()),sf.lit(0)).alias("score_vm"))
# dfparquetRM_VM3Final = dfparquetRM_VM3.select(dfparquetRM_VM3.id.alias("id_vm3"),sf.coalesce(dfparquetRM_VM3.Score.cast(IntegerType()),sf.lit(0)).alias("score_vm3"))
# dfparquetPSFinal = dfparquetPS.select(dfparquetPS.id.alias("id_ps"),sf.coalesce(dfparquetPS.csProfileSummary_NowDelinquentDerog.cast(IntegerType()),sf.lit(0)).alias("NowDelinquentDerog"))

dfparquetRM_VMFinal = dfparquetRM_VM.select(dfparquetRM_VM.id.alias("id_vm"),dfparquetRM_VM.Score.cast(IntegerType()).alias("score_vm"))
dfparquetRM_VM3Final = dfparquetRM_VM3.select(dfparquetRM_VM3.id.alias("id_vm3"),dfparquetRM_VM3.Score.cast(IntegerType()).alias("score_vm3"))
dfparquetPSFinal = dfparquetPS.select(dfparquetPS.id.alias("id_ps"),dfparquetPS.csProfileSummary_NowDelinquentDerog.cast(IntegerType()).alias("NowDelinquentDerog"))

#dfparquetPSFinal = dfparquetPSFinal.withColumn("newNowDelinquentDerog",sf.when(sf.col("NowDelinquentDerog").isNull(),0).otherwise(sf.col("NowDelinquentDerog")))

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

dfbaseDataVMs = dfbaseData.join(dfparquetRM_VMFinal,(dfbaseData.id == dfparquetRM_VMFinal.id_vm),'left_outer') \
                    .join(dfparquetRM_VM3Final,(dfbaseData.id == dfparquetRM_VM3Final.id_vm3) & \
                        (dfparquetRM_VMFinal.id_vm == dfparquetRM_VM3Final.id_vm3),'left_outer') \
                    .join(dfparquetPSFinal,(dfbaseData.id == dfparquetPSFinal.id_ps) & \
                        (dfparquetRM_VMFinal.id_vm == dfparquetPSFinal.id_ps),'left_outer') \
                    .select(dfbaseData.id.alias("id") \
                    ,dfbaseData.creditProfileId.alias("credit_profile_id") \
                    ,dfbaseData.clientID.alias("client_id") \
                    ,dfbaseData.mvpClientID.alias("mvp_client_id") \
                    ,dfbaseData.mvpApplicantId.alias("mvp_applicant_id") \
                    ,dfbaseData.loanApplicationId.alias("loan_application_id") \
                    ,dfbaseData.mvpLoanApplicationId.alias("mvp_loan_application_id") \
                    ,dfbaseData.pullCreditType.alias("pull_credit_type") \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("date_created_utc") \
                    ,dfbaseData.createdDatePT.alias("date_created_pt") \
                    ,sf.when(dfparquetRM_VMFinal.score_vm.isNull(),0).otherwise(dfparquetRM_VMFinal.score_vm).alias("vm_score") \
                    ,sf.when(dfparquetRM_VM3Final.score_vm3.isNull(),0).otherwise(dfparquetRM_VM3Final.score_vm3).alias("vm3_score") \
                    ,sf.when(dfparquetPSFinal.NowDelinquentDerog.isNull(),0).otherwise(dfparquetPSFinal.NowDelinquentDerog).alias("NowDelinquentDerog") \
                    # ,sf.when(dfparquetPSFinal.NowDelinquentDerog == None, 0).otherwise(dfparquetPSFinal.NowDelinquentDerog).alias("NowDelinquentDerogTest") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

dfjoinFinal = dfbaseDataVMs.join(dfrmi,(dfbaseDataVMs.loan_application_id == dfrmi.loan_application_id) & \
                                    ((dfbaseDataVMs.vm_score == sf.coalesce(dfrmi.risk_model_vantage_input,sf.lit(0))) & \
                                    (dfbaseDataVMs.vm3_score == sf.coalesce(dfrmi.risk_model_vantage3_input,sf.lit(0))) & \
                                    (dfbaseDataVMs.NowDelinquentDerog == dfrmi.now_delinquent_derog_input)),'left_outer') \
                    .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    .select(dfbaseDataVMs.id \
                    ,dfbaseDataVMs.credit_profile_id \
                    ,dfbaseDataVMs.client_id \
                    ,dfbaseDataVMs.mvp_client_id \
                    ,dfbaseDataVMs.mvp_applicant_id \
                    ,dfbaseDataVMs.loan_application_id \
                    ,dfbaseDataVMs.mvp_loan_application_id \
                    ,dfbaseDataVMs.pull_credit_type \
                    ,dfbaseDataVMs.date_created_utc \
                    ,dfbaseDataVMs.date_created_pt \
                    ,dfbaseDataVMs.vm_score \
                    ,dfbaseDataVMs.vm3_score \
                    ,dfbaseDataVMs.NowDelinquentDerog \
                    ,dfrmi.id.alias("rmi_id") \
                    ,dfrmi.date_created.alias("risk_timestamp_pt") \
                    ,dfrmsNS.score_type.alias("score_type") \
                    ,dfbaseDataVMs.year \
                    ,dfbaseDataVMs.month \
                    ,dfbaseDataVMs.day)

dfjoinFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRR)