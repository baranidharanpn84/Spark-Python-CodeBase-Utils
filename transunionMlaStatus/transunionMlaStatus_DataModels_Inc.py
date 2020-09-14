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

def blank_as_null(x):
    return when(sf.col(x) != "", sf.col(x)).otherwise(None)

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

# prefix_rmi = "gold/fdm/risk/risk_model_input/"
# rmi_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rmi).split("/")[:-3]) + "/*/*/*"

# prefix_rms = "gold/fdm/risk/risk_model_score/"
# rms_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rms).split("/")[:-3]) + "/*/*/*"

# dfrmi = sparkSession.read.format("parquet").load(rmi_path)
# dfrms = sparkSession.read.format("parquet").load(rms_path)

# dfrmsNS1 = dfrms.where(sf.col("shadow_record").isNull())

# dfrmsNS2 = dfrms.where(sf.col("shadow_record") == 0)

# dfrmsNS = dfrmsNS1.union(dfrmsNS2)

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/id/"
# tgtfilePathRR = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskrun/"
tgtfilePathSS = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/searchStatus/"

# dfapplicant = sparkSession.read.format("parquet").load(applicantfilePath)

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

# dfparquet = dfparquetSrc.join(dfapplicant, dfparquetSrc.mvpApplicantId == dfapplicant.applicant_id, 'left_outer') \
                # .select(dfparquetSrc["*"],dfapplicant["loan_application_id"].alias("app_loanApplicationId"))

df = dfparquetSrc.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0]) \
          .withColumn("srcNewID",sf.coalesce(blank_as_null("applicantId"),sf.coalesce(blank_as_null("clientID"),blank_as_null("loanApplicationId")), sf.lit("^"))) \

# dfriskNewID = dfrmi.withColumn("riskNewID", sf.coalesce(sf.col("applicant_id"),sf.coalesce(sf.col("client_id"),sf.col("loan_application_id")), sf.lit("^") ))

dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfbaseDataId = dfbaseData.select(dfbaseData.id.alias("id") \
                    # ,dfbaseData.creditProfileId.alias("credit_profile_id") \
                    ,dfbaseData.clientID.alias("client_id") \
                    ,dfbaseData.mvpClientID.alias("mvp_client_id") \
                    ,dfbaseData.mvpApplicantId.alias("mvp_applicant_id") \
                    ,dfbaseData.loanApplicationId.alias("loan_application_id") \
                    ,dfbaseData.mvpLoanApplicationId.alias("mvp_loan_application_id") \
                    # ,dfbaseData.pullCreditType.alias("pull_credit_type") \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("date_created_utc") \
                    ,dfbaseData.createdDatePT.alias("date_created_pt") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

# dfjoinRR = dfbaseData.join(dfriskNewID,(dfriskNewID.riskNewID == dfbaseData.srcNewID) & \
                    # (dfriskNewID.application_source == dfbaseData.applicationSource),'left_outer') \
                    # .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    # .select(dfbaseData.id \
                    # ,dfbaseData.creditProfileId \
                    # ,dfbaseData.clientID.alias("client_id") \
                    # ,dfbaseData.mvpClientID.alias("mvp_client_id") \
                    # ,dfbaseData.applicantId.alias("applicant_id") \
                    # ,dfbaseData.mvpApplicantId.alias("mvp_applicant_id") \
                    # ,dfbaseData.loanApplicationId.alias("loan_application_id") \
                    # ,dfbaseData.mvpLoanApplicationId .alias("mvp_loan_application_id")\
                    # ,dfbaseData.pullCreditType.alias("pull_credit_type") \
                    # ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("date_created_utc") \
                    # ,dfbaseData.createdDatePT.alias("date_created_pt") \
                    # ,dfrmi.id.alias("rmi_id") \
                    # ,dfrmsNS.score_type.alias("score_type") \
                    # ,dfrmi.date_created.alias("risk_timestamp_pt") \
                    # ,dfbaseData.year \
                    # ,dfbaseData.month \
                    # ,dfbaseData.day)

dfmiliLendAct = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`07051_[a-zA-Z0-9_]*`"))

dfmiliLendAct = dfmiliLendAct.withColumn("searchStatus", concat_udf(sf.array([col for col in dfmiliLendAct.columns if col.endswith("searchStatus")])))

dfsplitColMLA = dfmiliLendAct.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("searchStatus"))

# dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","transunionMlaStatusTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                    # .where(sf.col("row_num") == 1) \
                    # .select(dfjoin["*"])

dfbaseDataId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathId)

# dfjoinRR.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathRR)

dfsplitColMLA.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathSS)