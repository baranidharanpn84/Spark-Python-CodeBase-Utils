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
appNameSuffix = vendor + "_DataModels"

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
# tgtfilePathRR = "s3://" + bucket + "/enriched/experian/DataModels/riskrun/"
tgtfilePathCARefAddr = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/caRefAddr/"
tgtfilePathCustAttr = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/customAttributes"
tgtfilePathError = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/error/"
# tgtfilePathFraudServ = "s3://" + bucket + "/enriched/experian/DataModels/fraudServices/"
tgtfilePathHeader = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/header/"
tgtfilePathPreAttr = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/premierAttributes/"
tgtfilePathPS = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/profileSummary/"
# tgtfilePathPRec = "s3://" + bucket + "/enriched/experian/DataModels/publicRecord/"
# tgtfilePathSSN = "s3://" + bucket + "/enriched/experian/DataModels/SSN/"
# tgtfilePathStatement = "s3://" + bucket + "/enriched/experian/DataModels/statement/"

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquetSrc.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

# dfjoin = dfbaseData.join(dfrmi,(dfbaseData.loanApplicationId == dfrmi.loan_application_id) & \
                                    # (sf.unix_timestamp(regexp_replace(dfbaseData.createdDatePT,"T"," ")) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    # .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    # .select(dfbaseData.id.alias("id") \
                    # ,dfbaseData.creditProfileId.alias("credit_profile_id") \
                    # ,dfbaseData.clientID.alias("client_id") \
                    # ,dfbaseData.mvpClientID.alias("mvp_client_id") \
                    # ,dfbaseData.mvpApplicantId.alias("mvp_applicant_id") \
                    # ,dfbaseData.loanApplicationId.alias("loan_application_id") \
                    # ,dfbaseData.mvpLoanApplicationId.alias("mvp_loan_application_id") \
                    # ,dfrmi.id.alias("rmi_id") \
                    # ,dfbaseData.pullCreditType.alias("pull_credit_type") \
                    # ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("date_created_utc") \
                    # ,dfbaseData.createdDatePT.alias("date_created_pt") \
                    # ,dfrmi.date_created.alias("risk_date_created_pt") \
                    # ,dfrmsNS.score_type.alias("score_type") \
                    # ,dfbaseData.year \
                    # ,dfbaseData.month \
                    # ,dfbaseData.day)

dfbaseDataId = dfbaseData.select(dfbaseData.id.alias("id") \
                    ,dfbaseData.creditProfileId.alias("credit_profile_id") \
                    ,dfbaseData.clientID.alias("client_id") \
                    ,dfbaseData.mvpClientID.alias("mvp_client_id") \
                    ,dfbaseData.mvpApplicantId.alias("mvp_applicant_id") \
                    ,dfbaseData.loanApplicationId.alias("loan_application_id") \
                    ,dfbaseData.mvpLoanApplicationId.alias("mvp_loan_application_id") \
                    ,dfbaseData.pullCreditType.alias("pull_credit_type") \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("date_created_utc") \
                    ,dfbaseData.createdDatePT.alias("date_created_pt") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

dfCARefAddr = df.select([col for col in df.columns if col.startswith("csCARefAddr_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])

# dfcustAttr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            # ,df.colRegex("`csMA_Attribute_[a-zA-Z_]*`"))

dfcustAttr = df.select([col for col in df.columns if col.startswith("csMA_Attribute_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])
                    
dferror = df.select([col for col in df.columns if col.startswith("csError_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])

# dffraudServ = df.select([col for col in df.columns if col.startswith("csFraudServices_") \
                    # or col.startswith("id")
                    # or col.startswith("year")
                    # or col.startswith("month")
                    # or col.startswith("day") ])

dfheader = df.select([col for col in df.columns if col.startswith("csHeader_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])

dfpremierAttr = df.select([col for col in df.columns if col.startswith("PremierAttribute_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])

dfprofileSumm = df.select([col for col in df.columns if col.startswith("csProfileSummary_") \
                    or col.startswith("id")
                    or col.startswith("year")
                    or col.startswith("month")
                    or col.startswith("day") ])

# dfpublicRecord = df.select([col for col in df.columns if col.startswith("csPublicRecord_") \
                    # or col.startswith("id")
                    # or col.startswith("year")
                    # or col.startswith("month")
                    # or col.startswith("day") ])

# dfSSN = df.select([col for col in df.columns if col.startswith("csSSN_") \
                    # or col.startswith("id")
                    # or col.startswith("year")
                    # or col.startswith("month")
                    # or col.startswith("day") ])

# dfstatement = df.select([col for col in df.columns if col.startswith("csStatement_") \
                    # or col.startswith("id")
                    # or col.startswith("year")
                    # or col.startswith("month")
                    # or col.startswith("day") ])

# dffraudServ = dffraudServ.withColumn("AddressCount", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("AddressCount")]))) \
                       # .withColumn("AddressDate", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("AddressDate")]))) \
                       # .withColumn("AddressErrorCode_code", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("AddressErrorCode_code")]))) \
                       # .withColumn("DateOfBirth", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("DateOfBirth")]))) \
                       # .withColumn("DateOfDeath", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("DateOfDeath")]))) \
                       # .withColumn("Indicator_1", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_1")]))) \
                       # .withColumn("Indicator_2", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_2")]))) \
                       # .withColumn("Indicator_3", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_3")]))) \
                       # .withColumn("Indicator_4", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_4")]))) \
                       # .withColumn("Indicator_5", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_5")]))) \
                       # .withColumn("Indicator_6", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_6")]))) \
                       # .withColumn("Indicator_7", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_7")]))) \
                       # .withColumn("Indicator_8", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_8")]))) \
                       # .withColumn("Indicator_9", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_9")]))) \
                       # .withColumn("Indicator_10", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_10")]))) \
                       # .withColumn("Indicator_11", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_11")]))) \
                       # .withColumn("Indicator_12", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Indicator_12")]))) \
                       # .withColumn("SIC_code", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SIC_code")]))) \
                       # .withColumn("SSNFirstPossibleIssuanceYear", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SSNFirstPossibleIssuanceYear")]))) \
                       # .withColumn("SSNLastPossibleIssuanceYear", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SSNLastPossibleIssuanceYear")]))) \
                       # .withColumn("SocialCount", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SocialCount")]))) \
                       # .withColumn("SocialDate", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SocialDate")]))) \
                       # .withColumn("SocialErrorCode_code", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("SocialErrorCode_code")]))) \
                       # .withColumn("Text", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Text")]))) \
                       # .withColumn("Type_code", concat_udf(sf.array([col for col in dffraudServ.columns if col.endswith("Type_code")])))

# dfpublicRecord = dfpublicRecord.withColumn("Amount", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Amount")]))) \
                       # .withColumn("Bankruptcy_AdjustmentPercent", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_AdjustmentPercent")]))) \
                       # .withColumn("Bankruptcy_AssetAmount", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_AssetAmount")]))) \
                       # .withColumn("Bankruptcy_LiabilitiesAmount", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_LiabilitiesAmount")]))) \
                       # .withColumn("Bankruptcy_RepaymentPercent", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_RepaymentPercent")]))) \
                       # .withColumn("Bankruptcy_Type", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_Type")]))) \
                       # .withColumn("Bankruptcy_Type_code", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Bankruptcy_Type_code")]))) \
                       # .withColumn("BookPageSequence", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("BookPageSequence")]))) \
                       # .withColumn("ConsumerComment", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("ConsumerComment")]))) \
                       # .withColumn("Court", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Court")]))) \
                       # .withColumn("Court_code", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Court_code")]))) \
                       # .withColumn("Court_name", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Court_name")]))) \
                       # .withColumn("DisputeFlag", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("DisputeFlag")]))) \
                       # .withColumn("ECOA", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("ECOA")]))) \
                       # .withColumn("ECOA_code", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("ECOA_code")]))) \
                       # .withColumn("Evaluation", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Evaluation")]))) \
                       # .withColumn("Evaluation_code", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Evaluation_code")]))) \
                       # .withColumn("FilingDate", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("FilingDate")]))) \
                       # .withColumn("PlaintiffName", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("PlaintiffName")]))) \
                       # .withColumn("ReferenceNumber", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("ReferenceNumber")]))) \
                       # .withColumn("Status", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Status")]))) \
                       # .withColumn("StatusDate", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("StatusDate")]))) \
                       # .withColumn("Status_code", concat_udf(sf.array([col for col in dfpublicRecord.columns if col.endswith("Status_code")])))

# dfSSN = dfSSN.withColumn("Number", concat_udf(sf.array([col for col in dfSSN.columns if col.endswith("Number")]))) \
                       # .withColumn("VariationIndicator", concat_udf(sf.array([col for col in dfSSN.columns if col.endswith("VariationIndicator")]))) \
                       # .withColumn("VariationIndicator_code", concat_udf(sf.array([col for col in dfSSN.columns if col.endswith("VariationIndicator_code")])))

# dfstatement = dfstatement.withColumn("DateReported", concat_udf(sf.array([col for col in dfstatement.columns if col.endswith("DateReported")]))) \
                       # .withColumn("StatementText_MessageText", concat_udf(sf.array([col for col in dfstatement.columns if col.endswith("StatementText_MessageText")]))) \
                       # .withColumn("Type", concat_udf(sf.array([col for col in dfstatement.columns if col.endswith("Type")]))) \
                       # .withColumn("Type_code", concat_udf(sf.array([col for col in dfstatement.columns if col.endswith("Type_code")])))


# dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loan_application_id","date_created_pt").orderBy(sf.desc("risk_date_created_pt"),sf.desc("score_type")))) \
                    # .where(sf.col("row_num") == 1) \
                    # .select(dfjoin["*"])

# dfsplitColPubRec = dfpublicRecord.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("Amount"),sf.col("Bankruptcy_AdjustmentPercent")
                                    # ,sf.col("Bankruptcy_AssetAmount"),sf.col("Bankruptcy_LiabilitiesAmount"),sf.col("Bankruptcy_RepaymentPercent"),sf.col("Bankruptcy_Type")
                                    # ,sf.col("Bankruptcy_Type_code"),sf.col("BookPageSequence"),sf.col("ConsumerComment"),sf.col("Court"),sf.col("Court_code"),sf.col("Court_name")
                                    # ,sf.col("DisputeFlag"),sf.col("ECOA"),sf.col("ECOA_code"),sf.col("Evaluation"),sf.col("Evaluation_code"),sf.col("FilingDate"),sf.col("PlaintiffName")
                                    # ,sf.col("ReferenceNumber"),sf.col("Status"),sf.col("StatusDate"),sf.col("Status_code"))

# dfsplitColFraudServ = dffraudServ.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("AddressCount"),sf.col("AddressDate"),sf.col("AddressErrorCode_code")
                                    # ,sf.col("DateOfBirth"),sf.col("DateOfDeath")
                                    # ,sf.concat(
                                    # sf.coalesce(sf.col("Indicator_1"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_2"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_3"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_4"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_5"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_6"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_7"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_8"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_9"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_10"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_11"),sf.lit("null")), sf.lit(":"),\
                                    # sf.coalesce(sf.col("Indicator_12"),sf.lit("null")), sf.lit(":")
                                    # ).alias("Indicator")
                                    # ,sf.col("SIC_code"),sf.col("SSNFirstPossibleIssuanceYear"),sf.col("SSNLastPossibleIssuanceYear"),sf.col("SocialCount"),sf.col("SocialDate")
                                    # ,sf.col("SocialErrorCode_code"),sf.col("Text"),sf.col("Type_code"))

# dfsplitColSSN = dfSSN.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("Number"),sf.col("VariationIndicator"),sf.col("VariationIndicator_code"))

# dfsplitColStmt = dfstatement.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("DateReported"),sf.col("StatementText_MessageText"),sf.col("Type")
                                    # ,sf.col("Type_code"))

# dfrankedId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathRR)

dfbaseDataId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathId)

dfCARefAddr.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathCARefAddr)

dfcustAttr.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathCustAttr)

dferror.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathError)

# dfsplitColFraudServ.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathFraudServ)

dfheader.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathHeader)

dfpremierAttr.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathPreAttr)

dfprofileSumm.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathPS)

# dfsplitColPubRec.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathPRec)

# dfsplitColSSN.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathSSN)

# dfsplitColStmt.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathStatement)