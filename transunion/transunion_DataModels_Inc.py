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

def blank_as_null(x):
    return when(sf.col(x) != "", sf.col(x)).otherwise(None)

concat_udf = sf.udf(lambda cols: "".join([x if x is not None else "" for x in cols]), StringType())
concat_udf_vm = sf.udf(lambda cols: "|".join([x if x is not None else "" for x in cols]), StringType())

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

# tgtfilePathRR = "s3://" + bucket + "/enriched/transunion/DataModels/riskrun/"
# tgtfilePathCC = "s3://" + bucket + "/enriched/transunion/DataModels/creditorContact/"
tgtfilePathId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/id/"
tgtfilePathCreditSumm = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/creditSummary/"
# tgtfilePathHRFA = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/highRiskFraudAlert/"
tgtfilePathMLA = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/militaryLendAct/"
tgtfilePathSM = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/creditVisionAttributesV1/"
tgtfilePathCVAV2 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/creditVisionAttributesV2/"
tgtfilePathSC = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/standardCharacteristics/"
# tgtfilePathSF = "s3://" + bucket + "/enriched/transunion/DataModels/syntheticFraud/"
tgtfilePathVM = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/vantageModel/"
tgtfilePathVM3 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/vantageModel3/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

# dfjoin = dfbaseData.join(dfrmi,(dfbaseData.loanApplicationId == dfrmi.loan_application_id) & \
                                    # (sf.unix_timestamp(regexp_replace(dfbaseData.createdDatePT,"T"," ")) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    # .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    # .select(dfbaseData.id \
                    # ,dfbaseData.creditProfileId \
                    # ,dfbaseData.clientID \
                    # ,dfbaseData.mvpClientID \
                    # ,dfbaseData.mvpApplicantId \
                    # ,dfbaseData.loanApplicationId \
                    # ,dfbaseData.mvpLoanApplicationId \
                    # ,dfrmi.id.alias("rmiId") \
                    # ,dfbaseData.pullCreditType \
                    # ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("transunionTimestampUTC") \
                    # ,dfbaseData.createdDatePT.alias("transunionTimestampPT") \
                    # ,dfrmi.date_created.alias("riskTimestampPT") \
                    # ,dfrmsNS.score_type.alias("scoreType") \
                    # ,dfbaseData.year \
                    # ,dfbaseData.month \
                    # ,dfbaseData.day)

# dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","transunionTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                    # .where(sf.col("row_num") == 1) \
                    # .select(dfjoin["*"])

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

# dfcreditorContact = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`07500_[a-zA-Z0-9_]*`"))

dfcreditSummary = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`ccCreditSummary_[a-zA-Z0-9_]*`"))

# dfhRFA = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`06500_[a-zA-Z0-9_]*`"))

dfmiliLendAct = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`07051_[a-zA-Z0-9_]*`"))

dfscoreModel = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`characteristic_00V71_[a-zA-Z0-9_]*`"))

dfcreditVisionAttrV2 = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`characteristic_00WR3_[a-zA-Z0-9_]*`"))

dfstdChar = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`characteristic_00N05_[a-zA-Z0-9_]*`"))

# dfsynFraud = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`00WB1_[a-zA-Z0-9_]*`"))

dfvantageModel = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`00P94_[a-zA-Z0-9_]*`"))

dfvantageModel3 = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`00V60_[a-zA-Z0-9_]*`"))


# dfcreditorContact = dfcreditorContact.withColumn("decodeData", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("decodeData")]))) \
                       # .withColumn("subscriber_address_location_city", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_address_location_city")]))) \
                       # .withColumn("subscriber_address_location_state", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_address_location_state")]))) \
                       # .withColumn("subscriber_address_location_zipCode", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_address_location_zipCode")]))) \
                       # .withColumn("subscriber_address_street_unparsed", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_address_street_unparsed")]))) \
                       # .withColumn("subscriber_industryCode", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_industryCode")]))) \
                       # .withColumn("subscriber_inquirySubscriberPrefixCode", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_inquirySubscriberPrefixCode")]))) \
                       # .withColumn("subscriber_memberCode", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_memberCode")]))) \
                       # .withColumn("subscriber_name_unparsed", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_name_unparsed")]))) \
                       # .withColumn("subscriber_phone_number_areaCode", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_number_areaCode")]))) \
                       # .withColumn("subscriber_phone_number_exchange", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_number_exchange")]))) \
                       # .withColumn("subscriber_phone_number_qualifier", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_number_qualifier")]))) \
                       # .withColumn("subscriber_phone_number_suffix", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_number_suffix")]))) \
                       # .withColumn("subscriber_phone_number_type", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_number_type")]))) \
                       # .withColumn("subscriber_phone_source", concat_udf(sf.array([col for col in dfcreditorContact.columns if col.endswith("subscriber_phone_source")])))

# dfhRFA = dfhRFA.withColumn("decease_alertMessageCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_alertMessageCode")]))) \
                       # .withColumn("decease_dateOfBirth_content", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_content")]))) \
                       # .withColumn("decease_dateOfBirth_estimatedCentury", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedCentury")]))) \
                       # .withColumn("decease_dateOfBirth_estimatedDay", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedDay")]))) \
                       # .withColumn("decease_dateOfBirth_estimatedMonth", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedMonth")]))) \
                       # .withColumn("decease_dateOfBirth_estimatedYear", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedYear")]))) \
                       # .withColumn("decease_dateOfDeath_content", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_content")]))) \
                       # .withColumn("decease_dateOfDeath_estimatedCentury", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedCentury")]))) \
                       # .withColumn("decease_dateOfDeath_estimatedDay", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedDay")]))) \
                       # .withColumn("decease_dateOfDeath_estimatedMonth", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedMonth")]))) \
                       # .withColumn("decease_dateOfDeath_estimatedYear", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedYear")]))) \
                       # .withColumn("decease_lastResidency_location_city", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_city")]))) \
                       # .withColumn("decease_lastResidency_location_state", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_state")]))) \
                       # .withColumn("decease_lastResidency_location_zipCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_zipCode")]))) \
                       # .withColumn("decease_locationOfPayments_location_city", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_city")]))) \
                       # .withColumn("decease_locationOfPayments_location_state", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_state")]))) \
                       # .withColumn("decease_locationOfPayments_location_zipCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_zipCode")]))) \
                       # .withColumn("decease_name_person_first", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_name_person_first")]))) \
                       # .withColumn("decease_name_person_last", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_name_person_last")]))) \
                       # .withColumn("decease_source", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_source")]))) \
                       # .withColumn("message_code_0", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_code")]))) \
                       # .withColumn("message_code_1", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_code")]))) \
                       # .withColumn("message_code_2", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_code")]))) \
                       # .withColumn("message_code_3", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_code")]))) \
                       # .withColumn("message_code_4", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_code")]))) \
                       # .withColumn("message_code_5", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_code")]))) \
                       # .withColumn("message_code_6", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_code")]))) \
                       # .withColumn("message_code_7", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_code")]))) \
                       # .withColumn("message_code_8", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_code")]))) \
                       # .withColumn("message_code_9", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_code")]))) \
                       # .withColumn("message_code_10", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_code")]))) \
                       # .withColumn("message_code_11", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_code")]))) \
                       # .withColumn("message_code_12", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_code")]))) \
                       # .withColumn("message_code_13", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_code")]))) \
                       # .withColumn("message_code_14", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_code")]))) \
                       # .withColumn("message_code_15", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_code")]))) \
                       # .withColumn("message_code_16", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_code")]))) \
                       # .withColumn("message_code_17", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_code")]))) \
                       # .withColumn("message_code_18", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_code")]))) \
                       # .withColumn("message_code_19", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_code")]))) \
                       # .withColumn("message_code_20", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_code")]))) \
                       # .withColumn("message_code_21", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_code")]))) \
                       # .withColumn("message_code_22", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_code")]))) \
                       # .withColumn("message_code_23", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_code")]))) \
                       # .withColumn("message_code_24", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_code")]))) \
                       # .withColumn("message_code_25", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_code")]))) \
                       # .withColumn("message_code_26", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_code")]))) \
                       # .withColumn("message_code_27", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_code")]))) \
                       # .withColumn("message_code_28", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_code")]))) \
                       # .withColumn("message_code_29", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_code")]))) \
                       # .withColumn("message_code_30", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_code")]))) \
                       # .withColumn("message_code_31", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_31_code")]))) \
                       # .withColumn("message_code_32", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_32_code")]))) \
                       # .withColumn("message_code_33", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_33_code")]))) \
                       # .withColumn("message_code_34", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_34_code")]))) \
                       # .withColumn("message_code_35", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_35_code")]))) \
                       # .withColumn("message_code_36", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_36_code")]))) \
                       # .withColumn("message_code_37", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_37_code")]))) \
                       # .withColumn("message_code_38", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_38_code")]))) \
                       # .withColumn("message_code_39", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_39_code")]))) \
                       # .withColumn("message_text", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_text")]))) \
                       # .withColumn("message_text_1", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_text")]))) \
                       # .withColumn("message_text_2", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_text")]))) \
                       # .withColumn("message_text_3", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_text")]))) \
                       # .withColumn("message_text_4", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_text")]))) \
                       # .withColumn("message_text_5", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_text")]))) \
                       # .withColumn("message_text_6", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_text")]))) \
                       # .withColumn("message_text_7", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_text")]))) \
                       # .withColumn("message_text_8", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_text")]))) \
                       # .withColumn("message_text_9", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_text")]))) \
                       # .withColumn("message_text_10", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_text")]))) \
                       # .withColumn("message_text_11", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_text")]))) \
                       # .withColumn("message_text_12", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_text")]))) \
                       # .withColumn("message_text_13", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_text")]))) \
                       # .withColumn("message_text_14", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_text")]))) \
                       # .withColumn("message_text_15", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_text")]))) \
                       # .withColumn("message_text_16", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_text")]))) \
                       # .withColumn("message_text_17", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_text")]))) \
                       # .withColumn("message_text_18", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_text")]))) \
                       # .withColumn("message_text_19", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_text")]))) \
                       # .withColumn("message_text_20", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_text")]))) \
                       # .withColumn("message_text_21", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_text")]))) \
                       # .withColumn("message_text_22", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_text")]))) \
                       # .withColumn("message_text_23", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_text")]))) \
                       # .withColumn("message_text_24", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_text")]))) \
                       # .withColumn("message_text_25", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_text")]))) \
                       # .withColumn("message_text_26", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_text")]))) \
                       # .withColumn("message_text_27", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_text")]))) \
                       # .withColumn("message_text_28", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_text")]))) \
                       # .withColumn("message_text_29", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_text")]))) \
                       # .withColumn("message_text_30", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_text")]))) \
                       # .withColumn("message_text_31", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_31_text")]))) \
                       # .withColumn("message_text_32", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_32_text")]))) \
                       # .withColumn("message_text_33", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_33_text")]))) \
                       # .withColumn("message_text_34", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_34_text")]))) \
                       # .withColumn("message_text_35", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_35_text")]))) \
                       # .withColumn("message_text_36", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_36_text")]))) \
                       # .withColumn("message_text_37", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_37_text")]))) \
                       # .withColumn("message_text_38", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_38_text")]))) \
                       # .withColumn("message_text_39", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_39_text")]))) \
                           # .withColumn("message_text_40", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_40_text")]))) \
                           # .withColumn("message_text_41", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_41_text")]))) \
                           # .withColumn("message_text_42", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_42_text")]))) \
                           # .withColumn("message_text_43", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_43_text")]))) \
                           # .withColumn("message_text_44", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_44_text")]))) \
                           # .withColumn("message_text_45", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_45_text")]))) \
                       # .withColumn("message_custom_1_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_custom_addressMatch")]))) \
                       # .withColumn("message_custom_2_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_custom_addressMatch")]))) \
                       # .withColumn("message_custom_3_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_custom_addressMatch")]))) \
                       # .withColumn("message_custom_4_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_custom_addressMatch")]))) \
                       # .withColumn("message_custom_5_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_custom_addressMatch")]))) \
                       # .withColumn("message_custom_6_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_custom_addressMatch")]))) \
                       # .withColumn("message_custom_7_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_custom_addressMatch")]))) \
                       # .withColumn("message_custom_8_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_custom_addressMatch")]))) \
                       # .withColumn("message_custom_9_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_custom_addressMatch")]))) \
                       # .withColumn("message_custom_10_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_custom_addressMatch")]))) \
                       # .withColumn("message_custom_11_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_custom_addressMatch")]))) \
                       # .withColumn("message_custom_12_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_custom_addressMatch")]))) \
                       # .withColumn("message_custom_13_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_custom_addressMatch")]))) \
                       # .withColumn("message_custom_14_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_custom_addressMatch")]))) \
                       # .withColumn("message_custom_15_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_custom_addressMatch")]))) \
                       # .withColumn("message_custom_16_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_custom_addressMatch")]))) \
                       # .withColumn("message_custom_17_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_custom_addressMatch")]))) \
                       # .withColumn("message_custom_18_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_custom_addressMatch")]))) \
                       # .withColumn("message_custom_19_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_custom_addressMatch")]))) \
                       # .withColumn("message_custom_20_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_custom_addressMatch")]))) \
                       # .withColumn("message_custom_21_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_custom_addressMatch")]))) \
                       # .withColumn("message_custom_22_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_custom_addressMatch")]))) \
                       # .withColumn("message_custom_23_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_custom_addressMatch")]))) \
                       # .withColumn("message_custom_24_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_custom_addressMatch")]))) \
                       # .withColumn("message_custom_25_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_custom_addressMatch")]))) \
                       # .withColumn("message_custom_26_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_custom_addressMatch")]))) \
                       # .withColumn("message_custom_27_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_custom_addressMatch")]))) \
                       # .withColumn("message_custom_28_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_custom_addressMatch")]))) \
                       # .withColumn("message_custom_29_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_custom_addressMatch")]))) \
                       # .withColumn("message_custom_30_addressMatch", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_custom_addressMatch")]))) \
                       # .withColumn("message_custom_1_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_custom_unitNumber")]))) \
                       # .withColumn("message_custom_2_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_custom_unitNumber")]))) \
                       # .withColumn("message_custom_3_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_custom_unitNumber")]))) \
                       # .withColumn("message_custom_4_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_custom_unitNumber")]))) \
                       # .withColumn("message_custom_5_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_custom_unitNumber")]))) \
                       # .withColumn("message_custom_6_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_custom_unitNumber")]))) \
                       # .withColumn("message_custom_7_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_custom_unitNumber")]))) \
                       # .withColumn("message_custom_8_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_custom_unitNumber")]))) \
                       # .withColumn("message_custom_9_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_custom_unitNumber")]))) \
                       # .withColumn("message_custom_10_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_custom_unitNumber")]))) \
                       # .withColumn("message_custom_11_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_custom_unitNumber")]))) \
                       # .withColumn("message_custom_12_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_custom_unitNumber")]))) \
                       # .withColumn("message_custom_13_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_custom_unitNumber")]))) \
                       # .withColumn("message_custom_14_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_custom_unitNumber")]))) \
                       # .withColumn("message_custom_15_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_custom_unitNumber")]))) \
                       # .withColumn("message_custom_16_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_custom_unitNumber")]))) \
                       # .withColumn("message_custom_17_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_custom_unitNumber")]))) \
                       # .withColumn("message_custom_18_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_custom_unitNumber")]))) \
                       # .withColumn("message_custom_19_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_custom_unitNumber")]))) \
                       # .withColumn("message_custom_20_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_custom_unitNumber")]))) \
                       # .withColumn("message_custom_21_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_custom_unitNumber")]))) \
                       # .withColumn("message_custom_22_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_custom_unitNumber")]))) \
                       # .withColumn("message_custom_23_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_custom_unitNumber")]))) \
                       # .withColumn("message_custom_24_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_custom_unitNumber")]))) \
                       # .withColumn("message_custom_25_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_custom_unitNumber")]))) \
                       # .withColumn("message_custom_26_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_custom_unitNumber")]))) \
                       # .withColumn("message_custom_27_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_custom_unitNumber")]))) \
                       # .withColumn("message_custom_28_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_custom_unitNumber")]))) \
                       # .withColumn("message_custom_29_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_custom_unitNumber")]))) \
                       # .withColumn("message_custom_30_unitNumber", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_custom_unitNumber")]))) \

# dfhRFA = dfhRFA.withColumn("newCustText", \
                        # sf.concat(
                            # sf.coalesce(blank_as_null("message_text"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_1"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_2"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_3"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_4"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_5"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_6"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_7"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_8"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_9"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_10"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_11"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_12"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_13"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_14"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_15"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_16"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_17"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_18"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_19"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_20"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_21"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_22"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_23"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_24"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_25"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_26"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_27"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_28"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_29"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_30"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_31"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_32"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_33"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_34"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_35"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_36"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_37"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_38"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            # sf.coalesce(blank_as_null("message_text_39"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_40"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_41"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_42"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_43"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_44"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                                # sf.coalesce(blank_as_null("message_text_45"),sf.lit("ZzZzZ")), sf.lit("YyYyY")\
                            # ) \
                            # )

# dfhRFA = dfhRFA.withColumn("newCustTextFormat",sf.regexp_replace(sf.col("newCustText"),"ZzZzZYyYyY",""))

# dfhRFA = dfhRFA.withColumn("newCustTextFormatFinal",sf.regexp_replace(sf.col("newCustTextFormat"),"YyYyY","^"))

# dfhRFA = dfhRFA.withColumn("message_text",sf.split("newCustTextFormatFinal","\^")[0]) \
                # .withColumn("message_text_1",sf.split("newCustTextFormatFinal","\^")[1]) \
                # .withColumn("message_text_2",sf.split("newCustTextFormatFinal","\^")[2]) \
                # .withColumn("message_text_3",sf.split("newCustTextFormatFinal","\^")[3]) \
                # .withColumn("message_text_4",sf.split("newCustTextFormatFinal","\^")[4]) \
                # .withColumn("message_text_5",sf.split("newCustTextFormatFinal","\^")[5]) \
                # .withColumn("message_text_6",sf.split("newCustTextFormatFinal","\^")[6]) \
                # .withColumn("message_text_7",sf.split("newCustTextFormatFinal","\^")[7]) \
                # .withColumn("message_text_8",sf.split("newCustTextFormatFinal","\^")[8]) \
                # .withColumn("message_text_9",sf.split("newCustTextFormatFinal","\^")[9]) \
                # .withColumn("message_text_10",sf.split("newCustTextFormatFinal","\^")[10]) \

# dfsplitColHRA = dfhRFA.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("decease_alertMessageCode")
                        # ,sf.col("decease_dateOfBirth_content"),sf.col("decease_dateOfBirth_estimatedCentury"),sf.col("decease_dateOfBirth_estimatedDay")
                        # ,sf.col("decease_dateOfBirth_estimatedMonth"),sf.col("decease_dateOfBirth_estimatedYear")
                        # ,sf.col("decease_dateOfDeath_content"),sf.col("decease_dateOfDeath_estimatedCentury"),sf.col("decease_dateOfDeath_estimatedDay")
                        # ,sf.col("decease_dateOfDeath_estimatedMonth"),sf.col("decease_dateOfDeath_estimatedYear")
                        # ,sf.col("decease_lastResidency_location_city"),sf.col("decease_lastResidency_location_state"),sf.col("decease_lastResidency_location_zipCode")
                        # ,sf.col("decease_locationOfPayments_location_city"),sf.col("decease_locationOfPayments_location_state"),sf.col("decease_locationOfPayments_location_zipCode")
                        # ,sf.col("decease_name_person_first"),sf.col("decease_name_person_last"),sf.col("decease_source")
                        # ,sf.when(sf.col("message_code_1")=="",sf.col("decease_alertMessageCode")).otherwise(sf.when(sf.col("message_code_1")=="99999999","9999").otherwise(sf.col("message_code_1"))).alias("message_code")
                        # ,sf.when(sf.col("message_code_2")=="99999999","9999").otherwise(sf.col("message_code_2")).alias("message_code_1")
                        # ,sf.col("message_code_3").alias("message_code_2"),sf.col("message_code_4").alias("message_code_3"),sf.col("message_code_5").alias("message_code_4")
                        # ,sf.col("message_code_6").alias("message_code_5"),sf.col("message_code_7").alias("message_code_6"),sf.col("message_code_8").alias("message_code_7"),sf.col("message_code_9").alias("message_code_8"),sf.col("message_code_10").alias("message_code_9")
                        # ,sf.col("message_code_11").alias("message_code_10"),sf.col("message_code_12").alias("message_code_11"),sf.col("message_code_13").alias("message_code_12"),sf.col("message_code_14").alias("message_code_13"),sf.col("message_code_15").alias("message_code_14")
                        # ,sf.col("message_code_16").alias("message_code_15"),sf.col("message_code_17").alias("message_code_16"),sf.col("message_code_18").alias("message_code_17"),sf.col("message_code_19").alias("message_code_18"),sf.col("message_code_20").alias("message_code_19")
                        # ,sf.col("message_code_21").alias("message_code_20"),sf.col("message_code_22").alias("message_code_21"),sf.col("message_code_23").alias("message_code_22"),sf.col("message_code_24").alias("message_code_23"),sf.col("message_code_25").alias("message_code_24")
                        # ,sf.col("message_code_26").alias("message_code_25"),sf.col("message_code_27").alias("message_code_26"),sf.col("message_code_28").alias("message_code_27"),sf.col("message_code_29").alias("message_code_28"),sf.col("message_code_30").alias("message_code_29")
                        # ,sf.col("message_code_31").alias("message_code_30"),sf.col("message_code_32").alias("message_code_31"),sf.col("message_code_33").alias("message_code_32"),sf.col("message_code_34").alias("message_code_33"),sf.col("message_code_35").alias("message_code_34")
                        # ,sf.col("message_code_36").alias("message_code_35"),sf.col("message_code_37").alias("message_code_36"),sf.col("message_code_38").alias("message_code_37"),sf.col("message_code_39").alias("message_code_38")
                        # ,sf.col("message_custom_1_addressMatch"),sf.col("message_custom_2_addressMatch"),sf.col("message_custom_3_addressMatch"),sf.col("message_custom_4_addressMatch"),sf.col("message_custom_5_addressMatch")
                        # ,sf.col("message_custom_6_addressMatch"),sf.col("message_custom_7_addressMatch"),sf.col("message_custom_8_addressMatch"),sf.col("message_custom_9_addressMatch"),sf.col("message_custom_10_addressMatch")
                        # ,sf.col("message_custom_11_addressMatch"),sf.col("message_custom_12_addressMatch"),sf.col("message_custom_13_addressMatch"),sf.col("message_custom_14_addressMatch"),sf.col("message_custom_15_addressMatch")
                        # ,sf.col("message_custom_16_addressMatch"),sf.col("message_custom_17_addressMatch"),sf.col("message_custom_18_addressMatch"),sf.col("message_custom_19_addressMatch"),sf.col("message_custom_20_addressMatch")
                        # ,sf.col("message_custom_21_addressMatch"),sf.col("message_custom_22_addressMatch"),sf.col("message_custom_23_addressMatch"),sf.col("message_custom_24_addressMatch"),sf.col("message_custom_25_addressMatch")
                        # ,sf.col("message_custom_26_addressMatch"),sf.col("message_custom_27_addressMatch"),sf.col("message_custom_28_addressMatch"),sf.col("message_custom_29_addressMatch"),sf.col("message_custom_30_addressMatch")
                        # ,sf.col("message_custom_1_unitNumber"),sf.col("message_custom_2_unitNumber"),sf.col("message_custom_3_unitNumber"),sf.col("message_custom_4_unitNumber"),sf.col("message_custom_5_unitNumber")
                        # ,sf.col("message_custom_6_unitNumber"),sf.col("message_custom_7_unitNumber"),sf.col("message_custom_8_unitNumber"),sf.col("message_custom_9_unitNumber"),sf.col("message_custom_10_unitNumber")
                        # ,sf.col("message_custom_11_unitNumber"),sf.col("message_custom_12_unitNumber"),sf.col("message_custom_13_unitNumber"),sf.col("message_custom_14_unitNumber"),sf.col("message_custom_15_unitNumber")
                        # ,sf.col("message_custom_16_unitNumber"),sf.col("message_custom_17_unitNumber"),sf.col("message_custom_18_unitNumber"),sf.col("message_custom_19_unitNumber"),sf.col("message_custom_20_unitNumber")
                        # ,sf.col("message_custom_21_unitNumber"),sf.col("message_custom_22_unitNumber"),sf.col("message_custom_23_unitNumber"),sf.col("message_custom_24_unitNumber"),sf.col("message_custom_25_unitNumber")
                        # ,sf.col("message_custom_26_unitNumber"),sf.col("message_custom_27_unitNumber"),sf.col("message_custom_28_unitNumber"),sf.col("message_custom_29_unitNumber"),sf.col("message_custom_30_unitNumber")
                        # ,sf.when(sf.col("message_text")=="HIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZEDHIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZED","HIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZED").otherwise(sf.col("message_text")).alias("message_text")
                        # ,sf.col("message_text_1"),sf.col("message_text_2"),sf.col("message_text_3"),sf.col("message_text_4"),sf.col("message_text_5")
                        # ,sf.col("message_text_6"),sf.col("message_text_7"),sf.col("message_text_8"),sf.col("message_text_9"),sf.col("message_text_10")
                        # )

# dfsplitColHRAFinal = dfsplitColHRA.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("decease_alertMessageCode")
                        # ,sf.col("decease_dateOfBirth_content"),sf.col("decease_dateOfBirth_estimatedCentury"),sf.col("decease_dateOfBirth_estimatedDay")
                        # ,sf.col("decease_dateOfBirth_estimatedMonth"),sf.col("decease_dateOfBirth_estimatedYear")
                        # ,sf.col("decease_dateOfDeath_content"),sf.col("decease_dateOfDeath_estimatedCentury"),sf.col("decease_dateOfDeath_estimatedDay")
                        # ,sf.col("decease_dateOfDeath_estimatedMonth"),sf.col("decease_dateOfDeath_estimatedYear")
                        # ,sf.col("decease_lastResidency_location_city"),sf.col("decease_lastResidency_location_state"),sf.col("decease_lastResidency_location_zipCode")
                        # ,sf.col("decease_locationOfPayments_location_city"),sf.col("decease_locationOfPayments_location_state"),sf.col("decease_locationOfPayments_location_zipCode")
                        # ,sf.col("decease_name_person_first"),sf.col("decease_name_person_last"),sf.col("decease_source")
                        # ,sf.when(sf.col("message_code_0")=="",sf.col("message_code")).otherwise(sf.col("message_code_0")).alias("message_code")
                        # ,sf.col("message_code_1"),sf.col("message_code_2"),sf.col("message_code_3"),sf.col("message_code_4"),sf.col("message_code_5"),sf.col("message_code_6")
                        # ,sf.col("message_code_7"),sf.col("message_code_8"),sf.col("message_code_9"),sf.col("message_code_10"),sf.col("message_code_11"),sf.col("message_code_12")
                        # ,sf.col("message_code_13"),sf.col("message_code_14"),sf.col("message_code_15"),sf.col("message_code_16"),sf.col("message_code_17"),sf.col("message_code_18")
                        # ,sf.col("message_code_19"),sf.col("message_code_20"),sf.col("message_code_21"),sf.col("message_code_22"),sf.col("message_code_23"),sf.col("message_code_24")
                        # ,sf.col("message_code_25"),sf.col("message_code_26"),sf.col("message_code_27"),sf.col("message_code_28"),sf.col("message_code_29"),sf.col("message_code_30")
                        # ,sf.col("message_code_31"),sf.col("message_code_32"),sf.col("message_code_33"),sf.col("message_code_34"),sf.col("message_code_35"),sf.col("message_code_36")
                        # ,sf.col("message_code_37"),sf.col("message_code_38")
                        # ,sf.col("message_custom_1_addressMatch"),sf.col("message_custom_2_addressMatch"),sf.col("message_custom_3_addressMatch"),sf.col("message_custom_4_addressMatch"),sf.col("message_custom_5_addressMatch")
                        # ,sf.col("message_custom_6_addressMatch"),sf.col("message_custom_7_addressMatch"),sf.col("message_custom_8_addressMatch"),sf.col("message_custom_9_addressMatch"),sf.col("message_custom_10_addressMatch")
                        # ,sf.col("message_custom_11_addressMatch"),sf.col("message_custom_12_addressMatch"),sf.col("message_custom_13_addressMatch"),sf.col("message_custom_14_addressMatch"),sf.col("message_custom_15_addressMatch")
                        # ,sf.col("message_custom_16_addressMatch"),sf.col("message_custom_17_addressMatch"),sf.col("message_custom_18_addressMatch"),sf.col("message_custom_19_addressMatch"),sf.col("message_custom_20_addressMatch")
                        # ,sf.col("message_custom_21_addressMatch"),sf.col("message_custom_22_addressMatch"),sf.col("message_custom_23_addressMatch"),sf.col("message_custom_24_addressMatch"),sf.col("message_custom_25_addressMatch")
                        # ,sf.col("message_custom_26_addressMatch"),sf.col("message_custom_27_addressMatch"),sf.col("message_custom_28_addressMatch"),sf.col("message_custom_29_addressMatch"),sf.col("message_custom_30_addressMatch")
                        # ,sf.col("message_custom_1_unitNumber"),sf.col("message_custom_2_unitNumber"),sf.col("message_custom_3_unitNumber"),sf.col("message_custom_4_unitNumber"),sf.col("message_custom_5_unitNumber")
                        # ,sf.col("message_custom_6_unitNumber"),sf.col("message_custom_7_unitNumber"),sf.col("message_custom_8_unitNumber"),sf.col("message_custom_9_unitNumber"),sf.col("message_custom_10_unitNumber")
                        # ,sf.col("message_custom_11_unitNumber"),sf.col("message_custom_12_unitNumber"),sf.col("message_custom_13_unitNumber"),sf.col("message_custom_14_unitNumber"),sf.col("message_custom_15_unitNumber")
                        # ,sf.col("message_custom_16_unitNumber"),sf.col("message_custom_17_unitNumber"),sf.col("message_custom_18_unitNumber"),sf.col("message_custom_19_unitNumber"),sf.col("message_custom_20_unitNumber")
                        # ,sf.col("message_custom_21_unitNumber"),sf.col("message_custom_22_unitNumber"),sf.col("message_custom_23_unitNumber"),sf.col("message_custom_24_unitNumber"),sf.col("message_custom_25_unitNumber")
                        # ,sf.col("message_custom_26_unitNumber"),sf.col("message_custom_27_unitNumber"),sf.col("message_custom_28_unitNumber"),sf.col("message_custom_29_unitNumber"),sf.col("message_custom_30_unitNumber")
                        # ,sf.col("message_text"),sf.col("message_text_1"),sf.col("message_text_2"),sf.col("message_text_3"),sf.col("message_text_4"),sf.col("message_text_5")
                        # ,sf.col("message_text_6"),sf.col("message_text_7"),sf.col("message_text_8"),sf.col("message_text_9"),sf.col("message_text_10")
                        # )

dfmiliLendAct = dfmiliLendAct.withColumn("searchStatus", concat_udf(sf.array([col for col in dfmiliLendAct.columns if col.endswith("searchStatus")])))

dfsplitColMLA = dfmiliLendAct.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("searchStatus"))

# dfsynFraud = dfsynFraud.withColumn("derogatoryAlert", concat_udf(sf.array([col for col in dfsynFraud.columns if col.endswith("derogatoryAlert")]))) \
                       # .withColumn("fileInquiriesImpactedScore", concat_udf(sf.array([col for col in dfsynFraud.columns if col.endswith("fileInquiriesImpactedScore")]))) \
                       # .withColumn("results", concat_udf(sf.array([col for col in dfsynFraud.columns if col.endswith("results")])))

dfvantageModel = dfvantageModel.withColumn("1_derogatoryAlert", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("derogatoryAlert")]))) \
                       .withColumn("1_factor_rank_1", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("factor_1_code")]))) \
                       .withColumn("1_factor_rank_2", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("factor_2_code")]))) \
                       .withColumn("1_factor_rank_3", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("factor_3_code")]))) \
                       .withColumn("1_factor_rank_4", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("factor_4_code")]))) \
                       .withColumn("1_fileInquiriesImpactedScore", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("fileInquiriesImpactedScore")]))) \
                       .withColumn("1_noScoreReason", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("noScoreReason")]))) \
                       .withColumn("1_results", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("results")]))) \
                       .withColumn("1_scoreCard", concat_udf_vm(sf.array([col for col in dfvantageModel.columns if col.endswith("scoreCard")]))) \

dfvantageModelSplit = dfvantageModel.withColumn("derogatoryAlert",sf.when(sf.split("1_derogatoryAlert","\|")[0]=="",sf.when(sf.split("1_derogatoryAlert","\|")[1]=="",sf.split("1_derogatoryAlert","\|")[2]).otherwise(sf.split("1_derogatoryAlert","\|")[1])).otherwise(sf.split("1_derogatoryAlert","\|")[0])) \
                                    .withColumn("factor_rank_1",sf.when(sf.split("1_factor_rank_1","\|")[0]=="",sf.when(sf.split("1_factor_rank_1","\|")[1]=="",sf.split("1_factor_rank_1","\|")[2]).otherwise(sf.split("1_factor_rank_1","\|")[1])).otherwise(sf.split("1_factor_rank_1","\|")[0])) \
                                    .withColumn("factor_rank_2",sf.when(sf.split("1_factor_rank_2","\|")[0]=="",sf.when(sf.split("1_factor_rank_2","\|")[1]=="",sf.split("1_factor_rank_2","\|")[2]).otherwise(sf.split("1_factor_rank_2","\|")[1])).otherwise(sf.split("1_factor_rank_2","\|")[0])) \
                                    .withColumn("factor_rank_3",sf.when(sf.split("1_factor_rank_3","\|")[0]=="",sf.when(sf.split("1_factor_rank_3","\|")[1]=="",sf.split("1_factor_rank_3","\|")[2]).otherwise(sf.split("1_factor_rank_3","\|")[1])).otherwise(sf.split("1_factor_rank_3","\|")[0])) \
                                    .withColumn("factor_rank_4",sf.when(sf.split("1_factor_rank_4","\|")[0]=="",sf.when(sf.split("1_factor_rank_4","\|")[1]=="",sf.split("1_factor_rank_4","\|")[2]).otherwise(sf.split("1_factor_rank_4","\|")[1])).otherwise(sf.split("1_factor_rank_4","\|")[0])) \
                                    .withColumn("fileInquiriesImpactedScore",sf.when(sf.split("1_fileInquiriesImpactedScore","\|")[0]=="",sf.when(sf.split("1_fileInquiriesImpactedScore","\|")[1]=="",sf.split("1_fileInquiriesImpactedScore","\|")[2]).otherwise(sf.split("1_fileInquiriesImpactedScore","\|")[1])).otherwise(sf.split("1_fileInquiriesImpactedScore","\|")[0])) \
                                    .withColumn("noScoreReason",sf.when(sf.split("1_noScoreReason","\|")[0]=="",sf.when(sf.split("1_noScoreReason","\|")[1]=="",sf.split("1_noScoreReason","\|")[2]).otherwise(sf.split("1_noScoreReason","\|")[1])).otherwise(sf.split("1_noScoreReason","\|")[0])) \
                                    .withColumn("results",sf.when(sf.split("1_results","\|")[0]=="",sf.when(sf.split("1_results","\|")[1]=="",sf.split("1_results","\|")[2]).otherwise(sf.split("1_results","\|")[1])).otherwise(sf.split("1_results","\|")[0])) \
                                    .withColumn("scoreCard",sf.when(sf.split("1_scoreCard","\|")[0]=="",sf.when(sf.split("1_scoreCard","\|")[1]=="",sf.split("1_scoreCard","\|")[2]).otherwise(sf.split("1_scoreCard","\|")[1])).otherwise(sf.split("1_scoreCard","\|")[0])) \

dfsplitColVM = dfvantageModelSplit.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("derogatoryAlert"),sf.col("factor_rank_1"),sf.col("factor_rank_2")
                                    ,sf.col("factor_rank_3"),sf.col("factor_rank_4"),sf.col("fileInquiriesImpactedScore"),sf.col("noScoreReason")
                                    ,sf.col("results"),sf.col("scoreCard"))

dfvantageModel3 = dfvantageModel3.withColumn("1_derogatoryAlert", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("derogatoryAlert")]))) \
                       .withColumn("1_factor_rank_1", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("factor_1_code")]))) \
                       .withColumn("1_factor_rank_2", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("factor_2_code")]))) \
                       .withColumn("1_factor_rank_3", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("factor_3_code")]))) \
                       .withColumn("1_factor_rank_4", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("factor_4_code")]))) \
                       .withColumn("1_fileInquiriesImpactedScore", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("fileInquiriesImpactedScore")]))) \
                       .withColumn("1_noScoreReason", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("noScoreReason")]))) \
                       .withColumn("1_results", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("results")]))) \
                       .withColumn("1_scoreCard", concat_udf_vm(sf.array([col for col in dfvantageModel3.columns if col.endswith("scoreCard")])))

dfvantageModel3Split = dfvantageModel3.withColumn("derogatoryAlert",sf.when(sf.split("1_derogatoryAlert","\|")[0]=="",sf.when(sf.split("1_derogatoryAlert","\|")[1]=="",sf.split("1_derogatoryAlert","\|")[2]).otherwise(sf.split("1_derogatoryAlert","\|")[1])).otherwise(sf.split("1_derogatoryAlert","\|")[0])) \
                                    .withColumn("factor_rank_1",sf.when(sf.split("1_factor_rank_1","\|")[0]=="",sf.when(sf.split("1_factor_rank_1","\|")[1]=="",sf.split("1_factor_rank_1","\|")[2]).otherwise(sf.split("1_factor_rank_1","\|")[1])).otherwise(sf.split("1_factor_rank_1","\|")[0])) \
                                    .withColumn("factor_rank_2",sf.when(sf.split("1_factor_rank_2","\|")[0]=="",sf.when(sf.split("1_factor_rank_2","\|")[1]=="",sf.split("1_factor_rank_2","\|")[2]).otherwise(sf.split("1_factor_rank_2","\|")[1])).otherwise(sf.split("1_factor_rank_2","\|")[0])) \
                                    .withColumn("factor_rank_3",sf.when(sf.split("1_factor_rank_3","\|")[0]=="",sf.when(sf.split("1_factor_rank_3","\|")[1]=="",sf.split("1_factor_rank_3","\|")[2]).otherwise(sf.split("1_factor_rank_3","\|")[1])).otherwise(sf.split("1_factor_rank_3","\|")[0])) \
                                    .withColumn("factor_rank_4",sf.when(sf.split("1_factor_rank_4","\|")[0]=="",sf.when(sf.split("1_factor_rank_4","\|")[1]=="",sf.split("1_factor_rank_4","\|")[2]).otherwise(sf.split("1_factor_rank_4","\|")[1])).otherwise(sf.split("1_factor_rank_4","\|")[0])) \
                                    .withColumn("fileInquiriesImpactedScore",sf.when(sf.split("1_fileInquiriesImpactedScore","\|")[0]=="",sf.when(sf.split("1_fileInquiriesImpactedScore","\|")[1]=="",sf.split("1_fileInquiriesImpactedScore","\|")[2]).otherwise(sf.split("1_fileInquiriesImpactedScore","\|")[1])).otherwise(sf.split("1_fileInquiriesImpactedScore","\|")[0])) \
                                    .withColumn("noScoreReason",sf.when(sf.split("1_noScoreReason","\|")[0]=="",sf.when(sf.split("1_noScoreReason","\|")[1]=="",sf.split("1_noScoreReason","\|")[2]).otherwise(sf.split("1_noScoreReason","\|")[1])).otherwise(sf.split("1_noScoreReason","\|")[0])) \
                                    .withColumn("results",sf.when(sf.split("1_results","\|")[0]=="",sf.when(sf.split("1_results","\|")[1]=="",sf.split("1_results","\|")[2]).otherwise(sf.split("1_results","\|")[1])).otherwise(sf.split("1_results","\|")[0])) \
                                    .withColumn("scoreCard",sf.when(sf.split("1_scoreCard","\|")[0]=="",sf.when(sf.split("1_scoreCard","\|")[1]=="",sf.split("1_scoreCard","\|")[2]).otherwise(sf.split("1_scoreCard","\|")[1])).otherwise(sf.split("1_scoreCard","\|")[0])) \

dfsplitColVM3 = dfvantageModel3Split.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("derogatoryAlert"),sf.col("factor_rank_1"),sf.col("factor_rank_2")
                                    ,sf.col("factor_rank_3"),sf.col("factor_rank_4"),sf.col("fileInquiriesImpactedScore"),sf.col("noScoreReason")
                                    ,sf.col("results"),sf.col("scoreCard"))

# dfsplitColcrCon = dfcreditorContact.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("decodeData"),sf.col("subscriber_address_location_city")
                                    # ,sf.col("subscriber_address_location_state"),sf.col("subscriber_address_location_zipCode"),sf.col("subscriber_address_street_unparsed")
                                    # ,sf.col("subscriber_industryCode"),sf.col("subscriber_inquirySubscriberPrefixCode"),sf.col("subscriber_memberCode"),sf.col("subscriber_name_unparsed")
                                    # ,sf.col("subscriber_phone_number_areaCode"),sf.col("subscriber_phone_number_exchange"),sf.col("subscriber_phone_number_qualifier")
                                    # ,sf.col("subscriber_phone_number_suffix"),sf.col("subscriber_phone_number_type"),sf.col("subscriber_phone_source"))

# dfsplitColSynFraud = dfsynFraud.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("derogatoryAlert"),sf.col("fileInquiriesImpactedScore")
                                    # ,sf.col("results"))


# dfrankedId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathRR)

# dfsplitColcrCon.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathCC)

dfbaseDataId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathId)

dfcreditSummary.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathCreditSumm)

# dfsplitColHRAFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathHRFA)
                   
dfsplitColMLA.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathMLA)

dfscoreModel.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathSM)

dfcreditVisionAttrV2.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathCVAV2)

dfstdChar.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathSC)

# dfsplitColSynFraud.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathSF)

dfsplitColVM.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathVM)

dfsplitColVM3.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathVM3)