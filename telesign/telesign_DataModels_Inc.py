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
appNameSuffix = vendor + "_Spark_DataModels"

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

prefix_app = "gold/fdm/loan_application/applicant/"
app_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_app).split("/")[:-1]) + "/"

dfrmi = sparkSession.read.format("parquet").load(rmi_path)
dfrms = sparkSession.read.format("parquet").load(rms_path)
dfapplicant = sparkSession.read.format("parquet").load(app_path)

dfrmsNS1 = dfrms.where(sf.col("shadow_record").isNull())

dfrmsNS2 = dfrms.where(sf.col("shadow_record") == 0)

dfrmsNS = dfrmsNS1.union(dfrmsNS2)

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskrun/"
tgtfilePathDevice = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/device/"
tgtfilePathContact = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/contact/"
tgtfilePathSubStatus = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/sub_status/"
tgtfilePathStatus = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/status/"
tgtfilePathRefId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/reference_id"
tgtfilePathPhoneType = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/phone_type/"
tgtfilePathNumOriginal = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/num_original/"
tgtfilePathNumCleansing = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/num_cleansing/"
tgtfilePathNumDeactivate = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/num_deactivation/"
tgtfilePathLocation = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/location/"
tgtfilePathExternalId = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/external_id/"
tgtfilePathCarrier = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/carrier/"
tgtfilePathBlockListing = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/block_listing/"

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

dfparquet = dfparquetSrc.join(dfapplicant, dfparquetSrc.applicantId == dfapplicant.applicant_id, 'left_outer') \
                .select(dfparquetSrc["*"],dfapplicant["loan_application_id"].alias("loanApplicationId"))

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

dfjoin = dfbaseData.join(dfrmi,(dfbaseData.loanApplicationId == dfrmi.loan_application_id) & \
                                    (sf.unix_timestamp(dfbaseData.createdDate) - sf.unix_timestamp(dfrmi.date_created) >= 0),'left_outer') \
                    .join(dfrmsNS,(dfrmi.id == dfrmsNS.input_id),'left_outer') \
                    .select(dfbaseData.id \
                    ,dfbaseData.applicantId \
                    ,dfbaseData.applicationSource \
                    ,dfbaseData.mvpApplicantId \
                    ,dfbaseData.loanApplicationId \
                    # ,dfbaseData.mvpLoanApplicationId \
                    ,sf.regexp_replace(dfbaseData.createdDate,"T"," ").cast(TimestampType()).alias("telesignTimestampUTC") \
                    ,dfbaseData.createdDatePT.alias("telesignTimestampPT") \
                    ,dfrmi.id.alias("rmiId") \
                    ,dfrmi.date_created.alias("riskTimestampPT") \
                    ,dfrmsNS.score_type.alias("scoreType") \
                    ,dfbaseData.year \
                    ,dfbaseData.month \
                    ,dfbaseData.day)

dfrankedId =  dfjoin.withColumn("row_num", sf.row_number().over(Window.partitionBy("loanApplicationId","telesignTimestampPT").orderBy(sf.desc("riskTimestampPT"),sf.desc("scoreType")))) \
                    .where(sf.col("row_num") == 1) \
                    .select(dfjoin["*"])

dfsplitColDevice = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("device_info_imei").alias("imei")
                                    ,sf.col("device_info_make").alias("make")
                                    ,sf.col("device_info_model").alias("model")
                                    ,sf.col("device_info_status_code").alias("status_code")
                                    ,sf.col("device_info_status_description").alias("status_desc")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColContact = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("contact_address1").alias("address1")
                                    ,sf.col("contact_address2").alias("address2")
                                    ,sf.col("contact_address3").alias("address3")
                                    ,sf.col("contact_address4").alias("address4")
                                    ,sf.col("contact_city").alias("city")
                                    ,sf.col("contact_country").alias("country")
                                    ,sf.col("contact_email_address").alias("email_address")
                                    ,sf.col("contact_first_name").alias("first_name")
                                    ,sf.col("contact_last_name").alias("last_name")
                                    ,sf.col("contact_state_province").alias("state_province")
                                    ,sf.col("contact_status_code").alias("status_code")
                                    ,sf.col("contact_status_description").alias("status_description")
                                    ,sf.col("contact_zip_postal_code").alias("zip_postal_code")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColSubStatus = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("subscriber_status_account_activation_date").alias("account_activation_date")
                                    ,sf.col("subscriber_status_account_status").alias("account_status")
                                    ,sf.col("subscriber_status_account_tenure_max").alias("account_tenure_max")
                                    ,sf.col("subscriber_status_account_tenure_min").alias("account_tenure_min")
                                    ,sf.col("subscriber_status_account_type").alias("account_type")
                                    ,sf.col("subscriber_status_contract_type").alias("contract_type")
                                    ,sf.col("subscriber_status_primary_account_holder").alias("primary_account_holder")
                                    ,sf.col("subscriber_status_status_code").alias("status_status_code")
                                    ,sf.col("subscriber_status_status_description").alias("status_description")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColStatus = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("status_code").alias("code")
                                    ,sf.col("status_description").alias("description")
                                    ,sf.col("status_updated_on").alias("updated_on")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColRefId = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("reference_id").alias("reference_id")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColPhoneType = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("phone_type_code").alias("code")
                                    ,sf.col("phone_type_description").alias("description")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColNumOriginal = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("numbering_original_complete_phone_number").alias("complete_phone_number")
                                    ,sf.col("numbering_original_country_code").alias("country_code")
                                    ,sf.col("numbering_original_phone_number").alias("phone_number")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColNumCleansing = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("numbering_cleansing_call_cleansed_code").alias("call_cleansed_code")
                                    ,sf.col("numbering_cleansing_call_country_code").alias("call_country_code")
                                    ,sf.col("numbering_cleansing_call_max_length").alias("call_max_length")
                                    ,sf.col("numbering_cleansing_call_min_length").alias("call_min_length")
                                    ,sf.col("numbering_cleansing_call_phone_number").alias("call_phone_number")
                                    ,sf.col("numbering_cleansing_sms_cleansed_code").alias("sms_cleansed_code")
                                    ,sf.col("numbering_cleansing_sms_country_code").alias("sms_country_code")
                                    ,sf.col("numbering_cleansing_sms_max_length").alias("sms_max_length")
                                    ,sf.col("numbering_cleansing_sms_min_length").alias("sms_min_length")
                                    ,sf.col("numbering_cleansing_sms_phone_number").alias("sms_phone_number")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColNumDeactivate = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("carrier_name").alias("carrier_name")
                                    ,sf.col("number_deactivation_last_deactivated").alias("last_deactivated")
                                    ,sf.col("number_deactivation_status_code").alias("status_code")
                                    ,sf.col("number_deactivation_status_description").alias("status_description")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColLocation = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("location_city").alias("city")
                                    ,sf.col("location_coordinates_latitude").alias("coordinates_latitude")
                                    ,sf.col("location_coordinates_longitude").alias("coordinates_longitude")
                                    ,sf.col("location_country_iso2").alias("country_iso2")
                                    ,sf.col("location_country_iso3").alias("country_iso3")
                                    ,sf.col("location_country_name").alias("country_name")
                                    ,sf.col("location_county").alias("county") 
                                    ,sf.col("location_metro_code").alias("metro_code")
                                    ,sf.col("location_state").alias("state")
                                    ,sf.col("location_time_zone_name").alias("time_zone_name")
                                    ,sf.col("location_time_zone_utc_offset_max").alias("time_zone_utc_offset_max")
                                    ,sf.col("location_time_zone_utc_offset_min").alias("time_zone_utc_offset_min")
                                    ,sf.col("location_zip").alias("zip")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

# dfsplitColExternalId = dfbaseData.select(sf.col("id").alias("id")
                                    # ,sf.col("external_id").alias("external_id")
                                    # ,sf.col("year")
                                    # ,sf.col("month")
                                    # ,sf.col("day"))

dfsplitColCarrier = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("carrier_name").alias("carrier_name")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfsplitColBlockListing = dfbaseData.select(sf.col("id").alias("id")
                                    ,sf.col("blocklisting_block_code").alias("block_code")
                                    ,sf.col("blocklisting_block_description").alias("block_description")
                                    ,sf.col("blocklisting_blocked").alias("blocked")
                                    ,sf.col("year")
                                    ,sf.col("month")
                                    ,sf.col("day"))

dfrankedId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathId)

dfsplitColDevice.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathDevice)

dfsplitColContact.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathContact)

dfsplitColSubStatus.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathSubStatus)

dfsplitColStatus.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathStatus)

dfsplitColRefId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRefId)

dfsplitColPhoneType.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathPhoneType)

dfsplitColNumOriginal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathNumOriginal)

dfsplitColNumCleansing.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathNumCleansing)

dfsplitColNumDeactivate.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathNumDeactivate)

dfsplitColLocation.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathLocation)

# dfsplitColExternalId.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   # .write.format("parquet") \
                   # .partitionBy("year","month","day") \
                   # .mode("overwrite") \
                   # .save(tgtfilePathExternalId)

dfsplitColCarrier.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathCarrier)

dfsplitColBlockListing.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathBlockListing)