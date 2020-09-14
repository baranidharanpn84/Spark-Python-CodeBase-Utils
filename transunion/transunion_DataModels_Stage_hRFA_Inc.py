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
appNameSuffix = vendor + "Spark_DataModels_stg_hRFA"

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

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathHRFA = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/highRiskFraudAlert/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfhRFA = df.select(sf.col("id"),sf.col("createdDate"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`06500_[a-zA-Z0-9_]*`"))

dfhRFA_1 = dfhRFA.withColumn("decease_alertMessageCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_alertMessageCode")]))) \
                       .withColumn("decease_dateOfBirth_content", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_content")]))) \
                       .withColumn("decease_dateOfBirth_estimatedCentury", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedCentury")]))) \
                       .withColumn("decease_dateOfBirth_estimatedDay", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedDay")]))) \
                       .withColumn("decease_dateOfBirth_estimatedMonth", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedMonth")]))) \
                       .withColumn("decease_dateOfBirth_estimatedYear", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfBirth_estimatedYear")]))) \
                       .withColumn("decease_dateOfDeath_content", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_content")]))) \
                       .withColumn("decease_dateOfDeath_estimatedCentury", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedCentury")]))) \
                       .withColumn("decease_dateOfDeath_estimatedDay", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedDay")]))) \
                       .withColumn("decease_dateOfDeath_estimatedMonth", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedMonth")]))) \
                       .withColumn("decease_dateOfDeath_estimatedYear", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_dateOfDeath_estimatedYear")]))) \
                       .withColumn("decease_lastResidency_location_city", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_city")]))) \
                       .withColumn("decease_lastResidency_location_state", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_state")]))) \
                       .withColumn("decease_lastResidency_location_zipCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_lastResidency_location_zipCode")]))) \
                       .withColumn("decease_locationOfPayments_location_city", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_city")]))) \
                       .withColumn("decease_locationOfPayments_location_state", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_state")]))) \
                       .withColumn("decease_locationOfPayments_location_zipCode", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_locationOfPayments_location_zipCode")]))) \
                       .withColumn("decease_name_person_first", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_name_person_first")]))) \
                       .withColumn("decease_name_person_last", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_name_person_last")]))) \
                       .withColumn("decease_source", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("decease_source")]))) \
                       .withColumn("message_code_0", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_code")]))) \
                       .withColumn("message_code_1", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_code")]))) \
                       .withColumn("message_code_2", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_code")]))) \
                       .withColumn("message_code_3", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_code")]))) \
                       .withColumn("message_code_4", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_code")]))) \
                       .withColumn("message_code_5", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_code")]))) \
                       .withColumn("message_code_6", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_code")]))) \
                       .withColumn("message_code_7", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_code")]))) \
                       .withColumn("message_code_8", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_code")]))) \
                       .withColumn("message_code_9", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_code")]))) \
                       .withColumn("message_code_10", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_code")]))) \
                       .withColumn("message_code_11", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_code")]))) \
                       .withColumn("message_code_12", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_code")]))) \
                       .withColumn("message_code_13", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_code")]))) \
                       .withColumn("message_code_14", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_code")]))) \
                       .withColumn("message_code_15", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_code")]))) \
                       .withColumn("message_code_16", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_code")]))) \
                       .withColumn("message_code_17", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_code")]))) \
                       .withColumn("message_code_18", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_code")]))) \
                       .withColumn("message_code_19", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_code")]))) \
                       .withColumn("message_code_20", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_code")]))) \
                       .withColumn("message_code_21", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_code")]))) \
                       .withColumn("message_code_22", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_code")]))) \
                       .withColumn("message_code_23", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_code")]))) \
                       .withColumn("message_code_24", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_code")]))) \
                       .withColumn("message_code_25", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_code")]))) \
                       .withColumn("message_code_26", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_code")]))) \
                       .withColumn("message_code_27", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_code")]))) \
                       .withColumn("message_code_28", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_code")]))) \
                       .withColumn("message_code_29", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_code")]))) \
                       .withColumn("message_code_30", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_code")]))) \
                       .withColumn("message_code_31", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_31_code")]))) \
                       .withColumn("message_code_32", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_32_code")]))) \
                       .withColumn("message_code_33", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_33_code")]))) \
                       .withColumn("message_code_34", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_34_code")]))) \
                       .withColumn("message_code_35", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_35_code")]))) \
                       .withColumn("message_code_36", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_36_code")]))) \
                       .withColumn("message_code_37", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_37_code")]))) \
                       .withColumn("message_code_38", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_38_code")]))) \
                       .withColumn("message_code_39", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_39_code")]))) \
                       .withColumn("message_text", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_text")]))) \
                       .withColumn("message_text_1", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_1_text")]))) \
                       .withColumn("message_text_2", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_2_text")]))) \
                       .withColumn("message_text_3", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_3_text")]))) \
                       .withColumn("message_text_4", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_4_text")]))) \
                       .withColumn("message_text_5", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_5_text")]))) \
                       .withColumn("message_text_6", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_6_text")]))) \
                       .withColumn("message_text_7", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_7_text")]))) \
                       .withColumn("message_text_8", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_8_text")]))) \
                       .withColumn("message_text_9", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_9_text")]))) \
                       .withColumn("message_text_10", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_10_text")]))) \
                       .withColumn("message_text_11", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_11_text")]))) \
                       .withColumn("message_text_12", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_12_text")]))) \
                       .withColumn("message_text_13", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_13_text")]))) \
                       .withColumn("message_text_14", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_14_text")]))) \
                       .withColumn("message_text_15", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_15_text")]))) \
                       .withColumn("message_text_16", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_16_text")]))) \
                       .withColumn("message_text_17", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_17_text")]))) \
                       .withColumn("message_text_18", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_18_text")]))) \
                       .withColumn("message_text_19", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_19_text")]))) \
                       .withColumn("message_text_20", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_20_text")]))) \
                       .withColumn("message_text_21", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_21_text")]))) \
                       .withColumn("message_text_22", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_22_text")]))) \
                       .withColumn("message_text_23", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_23_text")]))) \
                       .withColumn("message_text_24", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_24_text")]))) \
                       .withColumn("message_text_25", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_25_text")]))) \
                       .withColumn("message_text_26", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_26_text")]))) \
                       .withColumn("message_text_27", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_27_text")]))) \
                       .withColumn("message_text_28", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_28_text")]))) \
                       .withColumn("message_text_29", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_29_text")]))) \
                       .withColumn("message_text_30", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_30_text")]))) \
                       .withColumn("message_text_31", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_31_text")]))) \
                       .withColumn("message_text_32", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_32_text")]))) \
                       .withColumn("message_text_33", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_33_text")]))) \
                       .withColumn("message_text_34", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_34_text")]))) \
                       .withColumn("message_text_35", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_35_text")]))) \
                       .withColumn("message_text_36", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_36_text")]))) \
                       .withColumn("message_text_37", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_37_text")]))) \
                       .withColumn("message_text_38", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_38_text")]))) \
                       .withColumn("message_text_39", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_39_text")]))) \
                       .withColumn("message_text_40", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_40_text")]))) \
                       .withColumn("message_text_41", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_41_text")]))) \
                       .withColumn("message_text_42", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_42_text")]))) \
                       .withColumn("message_text_43", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_43_text")]))) \
                       .withColumn("message_text_44", concat_udf(sf.array([col for col in dfhRFA.columns if col.endswith("message_44_text")]))) \

dfhRFA_11 = dfhRFA_1.select([col for col in dfhRFA_1.columns if not col.startswith("06500_")])

dfhRFA_11.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathHRFA)