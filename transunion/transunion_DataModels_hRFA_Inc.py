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
appNameSuffix = vendor + "_DataModels_hRFA"

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

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/highRiskFraudAlert/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathHRFA = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/highRiskFraudAlert/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfhRFA_2 = df.withColumn("newCustText", \
                        sf.concat(
                            sf.coalesce(blank_as_null("message_text"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_1"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_2"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_3"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_4"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_5"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_6"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_7"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_8"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_9"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_10"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_11"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_12"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_13"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_14"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_15"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_16"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_17"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_18"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_19"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_20"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_21"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_22"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_23"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_24"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_25"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_26"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_27"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_28"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_29"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_30"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_31"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_32"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_33"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_34"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_35"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_36"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_37"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_38"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_39"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_40"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_41"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_42"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_43"),sf.lit("ZzZzZ")), sf.lit("YyYyY"),\
                            sf.coalesce(blank_as_null("message_text_44"),sf.lit("ZzZzZ")), sf.lit("YyYyY")\
                            ) \
                            )

dfhRFA_3 = dfhRFA_2.withColumn("newCustTextFormat",sf.regexp_replace(sf.col("newCustText"),"ZzZzZYyYyY",""))

dfhRFA_4 = dfhRFA_3.withColumn("newCustTextFormatFinal",sf.regexp_replace(sf.col("newCustTextFormat"),"YyYyY","^"))

dfhRFA_5 = dfhRFA_4.withColumn("message_text",sf.split("newCustTextFormatFinal","\^")[0]) \
                .withColumn("message_text_1",sf.split("newCustTextFormatFinal","\^")[1]) \
                .withColumn("message_text_2",sf.split("newCustTextFormatFinal","\^")[2]) \
                .withColumn("message_text_3",sf.split("newCustTextFormatFinal","\^")[3]) \
                .withColumn("message_text_4",sf.split("newCustTextFormatFinal","\^")[4]) \
                .withColumn("message_text_5",sf.split("newCustTextFormatFinal","\^")[5]) \
                .withColumn("message_text_6",sf.split("newCustTextFormatFinal","\^")[6]) \
                .withColumn("message_text_7",sf.split("newCustTextFormatFinal","\^")[7]) \
                .withColumn("message_text_8",sf.split("newCustTextFormatFinal","\^")[8]) \
                .withColumn("message_text_9",sf.split("newCustTextFormatFinal","\^")[9]) \
                .withColumn("message_text_10",sf.split("newCustTextFormatFinal","\^")[10]) \
                .drop(sf.col("newCustText")) \
                .drop(sf.col("newCustTextFormat"))

dfsplitColHRA = dfhRFA_5.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("decease_alertMessageCode")
                        ,sf.col("decease_dateOfBirth_content"),sf.col("decease_dateOfBirth_estimatedCentury"),sf.col("decease_dateOfBirth_estimatedDay")
                        ,sf.col("decease_dateOfBirth_estimatedMonth"),sf.col("decease_dateOfBirth_estimatedYear")
                        ,sf.col("decease_dateOfDeath_content"),sf.col("decease_dateOfDeath_estimatedCentury"),sf.col("decease_dateOfDeath_estimatedDay")
                        ,sf.col("decease_dateOfDeath_estimatedMonth"),sf.col("decease_dateOfDeath_estimatedYear")
                        ,sf.col("decease_lastResidency_location_city"),sf.col("decease_lastResidency_location_state"),sf.col("decease_lastResidency_location_zipCode")
                        ,sf.col("decease_locationOfPayments_location_city"),sf.col("decease_locationOfPayments_location_state"),sf.col("decease_locationOfPayments_location_zipCode")
                        ,sf.col("decease_name_person_first"),sf.col("decease_name_person_last"),sf.col("decease_source")
                        ,sf.col("message_code_0")
                        ,sf.when(sf.col("message_code_1")=="",sf.col("decease_alertMessageCode")).otherwise(sf.when(sf.col("message_code_1")=="99999999","9999").otherwise(sf.col("message_code_1"))).alias("message_code")
                        ,sf.when(sf.col("message_code_2")=="99999999","9999").otherwise(sf.col("message_code_2")).alias("message_code_1")
                        ,sf.col("message_code_3").alias("message_code_2"),sf.col("message_code_4").alias("message_code_3"),sf.col("message_code_5").alias("message_code_4")
                        ,sf.col("message_code_6").alias("message_code_5"),sf.col("message_code_7").alias("message_code_6"),sf.col("message_code_8").alias("message_code_7"),sf.col("message_code_9").alias("message_code_8"),sf.col("message_code_10").alias("message_code_9")
                        ,sf.col("message_code_11").alias("message_code_10"),sf.col("message_code_12").alias("message_code_11"),sf.col("message_code_13").alias("message_code_12"),sf.col("message_code_14").alias("message_code_13"),sf.col("message_code_15").alias("message_code_14")
                        ,sf.col("message_code_16").alias("message_code_15"),sf.col("message_code_17").alias("message_code_16"),sf.col("message_code_18").alias("message_code_17"),sf.col("message_code_19").alias("message_code_18"),sf.col("message_code_20").alias("message_code_19")
                        ,sf.col("message_code_21").alias("message_code_20"),sf.col("message_code_22").alias("message_code_21"),sf.col("message_code_23").alias("message_code_22"),sf.col("message_code_24").alias("message_code_23"),sf.col("message_code_25").alias("message_code_24")
                        ,sf.col("message_code_26").alias("message_code_25"),sf.col("message_code_27").alias("message_code_26"),sf.col("message_code_28").alias("message_code_27"),sf.col("message_code_29").alias("message_code_28"),sf.col("message_code_30").alias("message_code_29")
                        ,sf.col("message_code_31").alias("message_code_30"),sf.col("message_code_32").alias("message_code_31"),sf.col("message_code_33").alias("message_code_32"),sf.col("message_code_34").alias("message_code_33"),sf.col("message_code_35").alias("message_code_34")
                        ,sf.col("message_code_36").alias("message_code_35"),sf.col("message_code_37").alias("message_code_36"),sf.col("message_code_38").alias("message_code_37"),sf.col("message_code_39").alias("message_code_38")
                        ,sf.when(sf.col("message_text")=="HIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZEDHIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZED","HIGH RISK FRAUD ALERT SYSTEM ACCESS NOT AUTHORIZED").otherwise(sf.col("message_text")).alias("message_text")
                        ,sf.col("message_text_1"),sf.col("message_text_2"),sf.col("message_text_3"),sf.col("message_text_4"),sf.col("message_text_5")
                        ,sf.col("message_text_6"),sf.col("message_text_7"),sf.col("message_text_8"),sf.col("message_text_9"),sf.col("message_text_10")
                        )

dfsplitColHRAFinal = dfsplitColHRA.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),sf.col("decease_alertMessageCode")
                        ,sf.col("decease_dateOfBirth_content"),sf.col("decease_dateOfBirth_estimatedCentury"),sf.col("decease_dateOfBirth_estimatedDay")
                        ,sf.col("decease_dateOfBirth_estimatedMonth"),sf.col("decease_dateOfBirth_estimatedYear")
                        ,sf.col("decease_dateOfDeath_content"),sf.col("decease_dateOfDeath_estimatedCentury"),sf.col("decease_dateOfDeath_estimatedDay")
                        ,sf.col("decease_dateOfDeath_estimatedMonth"),sf.col("decease_dateOfDeath_estimatedYear")
                        ,sf.col("decease_lastResidency_location_city"),sf.col("decease_lastResidency_location_state"),sf.col("decease_lastResidency_location_zipCode")
                        ,sf.col("decease_locationOfPayments_location_city"),sf.col("decease_locationOfPayments_location_state"),sf.col("decease_locationOfPayments_location_zipCode")
                        ,sf.col("decease_name_person_first"),sf.col("decease_name_person_last"),sf.col("decease_source")
                        ,sf.when(sf.col("message_code_0")=="",sf.col("message_code")).otherwise(sf.col("message_code_0")).alias("message_code")
                        ,sf.col("message_code_1"),sf.col("message_code_2"),sf.col("message_code_3"),sf.col("message_code_4"),sf.col("message_code_5"),sf.col("message_code_6")
                        ,sf.col("message_code_7"),sf.col("message_code_8"),sf.col("message_code_9"),sf.col("message_code_10"),sf.col("message_code_11"),sf.col("message_code_12")
                        ,sf.col("message_code_13"),sf.col("message_code_14"),sf.col("message_code_15"),sf.col("message_code_16"),sf.col("message_code_17"),sf.col("message_code_18")
                        ,sf.col("message_code_19"),sf.col("message_code_20"),sf.col("message_code_21"),sf.col("message_code_22"),sf.col("message_code_23"),sf.col("message_code_24")
                        ,sf.col("message_code_25"),sf.col("message_code_26"),sf.col("message_code_27"),sf.col("message_code_28"),sf.col("message_code_29"),sf.col("message_code_30")
                        ,sf.col("message_code_31"),sf.col("message_code_32"),sf.col("message_code_33"),sf.col("message_code_34"),sf.col("message_code_35"),sf.col("message_code_36")
                        ,sf.col("message_code_37"),sf.col("message_code_38")
                        ,sf.col("message_text"),sf.col("message_text_1"),sf.col("message_text_2"),sf.col("message_text_3"),sf.col("message_text_4"),sf.col("message_text_5")
                        ,sf.col("message_text_6"),sf.col("message_text_7"),sf.col("message_text_8"),sf.col("message_text_9"),sf.col("message_text_10")
                        )

dfsplitColHRAFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathHRFA)