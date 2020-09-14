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
appNameSuffix = vendor + "_DataModels_creditorContact"

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

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/creditorContact_3/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/creditorContact/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

dfcreditorContact = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfcreditorContact.printSchema()

dfcreditorContactFinalInt = dfcreditorContact.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), dfcreditorContact.colRegex("`07500_[0-9]+_\w*`"))

CreditorContactCnt = sorted([int(col.split("_")[1]) for col in dfcreditorContactFinalInt.columns if col.split("_")[0] == "07500" \
                        and col.split("_")[2] in ["decodeData","subscriber_address_location_city","subscriber_address_location_state","subscriber_address_location_zipCode" \
                                                ,"subscriber_address_street_unparsed","subscriber_industryCode","subscriber_inquirySubscriberPrefixCode","subscriber_memberCode" \
                                                ,"subscriber_name_unparsed","subscriber_phone_number_areaCode","subscriber_phone_number_exchange","subscriber_phone_number_qualifier" \
                                                ,"subscriber_phone_number_suffix","subscriber_phone_number_type","subscriber_phone_source"]])[-1]

# for col in dfcreditorContactFinalInt.columns:
    # instr = col.find('07500_', 7, 10)
    # for i in range(CreditorContactCnt):
        # rule = "_"+str(i+1)
        # if not rule+"_"+col[instr+1:] in dfcreditorContactFinalInt.columns:
            # dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(rule+"_"+col[instr+1:], sf.lit(None))

for i in range(CreditorContactCnt):
    prefix = "07500_"+str(i+1)
    if not prefix+"_decodeData" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_decodeData", sf.lit(None))
    if not prefix+"_subscriber_address_location_city" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_address_location_city", sf.lit(None))
    if not prefix+"_subscriber_address_location_state" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_address_location_state", sf.lit(None))
    if not prefix+"_subscriber_address_location_zipCode" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_address_location_zipCode", sf.lit(None))
    if not prefix+"_subscriber_address_street_unparsed" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_address_street_unparsed", sf.lit(None))
    if not prefix+"_subscriber_industryCode" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_industryCode", sf.lit(None))
    if not prefix+"_subscriber_inquirySubscriberPrefixCode" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_inquirySubscriberPrefixCode", sf.lit(None))
    if not prefix+"_subscriber_memberCode" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_memberCode", sf.lit(None))
    if not prefix+"_subscriber_name_unparsed" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_name_unparsed", sf.lit(None))
    if not prefix+"_subscriber_phone_number_areaCode" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_number_areaCode", sf.lit(None))
    if not prefix+"_subscriber_phone_number_exchange" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_number_exchange", sf.lit(None))
    if not prefix+"_subscriber_phone_number_qualifier" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_number_qualifier", sf.lit(None))
    if not prefix+"_subscriber_phone_number_suffix" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_number_suffix", sf.lit(None))
    if not prefix+"_subscriber_phone_number_type" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_number_type", sf.lit(None))
    if not prefix+"_subscriber_phone_source" in dfcreditorContactFinalInt.columns:
        dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn(prefix+"_subscriber_phone_source", sf.lit(None))


for i in range(CreditorContactCnt):
    rule = "07500_"+str(i+1)
    dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_decodeData"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_address_location_city"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_address_location_state"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_address_location_zipCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_address_street_unparsed"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_industryCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_inquirySubscriberPrefixCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_memberCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_name_unparsed"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_number_areaCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_number_exchange"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_number_qualifier"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_number_suffix"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_number_type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_phone_source"),sf.lit("$$$")), sf.lit("^"),\
                            ) \
                            )

#dfcreditorContact.show(2,False)

dfcreditorContactFinalInt = dfcreditorContactFinalInt.withColumn( "07500Array", sf.array([col for col in dfcreditorContactFinalInt.columns if col.split("_")[0] == "new07500"]) )

dfexplode = dfcreditorContactFinalInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("07500Array").alias("07500") \
            )

dfsplitCol = dfexplode.withColumn("07500_decodeData",sf.split("07500","\^")[0]) \
                    .withColumn("07500_subscriber_address_location_city",sf.split("07500","\^")[1]) \
                    .withColumn("07500_subscriber_address_location_state",sf.split("07500","\^")[2]) \
                    .withColumn("07500_subscriber_address_location_zipCode",sf.split("07500","\^")[3]) \
                    .withColumn("07500_subscriber_address_street_unparsed",sf.split("07500","\^")[4]) \
                    .withColumn("07500_subscriber_industryCode",sf.split("07500","\^")[5]) \
                    .withColumn("07500_subscriber_inquirySubscriberPrefixCode",sf.split("07500","\^")[6]) \
                    .withColumn("07500_subscriber_memberCode",sf.split("07500","\^")[7]) \
                    .withColumn("07500_subscriber_name_unparsed",sf.split("07500","\^")[8]) \
                    .withColumn("07500_subscriber_phone_number_areaCode",sf.split("07500","\^")[9]) \
                    .withColumn("07500_subscriber_phone_number_exchange",sf.split("07500","\^")[10]) \
                    .withColumn("07500_subscriber_phone_number_qualifier",sf.split("07500","\^")[11]) \
                    .withColumn("07500_subscriber_phone_number_suffix",sf.split("07500","\^")[12]) \
                    .withColumn("07500_subscriber_phone_number_type",sf.split("07500","\^")[13]) \
                    .withColumn("07500_subscriber_phone_source",sf.split("07500","\^")[14]) \
                    .drop("07500")

#dfsplitCol.show(10, False)

# df07500Str = df07500Str.withColumn("07500_datePaidOut_content", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_datePaidOut_estimatedCentury", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_datePaidOut_estimatedDay", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_datePaidOut_estimatedMonth", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_datePaidOut_estimatedYear", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_mostRecentPayment_date_content", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_mostRecentPayment_date_estimatedCentury", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_mostRecentPayment_date_estimatedDay", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_mostRecentPayment_date_estimatedMonth", sf.lit(None).cast(StringType())) \
                    # .withColumn("07500_mostRecentPayment_date_estimatedYear", sf.lit(None).cast(StringType())) \

# df07500StrCol = df07500Str.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            # ,sf.col("07500_decodeData")
                            # ,sf.col("07500_subscriber_address_location_city")
                            # ,sf.col("07500_subscriber_address_location_state")
                            # ,sf.col("07500_subscriber_address_location_zipCode")
                            # ,sf.col("07500_subscriber_address_street_unparsed")
                            # ,sf.col("07500_subscriber_industryCode")
                            # ,sf.col("07500_subscriber_inquirySubscriberPrefixCode")
                            # ,sf.col("07500_subscriber_memberCode")
                            # ,sf.col("07500_subscriber_name_unparsed")
                            # ,sf.col("07500_subscriber_phone_number_areaCode")
                            # ,sf.col("07500_subscriber_phone_number_exchange")
                            # ,sf.col("07500_subscriber_phone_number_qualifier")
                            # ,sf.col("07500_subscriber_phone_number_suffix")
                            # ,sf.col("07500_subscriber_phone_number_type")
                            # ,sf.col("07500_subscriber_phone_source")
                            # )

# dfsplitColFinal = dfsplitCol.union(df07500StrCol)

# dfsplitColFinal.show(100, False)

df07500 = dfsplitCol.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("07500_decodeData")=="$$$",None).otherwise(sf.col("07500_decodeData")).alias("decodeData"), \
                sf.when(sf.col("07500_subscriber_address_location_city")=="$$$",None).otherwise(sf.col("07500_subscriber_address_location_city")).alias("subscriber_address_location_city"), \
                sf.when(sf.col("07500_subscriber_address_location_state")=="$$$",None).otherwise(sf.col("07500_subscriber_address_location_state")).alias("subscriber_address_location_state"), \
                sf.when(sf.col("07500_subscriber_address_location_zipCode")=="$$$",None).otherwise(sf.col("07500_subscriber_address_location_zipCode")).alias("subscriber_address_location_zipCode"), \
                sf.when(sf.col("07500_subscriber_address_street_unparsed")=="$$$",None).otherwise(sf.col("07500_subscriber_address_street_unparsed")).alias("subscriber_address_street_unparsed"), \
                sf.when(sf.col("07500_subscriber_industryCode")=="$$$",None).otherwise(sf.col("07500_subscriber_industryCode")).alias("subscriber_industryCode"), \
                sf.when(sf.col("07500_subscriber_inquirySubscriberPrefixCode")=="$$$",None).otherwise(sf.col("07500_subscriber_inquirySubscriberPrefixCode")).alias("subscriber_inquirySubscriberPrefixCode"), \
                sf.when(sf.col("07500_subscriber_memberCode")=="$$$",None).otherwise(sf.col("07500_subscriber_memberCode")).alias("subscriber_memberCode"), \
                sf.when(sf.col("07500_subscriber_name_unparsed")=="$$$",None).otherwise(sf.col("07500_subscriber_name_unparsed")).alias("subscriber_name_unparsed"), \
                sf.when(sf.col("07500_subscriber_phone_number_areaCode")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_number_areaCode")).alias("subscriber_phone_number_areaCode"), \
                sf.when(sf.col("07500_subscriber_phone_number_exchange")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_number_exchange")).alias("subscriber_phone_number_exchange"), \
                sf.when(sf.col("07500_subscriber_phone_number_qualifier")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_number_qualifier")).alias("subscriber_phone_number_qualifier"), \
                sf.when(sf.col("07500_subscriber_phone_number_suffix")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_number_suffix")).alias("subscriber_phone_number_suffix"), \
                sf.when(sf.col("07500_subscriber_phone_number_type")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_number_type")).alias("subscriber_phone_number_type"), \
                sf.when(sf.col("07500_subscriber_phone_source")=="$$$",None).otherwise(sf.col("07500_subscriber_phone_source")).alias("subscriber_phone_source")) \
                .where(sf.col("decodeData").isNotNull() | \
                sf.col("subscriber_address_location_city").isNotNull() | \
                sf.col("subscriber_address_location_state").isNotNull() | \
                sf.col("subscriber_address_location_zipCode").isNotNull() | \
                sf.col("subscriber_address_street_unparsed").isNotNull() | \
                sf.col("subscriber_industryCode").isNotNull() | \
                sf.col("subscriber_inquirySubscriberPrefixCode").isNotNull() | \
                sf.col("subscriber_memberCode").isNotNull() | \
                sf.col("subscriber_name_unparsed").isNotNull() | \
                sf.col("subscriber_phone_number_areaCode").isNotNull() | \
                sf.col("subscriber_phone_number_exchange").isNotNull() | \
                sf.col("subscriber_phone_number_qualifier").isNotNull() | \
                sf.col("subscriber_phone_number_suffix").isNotNull() | \
                sf.col("subscriber_phone_number_type").isNotNull() | \
                sf.col("subscriber_phone_source").isNotNull() \
                )

df07500.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)