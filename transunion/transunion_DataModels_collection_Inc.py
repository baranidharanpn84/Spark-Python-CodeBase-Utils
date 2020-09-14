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
appNameSuffix = vendor + "_DataModels_Collection"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/collection/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfccCollectionInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`ccCollection_[0-9]+_\w*`"))

dfccCollectionStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`ccCollection_[a-zA-Z_]*`"))

#dfccCollectionStr.show()

collectionCnt = sorted([int(col.split("_")[1]) for col in dfccCollectionInt.columns if col.split("_")[0] == "ccCollection" \
                        and col.split("_")[2] in ["ECOADesignator","accountNumber","accountRating","account_type","closedIndicator","currentBalance","dateClosed_content","dateClosed_estimatedCentury" \
                                                ,"dateClosed_estimatedDay","dateClosed_estimatedMonth","dateClosed_estimatedYear","dateEffective_content","dateEffective_estimatedCentury","dateEffective_estimatedDay" \
                                                ,"dateEffective_estimatedMonth","dateEffective_estimatedYear","dateOpened_content","dateOpened_estimatedCentury","dateOpened_estimatedDay","dateOpened_estimatedMonth" \
                                                ,"dateOpened_estimatedYear","datePaidOut_content","datePaidOut_estimatedCentury","datePaidOut_estimatedDay","datePaidOut_estimatedMonth","datePaidOut_estimatedYear" \
                                                ,"mostRecentPayment_date_content","mostRecentPayment_date_estimatedCentury","mostRecentPayment_estimatedDay","mostRecentPayment_estimatedMonth" \
                                                ,"mostRecentPayment_estimatedYear","original_balance","original_creditGrantor_unparsed","original_creditorClassification","pastDue","portfolioType","remark_code","remark_type" \
                                                ,"subscriber_industryCode","subscriber_memberCode","subscriber_name_unparsed","updateMethod"]])[-1]

#print(collectionCnt)

# for col in dfccCollectionInt.columns:
    # instr = col.find('_', 14, 17)
    # for i in range(collectionCnt):
        # prefix = "ccCollection_"+str(i+1)
        # if not prefix+"_"+col[instr+1:] in dfccCollectionInt.columns:
            # dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_"+col[instr+1:], sf.lit(None))

for i in range(collectionCnt):
    prefix = "ccCollection_"+str(i+1)
    if not prefix+"_closedIndicator" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_closedIndicator", sf.lit(None))
    if not prefix+"_dateClosed_content" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_dateClosed_content", sf.lit(None))
    if not prefix+"_dateClosed_estimatedCentury" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_dateClosed_estimatedCentury", sf.lit(None))
    if not prefix+"_dateClosed_estimatedDay" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_dateClosed_estimatedDay", sf.lit(None))
    if not prefix+"_dateClosed_estimatedMonth" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_dateClosed_estimatedMonth", sf.lit(None))
    if not prefix+"_dateClosed_estimatedYear" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_dateClosed_estimatedYear", sf.lit(None))
    if not prefix+"_datePaidOut_content" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_datePaidOut_content", sf.lit(None))
    if not prefix+"_datePaidOut_estimatedCentury" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_datePaidOut_estimatedCentury", sf.lit(None))
    if not prefix+"_datePaidOut_estimatedDay" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_datePaidOut_estimatedDay", sf.lit(None))
    if not prefix+"_datePaidOut_estimatedMonth" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_datePaidOut_estimatedMonth", sf.lit(None))
    if not prefix+"_datePaidOut_estimatedYear" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_datePaidOut_estimatedYear", sf.lit(None))
    if not prefix+"_mostRecentPayment_date_content" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_mostRecentPayment_date_content", sf.lit(None))
    if not prefix+"_mostRecentPayment_date_estimatedCentury" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_mostRecentPayment_date_estimatedCentury", sf.lit(None))
    if not prefix+"_mostRecentPayment_date_estimatedDay" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_mostRecentPayment_date_estimatedDay", sf.lit(None))
    if not prefix+"_mostRecentPayment_date_estimatedMonth" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_mostRecentPayment_date_estimatedMonth", sf.lit(None))
    if not prefix+"_mostRecentPayment_date_estimatedYear" in dfccCollectionInt.columns:
        dfccCollectionInt = dfccCollectionInt.withColumn(prefix+"_mostRecentPayment_date_estimatedYear", sf.lit(None))

for i in range(collectionCnt):
    rule = "ccCollection_"+str(i+1)
    dfccCollectionInt = dfccCollectionInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_ECOADesignator"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_accountNumber"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_accountRating"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_account_type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_closedIndicator"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_currentBalance"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateClosed_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateClosed_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateClosed_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateClosed_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateClosed_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateEffective_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateEffective_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateEffective_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateEffective_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateEffective_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateOpened_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateOpened_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateOpened_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateOpened_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_dateOpened_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_datePaidOut_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_datePaidOut_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_datePaidOut_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_datePaidOut_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_datePaidOut_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_mostRecentPayment_date_content"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_mostRecentPayment_date_estimatedCentury"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_mostRecentPayment_date_estimatedDay"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_mostRecentPayment_date_estimatedMonth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_mostRecentPayment_date_estimatedYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_original_balance"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_original_creditGrantor_unparsed"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_original_creditorClassification"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_pastDue"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_portfolioType"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_remark_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_remark_type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_industryCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_memberCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_subscriber_name_unparsed"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_updateMethod"),sf.lit("$$$")) \
                            ) \
                            )

#dfccCollectionInt.show(2,False)

dfccCollectionInt = dfccCollectionInt.withColumn( "ccCollectionArray", sf.array([col for col in dfccCollectionInt.columns if col.split("_")[0] == "newccCollection"]) )

dfexplode = dfccCollectionInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("ccCollectionArray").alias("ccCollection") \
            )

dfsplitCol = dfexplode.withColumn("ccCollection_ECOADesignator",sf.split("ccCollection","\^")[0]) \
                    .withColumn("ccCollection_accountNumber",sf.split("ccCollection","\^")[1]) \
                    .withColumn("ccCollection_accountRating",sf.split("ccCollection","\^")[2]) \
                    .withColumn("ccCollection_account_type",sf.split("ccCollection","\^")[3]) \
                    .withColumn("ccCollection_closedIndicator",sf.split("ccCollection","\^")[4]) \
                    .withColumn("ccCollection_currentBalance",sf.split("ccCollection","\^")[5]) \
                    .withColumn("ccCollection_dateClosed_content",sf.split("ccCollection","\^")[6]) \
                    .withColumn("ccCollection_dateClosed_estimatedCentury",sf.split("ccCollection","\^")[7]) \
                    .withColumn("ccCollection_dateClosed_estimatedDay",sf.split("ccCollection","\^")[8]) \
                    .withColumn("ccCollection_dateClosed_estimatedMonth",sf.split("ccCollection","\^")[9]) \
                    .withColumn("ccCollection_dateClosed_estimatedYear",sf.split("ccCollection","\^")[10]) \
                    .withColumn("ccCollection_dateEffective_content",sf.split("ccCollection","\^")[11]) \
                    .withColumn("ccCollection_dateEffective_estimatedCentury",sf.split("ccCollection","\^")[12]) \
                    .withColumn("ccCollection_dateEffective_estimatedDay",sf.split("ccCollection","\^")[13]) \
                    .withColumn("ccCollection_dateEffective_estimatedMonth",sf.split("ccCollection","\^")[14]) \
                    .withColumn("ccCollection_dateEffective_estimatedYear",sf.split("ccCollection","\^")[15]) \
                    .withColumn("ccCollection_dateOpened_content",sf.split("ccCollection","\^")[16]) \
                    .withColumn("ccCollection_dateOpened_estimatedCentury",sf.split("ccCollection","\^")[17]) \
                    .withColumn("ccCollection_dateOpened_estimatedDay",sf.split("ccCollection","\^")[18]) \
                    .withColumn("ccCollection_dateOpened_estimatedMonth",sf.split("ccCollection","\^")[19]) \
                    .withColumn("ccCollection_dateOpened_estimatedYear",sf.split("ccCollection","\^")[20]) \
                    .withColumn("ccCollection_datePaidOut_content",sf.split("ccCollection","\^")[21]) \
                    .withColumn("ccCollection_datePaidOut_estimatedCentury",sf.split("ccCollection","\^")[22]) \
                    .withColumn("ccCollection_datePaidOut_estimatedDay",sf.split("ccCollection","\^")[23]) \
                    .withColumn("ccCollection_datePaidOut_estimatedMonth",sf.split("ccCollection","\^")[24]) \
                    .withColumn("ccCollection_datePaidOut_estimatedYear",sf.split("ccCollection","\^")[25]) \
                    .withColumn("ccCollection_mostRecentPayment_date_content",sf.split("ccCollection","\^")[26]) \
                    .withColumn("ccCollection_mostRecentPayment_date_estimatedCentury",sf.split("ccCollection","\^")[27]) \
                    .withColumn("ccCollection_mostRecentPayment_date_estimatedDay",sf.split("ccCollection","\^")[28]) \
                    .withColumn("ccCollection_mostRecentPayment_date_estimatedMonth",sf.split("ccCollection","\^")[29]) \
                    .withColumn("ccCollection_mostRecentPayment_date_estimatedYear",sf.split("ccCollection","\^")[30]) \
                    .withColumn("ccCollection_original_balance",sf.split("ccCollection","\^")[31]) \
                    .withColumn("ccCollection_original_creditGrantor_unparsed",sf.split("ccCollection","\^")[32]) \
                    .withColumn("ccCollection_original_creditorClassification",sf.split("ccCollection","\^")[33]) \
                    .withColumn("ccCollection_pastDue",sf.split("ccCollection","\^")[34]) \
                    .withColumn("ccCollection_portfolioType",sf.split("ccCollection","\^")[35]) \
                    .withColumn("ccCollection_remark_code",sf.split("ccCollection","\^")[36]) \
                    .withColumn("ccCollection_remark_type",sf.split("ccCollection","\^")[37]) \
                    .withColumn("ccCollection_subscriber_industryCode",sf.split("ccCollection","\^")[38]) \
                    .withColumn("ccCollection_subscriber_memberCode",sf.split("ccCollection","\^")[39]) \
                    .withColumn("ccCollection_subscriber_name_unparsed",sf.split("ccCollection","\^")[40]) \
                    .withColumn("ccCollection_updateMethod",sf.split("ccCollection","\^")[41]) \
                    .drop("ccCollection")

#dfsplitCol.show(10, False)

# dfccCollectionStr = dfccCollectionStr.withColumn("ccCollection_datePaidOut_content", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_datePaidOut_estimatedCentury", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_datePaidOut_estimatedDay", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_datePaidOut_estimatedMonth", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_datePaidOut_estimatedYear", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_mostRecentPayment_date_content", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_mostRecentPayment_date_estimatedCentury", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_mostRecentPayment_date_estimatedDay", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_mostRecentPayment_date_estimatedMonth", sf.lit(None).cast(StringType())) \
                    # .withColumn("ccCollection_mostRecentPayment_date_estimatedYear", sf.lit(None).cast(StringType())) \

dfccCollectionStrCol = dfccCollectionStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("ccCollection_ECOADesignator")
                            ,sf.col("ccCollection_accountNumber")
                            ,sf.col("ccCollection_accountRating")
                            ,sf.col("ccCollection_account_type")
                            ,sf.col("ccCollection_closedIndicator")
                            ,sf.col("ccCollection_currentBalance")
                            ,sf.col("ccCollection_dateClosed_content")
                            ,sf.col("ccCollection_dateClosed_estimatedCentury")
                            ,sf.col("ccCollection_dateClosed_estimatedDay")
                            ,sf.col("ccCollection_dateClosed_estimatedMonth")
                            ,sf.col("ccCollection_dateClosed_estimatedYear")
                            ,sf.col("ccCollection_dateEffective_content")
                            ,sf.col("ccCollection_dateEffective_estimatedCentury")
                            ,sf.col("ccCollection_dateEffective_estimatedDay")
                            ,sf.col("ccCollection_dateEffective_estimatedMonth")
                            ,sf.col("ccCollection_dateEffective_estimatedYear")
                            ,sf.col("ccCollection_dateOpened_content")
                            ,sf.col("ccCollection_dateOpened_estimatedCentury")
                            ,sf.col("ccCollection_dateOpened_estimatedDay")
                            ,sf.col("ccCollection_dateOpened_estimatedMonth")
                            ,sf.col("ccCollection_dateOpened_estimatedYear")
                            ,sf.col("ccCollection_datePaidOut_content")
                            ,sf.col("ccCollection_datePaidOut_estimatedCentury")
                            ,sf.col("ccCollection_datePaidOut_estimatedDay")
                            ,sf.col("ccCollection_datePaidOut_estimatedMonth")
                            ,sf.col("ccCollection_datePaidOut_estimatedYear")
                            ,sf.col("ccCollection_mostRecentPayment_date_content")
                            ,sf.col("ccCollection_mostRecentPayment_date_estimatedCentury")
                            ,sf.col("ccCollection_mostRecentPayment_date_estimatedDay")
                            ,sf.col("ccCollection_mostRecentPayment_date_estimatedMonth")
                            ,sf.col("ccCollection_mostRecentPayment_date_estimatedYear")
                            ,sf.col("ccCollection_original_balance")
                            ,sf.col("ccCollection_original_creditGrantor_unparsed")
                            ,sf.col("ccCollection_original_creditorClassification")
                            ,sf.col("ccCollection_pastDue")
                            ,sf.col("ccCollection_portfolioType")
                            ,sf.col("ccCollection_remark_code")
                            ,sf.col("ccCollection_remark_type")
                            ,sf.col("ccCollection_subscriber_industryCode")
                            ,sf.col("ccCollection_subscriber_memberCode")
                            ,sf.col("ccCollection_subscriber_name_unparsed")
                            ,sf.col("ccCollection_updateMethod")
                            )

dfsplitColFinal = dfsplitCol.union(dfccCollectionStrCol)

#dfsplitColFinal.show(100, False)

dfccCollection = dfsplitColFinal.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("ccCollection_ECOADesignator")=="$$$",None).otherwise(sf.col("ccCollection_ECOADesignator")).alias("ECOADesignator"), \
                sf.when(sf.col("ccCollection_accountNumber")=="$$$",None).otherwise(sf.col("ccCollection_accountNumber")).alias("accountNumber"), \
                sf.when(sf.col("ccCollection_accountRating")=="$$$",None).otherwise(sf.col("ccCollection_accountRating")).alias("accountRating"), \
                sf.when(sf.col("ccCollection_account_type")=="$$$",None).otherwise(sf.col("ccCollection_account_type")).alias("account_type"), \
                sf.when(sf.col("ccCollection_closedIndicator")=="$$$",None).otherwise(sf.col("ccCollection_closedIndicator")).alias("closedIndicator"), \
                sf.when(sf.col("ccCollection_currentBalance")=="$$$",None).otherwise(sf.col("ccCollection_currentBalance")).alias("currentBalance"), \
                sf.when(sf.col("ccCollection_dateClosed_content")=="$$$",None).otherwise(sf.col("ccCollection_dateClosed_content")).alias("dateClosed_content"), \
                sf.when(sf.col("ccCollection_dateClosed_estimatedCentury")=="$$$",None).otherwise(sf.col("ccCollection_dateClosed_estimatedCentury")).alias("dateClosed_estimatedCentury"), \
                sf.when(sf.col("ccCollection_dateClosed_estimatedDay")=="$$$",None).otherwise(sf.col("ccCollection_dateClosed_estimatedDay")).alias("dateClosed_estimatedDay"), \
                sf.when(sf.col("ccCollection_dateClosed_estimatedMonth")=="$$$",None).otherwise(sf.col("ccCollection_dateClosed_estimatedMonth")).alias("dateClosed_estimatedMonth"), \
                sf.when(sf.col("ccCollection_dateClosed_estimatedYear")=="$$$",None).otherwise(sf.col("ccCollection_dateClosed_estimatedYear")).alias("dateClosed_estimatedYear"), \
                sf.when(sf.col("ccCollection_dateEffective_content")=="$$$",None).otherwise(sf.col("ccCollection_dateEffective_content")).alias("dateEffective_content"), \
                sf.when(sf.col("ccCollection_dateEffective_estimatedCentury")=="$$$",None).otherwise(sf.col("ccCollection_dateEffective_estimatedCentury")).alias("dateEffective_estimatedCentury"), \
                sf.when(sf.col("ccCollection_dateEffective_estimatedDay")=="$$$",None).otherwise(sf.col("ccCollection_dateEffective_estimatedDay")).alias("dateEffective_estimatedDay"), \
                sf.when(sf.col("ccCollection_dateEffective_estimatedMonth")=="$$$",None).otherwise(sf.col("ccCollection_dateEffective_estimatedMonth")).alias("dateEffective_estimatedMonth"), \
                sf.when(sf.col("ccCollection_dateEffective_estimatedYear")=="$$$",None).otherwise(sf.col("ccCollection_dateEffective_estimatedYear")).alias("dateEffective_estimatedYear"), \
                sf.when(sf.col("ccCollection_dateOpened_content")=="$$$",None).otherwise(sf.col("ccCollection_dateOpened_content")).alias("dateOpened_content"), \
                sf.when(sf.col("ccCollection_dateOpened_estimatedCentury")=="$$$",None).otherwise(sf.col("ccCollection_dateOpened_estimatedCentury")).alias("dateOpened_estimatedCentury"), \
                sf.when(sf.col("ccCollection_dateOpened_estimatedDay")=="$$$",None).otherwise(sf.col("ccCollection_dateOpened_estimatedDay")).alias("dateOpened_estimatedDay"), \
                sf.when(sf.col("ccCollection_dateOpened_estimatedMonth")=="$$$",None).otherwise(sf.col("ccCollection_dateOpened_estimatedMonth")).alias("dateOpened_estimatedMonth"), \
                sf.when(sf.col("ccCollection_dateOpened_estimatedYear")=="$$$",None).otherwise(sf.col("ccCollection_dateOpened_estimatedYear")).alias("dateOpened_estimatedYear"), \
                sf.when(sf.col("ccCollection_datePaidOut_content")=="$$$",None).otherwise(sf.col("ccCollection_datePaidOut_content")).alias("datePaidOut_content"), \
                sf.when(sf.col("ccCollection_datePaidOut_estimatedCentury")=="$$$",None).otherwise(sf.col("ccCollection_datePaidOut_estimatedCentury")).alias("datePaidOut_estimatedCentury"), \
                sf.when(sf.col("ccCollection_datePaidOut_estimatedDay")=="$$$",None).otherwise(sf.col("ccCollection_datePaidOut_estimatedDay")).alias("datePaidOut_estimatedDay"), \
                sf.when(sf.col("ccCollection_datePaidOut_estimatedMonth")=="$$$",None).otherwise(sf.col("ccCollection_datePaidOut_estimatedMonth")).alias("datePaidOut_estimatedMonth"), \
                sf.when(sf.col("ccCollection_datePaidOut_estimatedYear")=="$$$",None).otherwise(sf.col("ccCollection_datePaidOut_estimatedYear")).alias("datePaidOut_estimatedYear"), \
                sf.when(sf.col("ccCollection_mostRecentPayment_date_content")=="$$$",None).otherwise(sf.col("ccCollection_mostRecentPayment_date_content")).alias("mostRecentPayment_date_content"), \
                sf.when(sf.col("ccCollection_mostRecentPayment_date_estimatedCentury")=="$$$",None).otherwise(sf.col("ccCollection_mostRecentPayment_date_estimatedCentury")).alias("mostRecentPayment_date_estimatedCentury"), \
                sf.when(sf.col("ccCollection_mostRecentPayment_date_estimatedDay")=="$$$",None).otherwise(sf.col("ccCollection_mostRecentPayment_date_estimatedDay")).alias("mostRecentPayment_date_estimatedDay"), \
                sf.when(sf.col("ccCollection_mostRecentPayment_date_estimatedMonth")=="$$$",None).otherwise(sf.col("ccCollection_mostRecentPayment_date_estimatedMonth")).alias("mostRecentPayment_date_estimatedMonth"), \
                sf.when(sf.col("ccCollection_mostRecentPayment_date_estimatedYear")=="$$$",None).otherwise(sf.col("ccCollection_mostRecentPayment_date_estimatedYear")).alias("mostRecentPayment_date_estimatedYear"), \
                sf.when(sf.col("ccCollection_original_balance")=="$$$",None).otherwise(sf.col("ccCollection_original_balance")).alias("original_balance"), \
                sf.when(sf.col("ccCollection_original_creditGrantor_unparsed")=="$$$",None).otherwise(sf.col("ccCollection_original_creditGrantor_unparsed")).alias("original_creditGrantor_unparsed"), \
                sf.when(sf.col("ccCollection_original_creditorClassification")=="$$$",None).otherwise(sf.col("ccCollection_original_creditorClassification")).alias("original_creditorClassification"), \
                sf.when(sf.col("ccCollection_pastDue")=="$$$",None).otherwise(sf.col("ccCollection_pastDue")).alias("pastDue"), \
                sf.when(sf.col("ccCollection_portfolioType")=="$$$",None).otherwise(sf.col("ccCollection_portfolioType")).alias("portfolioType"), \
                sf.when(sf.col("ccCollection_remark_code")=="$$$",None).otherwise(sf.col("ccCollection_remark_code")).alias("remark_code"), \
                sf.when(sf.col("ccCollection_remark_type")=="$$$",None).otherwise(sf.col("ccCollection_remark_type")).alias("remark_type"), \
                sf.when(sf.col("ccCollection_subscriber_industryCode")=="$$$",None).otherwise(sf.col("ccCollection_subscriber_industryCode")).alias("subscriber_industryCode"), \
                sf.when(sf.col("ccCollection_subscriber_memberCode")=="$$$",None).otherwise(sf.col("ccCollection_subscriber_memberCode")).alias("subscriber_memberCode"), \
                sf.when(sf.col("ccCollection_subscriber_name_unparsed")=="$$$",None).otherwise(sf.col("ccCollection_subscriber_name_unparsed")).alias("subscriber_name_unparsed"), \
                sf.when(sf.col("ccCollection_updateMethod")=="$$$",None).otherwise(sf.col("ccCollection_updateMethod")).alias("updateMethod")) \
                .where(sf.col("ECOADesignator").isNotNull() | \
                sf.col("accountNumber").isNotNull() | \
                sf.col("accountRating").isNotNull() | \
                sf.col("account_type").isNotNull() | \
                sf.col("closedIndicator").isNotNull() | \
                sf.col("currentBalance").isNotNull() | \
                sf.col("dateClosed_content").isNotNull() | \
                sf.col("dateClosed_estimatedCentury").isNotNull() | \
                sf.col("dateClosed_estimatedDay").isNotNull() | \
                sf.col("dateClosed_estimatedMonth").isNotNull() | \
                sf.col("dateClosed_estimatedYear").isNotNull() | \
                sf.col("dateEffective_content").isNotNull() | \
                sf.col("dateEffective_estimatedCentury").isNotNull() | \
                sf.col("dateEffective_estimatedDay").isNotNull() | \
                sf.col("dateEffective_estimatedMonth").isNotNull() | \
                sf.col("dateEffective_estimatedYear").isNotNull() | \
                sf.col("dateOpened_content").isNotNull() | \
                sf.col("dateOpened_estimatedCentury").isNotNull() | \
                sf.col("dateOpened_estimatedDay").isNotNull() | \
                sf.col("dateOpened_estimatedMonth").isNotNull() | \
                sf.col("dateOpened_estimatedYear").isNotNull() | \
                sf.col("datePaidOut_content").isNotNull() | \
                sf.col("datePaidOut_estimatedCentury").isNotNull() | \
                sf.col("datePaidOut_estimatedDay").isNotNull() | \
                sf.col("datePaidOut_estimatedMonth").isNotNull() | \
                sf.col("datePaidOut_estimatedYear").isNotNull() | \
                sf.col("mostRecentPayment_date_content").isNotNull() | \
                sf.col("mostRecentPayment_date_estimatedCentury").isNotNull() | \
                sf.col("mostRecentPayment_date_estimatedDay").isNotNull() | \
                sf.col("mostRecentPayment_date_estimatedMonth").isNotNull() | \
                sf.col("mostRecentPayment_date_estimatedYear").isNotNull() | \
                sf.col("original_balance").isNotNull() | \
                sf.col("original_creditGrantor_unparsed").isNotNull() | \
                sf.col("original_creditorClassification").isNotNull() | \
                sf.col("pastDue").isNotNull() | \
                sf.col("portfolioType").isNotNull() | \
                sf.col("remark_code").isNotNull() | \
                sf.col("remark_type").isNotNull() | \
                sf.col("subscriber_industryCode").isNotNull() | \
                sf.col("subscriber_memberCode").isNotNull() | \
                sf.col("subscriber_name_unparsed").isNotNull() | \
                sf.col("updateMethod").isNotNull() \
                )

dfccCollection.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)