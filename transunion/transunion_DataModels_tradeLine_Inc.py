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
appNameSuffix = vendor + "_DataModels_tradeLine"

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

srcfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/Stage/Parquet/tradeLine/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/tradeLine/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfccTrade = df.withColumn("paymentHistory_historicalCounters_late30DaysTotal", sf.lit(None).cast(StringType())) \
                    # .withColumn("paymentHistory_historicalCounters_late60DaysTotal", sf.lit(None).cast(StringType())) \
                    # .withColumn("paymentHistory_historicalCounters_late90DaysTotal", sf.lit(None).cast(StringType())) \

dfccTrade = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("ECOADesignator")
                            ,sf.col("accountNumber")
                            ,sf.col("accountRating")
                            ,sf.col("account_type")
                            ,sf.col("additionalTradeAccount_original_creditGrantor_person")
                            ,sf.col("additionalTradeAccount_original_creditGrantor_person_unparsed")
                            ,sf.col("additionalTradeAccount_payment")
                            ,sf.col("additionalTradeAccount_payment_dateDue_content")
                            ,sf.col("additionalTradeAccount_payment_dateDue_estimatedCentury")
                            ,sf.col("additionalTradeAccount_payment_dateDue_estimatedDay")
                            ,sf.col("additionalTradeAccount_payment_dateDue_estimatedMonth")
                            ,sf.col("additionalTradeAccount_payment_dateDue_estimatedYear")
                            ,sf.col("additionalTradeAccount_payment_dueAmount")
                            ,sf.col("additionalTradeAccount_payment_specializedType")
                            ,sf.col("additionalTradeAccount_portfolio_name")
                            ,sf.col("additionalTradeAccount_portfolio_name_unparsed")
                            ,sf.col("additionalTradeAccount_portfolio_saleIndicator")
                            ,sf.col("closedIndicator")
                            ,sf.col("creditLimit")
                            ,sf.col("currentBalance")
                            ,sf.col("dateClosed_content")
                            ,sf.col("dateClosed_estimatedCentury")
                            ,sf.col("dateClosed_estimatedDay")
                            ,sf.col("dateClosed_estimatedMonth")
                            ,sf.col("dateClosed_estimatedYear")
                            ,sf.col("dateEffective_content")
                            ,sf.col("dateEffective_estimatedCentury")
                            ,sf.col("dateEffective_estimatedDay")
                            ,sf.col("dateEffective_estimatedMonth")
                            ,sf.col("dateEffective_estimatedYear")
                            ,sf.col("dateOpened_content")
                            ,sf.col("dateOpened_estimatedCentury")
                            ,sf.col("dateOpened_estimatedDay")
                            ,sf.col("dateOpened_estimatedMonth")
                            ,sf.col("dateOpened_estimatedYear")
                            ,sf.col("datePaidOut_content")
                            ,sf.col("datePaidOut_estimatedCentury")
                            ,sf.col("datePaidOut_estimatedDay")
                            ,sf.col("datePaidOut_estimatedMonth")
                            ,sf.col("datePaidOut_estimatedYear")
                            ,sf.col("highCredit")
                            ,sf.col("mostRecentPayment_date_content")
                            ,sf.col("mostRecentPayment_date_estimatedCentury")
                            ,sf.col("mostRecentPayment_date_estimatedDay")
                            ,sf.col("mostRecentPayment_date_estimatedMonth")
                            ,sf.col("mostRecentPayment_date_estimatedYear")
                            ,sf.col("pastDue")
                            ,sf.col("paymentHistory")
                            ,sf.col("paymentHistory_historicalCounters_late30DaysTotal")
                            ,sf.col("paymentHistory_historicalCounters_late60DaysTotal")
                            ,sf.col("paymentHistory_historicalCounters_late90DaysTotal")
                            ,sf.col("paymentHistory_historicalCounters_monthsReviewedCount")
                            ,sf.col("paymentHistory_maxDelinquency_accountRating")
                            ,sf.col("paymentHistory_maxDelinquency_amount")
                            ,sf.col("paymentHistory_maxDelinquency_date_content")
                            ,sf.col("paymentHistory_maxDelinquency_date_estimatedCentury")
                            ,sf.col("paymentHistory_maxDelinquency_date_estimatedDay")
                            ,sf.col("paymentHistory_maxDelinquency_date_estimatedMonth")
                            ,sf.col("paymentHistory_maxDelinquency_date_estimatedYear")
                            ,sf.col("paymentHistory_maxDelinquency_earliest")
                            ,sf.col("paymentHistory_paymentPattern_startDate_content")
                            ,sf.col("paymentHistory_paymentPattern_startDate_estimatedCentury")
                            ,sf.col("paymentHistory_paymentPattern_startDate_estimatedDay")
                            ,sf.col("paymentHistory_paymentPattern_startDate_estimatedMonth")
                            ,sf.col("paymentHistory_paymentPattern_startDate_estimatedYear")
                            ,sf.col("paymentHistory_paymentPattern_text")
                            ,sf.col("portfolioType")
                            ,sf.col("remark_code")
                            ,sf.col("remark_type")
                            ,sf.col("subscriber_industryCode")
                            ,sf.col("subscriber_memberCode")
                            ,sf.col("subscriber_name_unparsed")
                            ,sf.col("terms_paymentFrequency")
                            ,sf.col("terms_paymentScheduleMonthCount")
                            ,sf.col("terms_scheduledMonthlyPayment")
                            ,sf.col("updateMethod")
                            ).distinct()

dfccTrade.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)