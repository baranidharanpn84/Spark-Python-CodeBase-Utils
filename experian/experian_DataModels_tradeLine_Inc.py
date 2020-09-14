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

dfcsTradeLine = df.withColumn('AccountType', sf.lit(None).cast(StringType())) \
                            .withColumn('Amount_Qualifier', sf.lit(None).cast(StringType())) \
                            .withColumn('Amount_Qualifier_1', sf.lit(None).cast(StringType())) \
                            .withColumn('Balloon', sf.lit(None).cast(StringType())) \
                            .withColumn('DisputeFlag', sf.lit(None).cast(StringType())) \
                            .withColumn('ECOA', sf.lit(None).cast(StringType())) \
                            .withColumn('EnhancedPaymentData_AccountCondition', sf.lit(None).cast(StringType())) \
                            .withColumn('EnhancedPaymentData_AccountType', sf.lit(None).cast(StringType())) \
                            .withColumn('EnhancedPaymentData_PaymentStatus', sf.lit(None).cast(StringType())) \
                            .withColumn('EnhancedPaymentData_SpecialComment', sf.lit(None).cast(StringType())) \
                            .withColumn('Evaluation', sf.lit(None).cast(StringType())) \
                            .withColumn('KOB', sf.lit(None).cast(StringType())) \
                            .withColumn('MaxPayment', sf.lit(None).cast(StringType())) \
                            .withColumn('MonthlyPaymentType', sf.lit(None).cast(StringType())) \
                            .withColumn('OpenOrClosed', sf.lit(None).cast(StringType())) \
                            .withColumn('RevolvingOrInstallment', sf.lit(None).cast(StringType())) \
                            .withColumn('SpecialComment', sf.lit(None).cast(StringType())) \
                            .withColumn('Status', sf.lit(None).cast(StringType())) \
                            .withColumn('TermsDuration', sf.lit(None).cast(StringType()))

dfcsTradeLine = dfcsTradeLine.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("AccountNumber")
                            ,sf.col("AccountType")
                            ,sf.col("AccountType_code")
                            ,sf.col("AmountPastDue")
                            ,sf.col("Amount_Qualifier")
                            ,sf.col("Amount_Qualifier_1")
                            ,sf.col("Amount_1_Qualifier_code").alias("Amount_Qualifier_code")
                            ,sf.col("Amount_2_Qualifier_code").alias("Amount_Qualifier_1_code")
                            ,sf.col("Amount_1_Value").alias("Amount_Value")
                            ,sf.col("Amount_2_Value").alias("Amount_Value_1")
                            ,sf.col("BalanceAmount")
                            ,sf.col("BalanceDate")
                            ,sf.col("Balloon")
                            ,sf.col("Balloon_amount")
                            ,sf.col("Balloon_dueDate")
                            ,sf.col("Collateral")
                            ,sf.col("ConsumerComment")
                            ,sf.col("DelinquenciesOver30Days")
                            ,sf.col("DelinquenciesOver60Days")
                            ,sf.col("DelinquenciesOver90Days")
                            ,sf.col("DerogCounter")
                            ,sf.col("DisputeFlag")
                            ,sf.col("ECOA")
                            ,sf.col("ECOA_code")
                            ,sf.col("EnhancedPaymentData_AccountCondition")
                            ,sf.col("EnhancedPaymentData_AccountCondition_code")
                            ,sf.col("EnhancedPaymentData_AccountType")
                            ,sf.col("EnhancedPaymentData_AccountType_code")
                            ,sf.col("EnhancedPaymentData_InitialPaymentLevelDate")
                            ,sf.col("EnhancedPaymentData_PaymentStatus")
                            ,sf.col("EnhancedPaymentData_PaymentStatus_code")
                            ,sf.col("EnhancedPaymentData_SpecialComment")
                            ,sf.col("EnhancedPaymentData_SpecialComment_code")
                            ,sf.col("Evaluation")
                            ,sf.col("Evaluation_code")
                            ,sf.col("FirstDelinquencyDate")
                            ,sf.col("KOB")
                            ,sf.col("KOB_code")
                            ,sf.col("LastPaymentDate")
                            ,sf.col("MaxDelinquencyDate")
                            ,sf.col("MaxPayment")
                            ,sf.col("MaxPayment_code")
                            ,sf.col("MonthlyPaymentAmount")
                            ,sf.col("MonthlyPaymentType")
                            ,sf.col("MonthlyPaymentType_code")
                            ,sf.col("MonthsHistory")
                            ,sf.col("OpenDate")
                            ,sf.col("OpenOrClosed")
                            ,sf.col("OpenOrClosed_code")
                            ,sf.col("OriginalCreditorName")
                            ,sf.col("PaymentProfile")
                            ,sf.col("RevolvingOrInstallment")
                            ,sf.col("RevolvingOrInstallment_code")
                            ,sf.col("SecondDelinquencyDate")
                            ,sf.col("SoldToName")
                            ,sf.col("SpecialComment")
                            ,sf.col("SpecialComment_code")
                            ,sf.col("Status")
                            ,sf.col("StatusDate")
                            ,sf.col("Status_code")
                            ,sf.col("Subcode")
                            ,sf.col("SubscriberDisplayName")
                            ,sf.col("TermsDuration")
                            ,sf.col("TermsDuration_code")
                            ) \
                            .distinct()

dfcsTradeLine.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)