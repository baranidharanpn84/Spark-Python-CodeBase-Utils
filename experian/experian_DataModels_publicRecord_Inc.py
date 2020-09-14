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
appNameSuffix = vendor + "_DataModels_publicRecord"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/publicRecord/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData.show(10,False)

dfcsPublicRecord = df.select(df.colRegex("`csPublicRecord_[a-zA-Z0-9]+_\w*`"))

# dfcsPublicRecord.printSchema()

dfcsPublicRecordInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csPublicRecord_[0-9]+_\w*`"))

dfcsPublicRecordStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csPublicRecord_[a-zA-Z_]*`"))

#dfcsPublicRecordStr.show()

PublicRecordCnt = sorted([int(col.split("_")[1]) for col in dfcsPublicRecordInt.columns if col.split("_")[0] == "csPublicRecord" \
                        and col.split("_")[2] in ["Amount","Bankruptcy_AdjustmentPercent","Bankruptcy_AssetAmount","Bankruptcy_LiabilitiesAmount","Bankruptcy_RepaymentPercent" \
                                            ,"Bankruptcy_Type","Bankruptcy_Type_code","BookPageSequence","ConsumerComment","Court","Court_code","Court_name","DisputeFlag","ECOA" \
                                            ,"ECOA_code","Evaluation","Evaluation_code","FilingDate","PlaintiffName","ReferenceNumber","Status","StatusDate","Status_code"]])[-1]

#print(PublicRecordCnt)

for col in dfcsPublicRecordInt.columns:
    instr = col.find('_', 16, 19)
    for i in range(PublicRecordCnt):
        rule = "csPublicRecord_"+str(i+1)
        if not "csPublicRecord_"+str(i+1)+"_"+col[instr+1:] in dfcsPublicRecordInt.columns:
            dfcsPublicRecordInt = dfcsPublicRecordInt.withColumn("csPublicRecord_"+str(i+1)+"_"+col[instr+1:], sf.lit(None))

for i in range(PublicRecordCnt):
    rule = "csPublicRecord_"+str(i+1)
    dfcsPublicRecordInt = dfcsPublicRecordInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_Amount"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_Bankruptcy_AdjustmentPercent"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Bankruptcy_AssetAmount"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Bankruptcy_LiabilitiesAmount"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Bankruptcy_RepaymentPercent"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Bankruptcy_Type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Bankruptcy_Type_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_BookPageSequence"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ConsumerComment"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Court"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Court_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Court_name"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_DisputeFlag"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_ECOA"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ECOA_code"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Evaluation"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Evaluation_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_FilingDate"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_PlaintiffName"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ReferenceNumber"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Status"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_StatusDate"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Status_code"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#dfcsPublicRecordInt.show(2,False)

dfcsPublicRecordInt = dfcsPublicRecordInt.withColumn( "csPublicRecordArray", sf.array([col for col in dfcsPublicRecordInt.columns if col.split("_")[0] == "newcsPublicRecord"]) )

dfexplode = dfcsPublicRecordInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csPublicRecordArray").alias("csPublicRecord") \
            )

dfsplitCol = dfexplode.withColumn("csPublicRecord_Amount",sf.split("csPublicRecord","\^")[0]) \
                     .withColumn("csPublicRecord_Bankruptcy_AdjustmentPercent",sf.split("csPublicRecord","\^")[1]) \
                     .withColumn("csPublicRecord_Bankruptcy_AssetAmount",sf.split("csPublicRecord","\^")[2]) \
                     .withColumn("csPublicRecord_Bankruptcy_LiabilitiesAmount",sf.split("csPublicRecord","\^")[3]) \
                     .withColumn("csPublicRecord_Bankruptcy_RepaymentPercent",sf.split("csPublicRecord","\^")[4]) \
                     .withColumn("csPublicRecord_Bankruptcy_Type", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_Bankruptcy_Type_code",sf.split("csPublicRecord","\^")[5]) \
                     .withColumn("csPublicRecord_BookPageSequence",sf.split("csPublicRecord","\^")[6]) \
                     .withColumn("csPublicRecord_ConsumerComment",sf.split("csPublicRecord","\^")[7]) \
                     .withColumn("csPublicRecord_Court", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_Court_code",sf.split("csPublicRecord","\^")[8]) \
                     .withColumn("csPublicRecord_Court_name",sf.split("csPublicRecord","\^")[9]) \
                     .withColumn("csPublicRecord_DisputeFlag", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_ECOA", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_ECOA_code",sf.split("csPublicRecord","\^")[10]) \
                     .withColumn("csPublicRecord_Evaluation", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_Evaluation_code",sf.split("csPublicRecord","\^")[11]) \
                     .withColumn("csPublicRecord_FilingDate",sf.split("csPublicRecord","\^")[12]) \
                     .withColumn("csPublicRecord_PlaintiffName",sf.split("csPublicRecord","\^")[13]) \
                     .withColumn("csPublicRecord_ReferenceNumber",sf.split("csPublicRecord","\^")[14]) \
                     .withColumn("csPublicRecord_Status", sf.lit(None).cast(StringType())) \
                     .withColumn("csPublicRecord_StatusDate",sf.split("csPublicRecord","\^")[15]) \
                     .withColumn("csPublicRecord_Status_code",sf.split("csPublicRecord","\^")[16]) \
                     .drop("csPublicRecord")

#dfsplitCol.show(10, False)

dfcsPublicRecordStr = dfcsPublicRecordStr.withColumn('csPublicRecord_Bankruptcy_Type', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_Bankruptcy_Type_code', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_Court', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_DisputeFlag', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_ECOA', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_Evaluation', sf.lit(None).cast(StringType())) \
                            .withColumn('csPublicRecord_Status', sf.lit(None).cast(StringType())) \

dfcsPublicRecordStrCol = dfcsPublicRecordStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csPublicRecord_Amount")
                            ,sf.col("csPublicRecord_Bankruptcy_AdjustmentPercent")
                            ,sf.col("csPublicRecord_Bankruptcy_AssetAmount")
                            ,sf.col("csPublicRecord_Bankruptcy_LiabilitiesAmount")
                            ,sf.col("csPublicRecord_Bankruptcy_RepaymentPercent")
                            ,sf.col("csPublicRecord_Bankruptcy_Type")
                            ,sf.col("csPublicRecord_Bankruptcy_Type_code")
                            ,sf.col("csPublicRecord_BookPageSequence")
                            ,sf.col("csPublicRecord_ConsumerComment")
                            ,sf.col("csPublicRecord_Court")
                            ,sf.col("csPublicRecord_Court_code")
                            ,sf.col("csPublicRecord_Court_name")
                            ,sf.col("csPublicRecord_DisputeFlag")
                            ,sf.col("csPublicRecord_ECOA")
                            ,sf.col("csPublicRecord_ECOA_code")
                            ,sf.col("csPublicRecord_Evaluation")
                            ,sf.col("csPublicRecord_Evaluation_code")
                            ,sf.col("csPublicRecord_FilingDate")
                            ,sf.col("csPublicRecord_PlaintiffName")
                            ,sf.col("csPublicRecord_ReferenceNumber")
                            ,sf.col("csPublicRecord_Status")
                            ,sf.col("csPublicRecord_StatusDate")
                            ,sf.col("csPublicRecord_Status_code")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsPublicRecordStrCol)

#dfsplitColFinal.show(1, False)

dfcsPublicRecord = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csPublicRecord_Amount")=="$$$",None).otherwise(sf.col("csPublicRecord_Amount")).alias("Amount"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_AdjustmentPercent")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_AdjustmentPercent")).alias("Bankruptcy_AdjustmentPercent"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_AssetAmount")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_AssetAmount")).alias("Bankruptcy_AssetAmount"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_LiabilitiesAmount")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_LiabilitiesAmount")).alias("Bankruptcy_LiabilitiesAmount"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_RepaymentPercent")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_RepaymentPercent")).alias("Bankruptcy_RepaymentPercent"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_Type")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_Type")).alias("Bankruptcy_Type"), \
                sf.when(sf.col("csPublicRecord_Bankruptcy_Type_code")=="$$$",None).otherwise(sf.col("csPublicRecord_Bankruptcy_Type_code")).alias("Bankruptcy_Type_code"), \
                sf.when(sf.col("csPublicRecord_BookPageSequence")=="$$$",None).otherwise(sf.col("csPublicRecord_BookPageSequence")).alias("BookPageSequence"), \
                sf.when(sf.col("csPublicRecord_ConsumerComment")=="$$$",None).otherwise(sf.col("csPublicRecord_ConsumerComment")).alias("ConsumerComment"), \
                sf.when(sf.col("csPublicRecord_Court")=="$$$",None).otherwise(sf.col("csPublicRecord_Court")).alias("Court"), \
                sf.when(sf.col("csPublicRecord_Court_code")=="$$$",None).otherwise(sf.col("csPublicRecord_Court_code")).alias("Court_code"), \
                sf.when(sf.col("csPublicRecord_Court_name")=="$$$",None).otherwise(sf.col("csPublicRecord_Court_name")).alias("Court_name"), \
                sf.when(sf.col("csPublicRecord_DisputeFlag")=="$$$",None).otherwise(sf.col("csPublicRecord_DisputeFlag")).alias("DisputeFlag"), \
                sf.when(sf.col("csPublicRecord_ECOA")=="$$$",None).otherwise(sf.col("csPublicRecord_ECOA")).alias("ECOA"), \
                sf.when(sf.col("csPublicRecord_ECOA_code")=="$$$",None).otherwise(sf.col("csPublicRecord_ECOA_code")).alias("ECOA_code"), \
                sf.when(sf.col("csPublicRecord_Evaluation")=="$$$",None).otherwise(sf.col("csPublicRecord_Evaluation")).alias("Evaluation"), \
                sf.when(sf.col("csPublicRecord_Evaluation_code")=="$$$",None).otherwise(sf.col("csPublicRecord_Evaluation_code")).alias("Evaluation_code"), \
                sf.when(sf.col("csPublicRecord_FilingDate")=="$$$",None).otherwise(sf.col("csPublicRecord_FilingDate")).alias("FilingDate"), \
                sf.when(sf.col("csPublicRecord_PlaintiffName")=="$$$",None).otherwise(sf.col("csPublicRecord_PlaintiffName")).alias("PlaintiffName"), \
                sf.when(sf.col("csPublicRecord_ReferenceNumber")=="$$$",None).otherwise(sf.col("csPublicRecord_ReferenceNumber")).alias("ReferenceNumber"), \
                sf.when(sf.col("csPublicRecord_Status")=="$$$",None).otherwise(sf.col("csPublicRecord_Status")).alias("Status"), \
                sf.when(sf.col("csPublicRecord_StatusDate")=="$$$",None).otherwise(sf.col("csPublicRecord_StatusDate")).alias("StatusDate"), \
                sf.when(sf.col("csPublicRecord_Status_code")=="$$$",None).otherwise(sf.col("csPublicRecord_Status_code")).alias("Status_code")) \
                .where(sf.col("Amount").isNotNull() | \
                sf.col("Bankruptcy_AdjustmentPercent").isNotNull() | \
                sf.col("Bankruptcy_AssetAmount").isNotNull() | \
                sf.col("Bankruptcy_LiabilitiesAmount").isNotNull() | \
                sf.col("Bankruptcy_RepaymentPercent").isNotNull() | \
                sf.col("Bankruptcy_Type").isNotNull() | \
                sf.col("Bankruptcy_Type_code").isNotNull() | \
                sf.col("BookPageSequence").isNotNull() | \
                sf.col("ConsumerComment").isNotNull() | \
                sf.col("Court").isNotNull() | \
                sf.col("Court_code").isNotNull() | \
                sf.col("Court_name").isNotNull() | \
                sf.col("DisputeFlag").isNotNull() | \
                sf.col("ECOA").isNotNull() | \
                sf.col("ECOA_code").isNotNull() | \
                sf.col("Evaluation").isNotNull() | \
                sf.col("Evaluation_code").isNotNull() | \
                sf.col("FilingDate").isNotNull() | \
                sf.col("PlaintiffName").isNotNull() | \
                sf.col("ReferenceNumber").isNotNull() | \
                sf.col("Status").isNotNull() | \
                sf.col("StatusDate").isNotNull() | \
                sf.col("Status_code").isNotNull() \
                )

dfcsPublicRecord.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)