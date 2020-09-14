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
appNameSuffix = vendor + "_DataModels_empInfo"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/empInfo/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfcsEmpInfo = df.select(df.colRegex("`csEmpInfo_[a-zA-Z0-9]+_\w*`"))

# dfcsEmpInfo.printSchema()

dfcsEmpInfoInt = df.select(df.colRegex("`csEmpInfo_[0-9]+_\w*`"))

dfcsEmpInfoStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csEmpInfo_[a-zA-Z_]*`"))

#dfcsEmpInfoStr.show()

EmpInfoCnt = sorted([int(col.split("_")[1]) for col in dfcsEmpInfoInt.columns if col.split("_")[0] == "csEmpInfo" \
                        and col.split("_")[2] in ["AddressExtraLine","AddressFirstLine","AddressSecondLine","FirstReportedDate","LastUpdatedDate","Name" \
                                            ,"Origination","Origination_code","Zip"]])[-1]

#print(EmpInfoCnt)

for i in range(EmpInfoCnt):
    rule = "csEmpInfo_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_AddressExtraLine"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_AddressFirstLine"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_AddressSecondLine"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_FirstReportedDate"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_LastUpdatedDate"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Origination"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Origination_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Zip"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#df.show(2,False)

df = df.withColumn( "csEmpInfoArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsEmpInfo"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csEmpInfoArray").alias("csEmpInfo") \
            )

dfsplitCol = dfexplode.withColumn("csEmpInfo_AddressExtraLine",sf.split("csEmpInfo","\^")[0]) \
                     .withColumn("csEmpInfo_AddressFirstLine",sf.split("csEmpInfo","\^")[1]) \
                     .withColumn("csEmpInfo_AddressSecondLine",sf.split("csEmpInfo","\^")[2]) \
                     .withColumn("csEmpInfo_FirstReportedDate",sf.split("csEmpInfo","\^")[3]) \
                     .withColumn("csEmpInfo_LastUpdatedDate",sf.split("csEmpInfo","\^")[4]) \
                     .withColumn("csEmpInfo_Name",sf.split("csEmpInfo","\^")[5]) \
                     .withColumn("csEmpInfo_Origination",sf.lit(None).cast(StringType())) \
                     .withColumn("csEmpInfo_Origination_code",sf.split("csEmpInfo","\^")[6]) \
                     .withColumn("csEmpInfo_Zip",sf.split("csEmpInfo","\^")[7]) \
                     .drop("csEmpInfo")

#dfsplitCol.show(10, False)

dfcsEmpInfoStr = dfcsEmpInfoStr.withColumn('csEmpInfo_Origination', sf.lit(None).cast(StringType()))

dfcsEmpInfoStrCol = dfcsEmpInfoStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csEmpInfo_AddressExtraLine")
                            ,sf.col("csEmpInfo_AddressFirstLine")
                            ,sf.col("csEmpInfo_AddressSecondLine")
                            ,sf.col("csEmpInfo_FirstReportedDate")
                            ,sf.col("csEmpInfo_LastUpdatedDate")
                            ,sf.col("csEmpInfo_Name")
                            ,sf.col("csEmpInfo_Origination")
                            ,sf.col("csEmpInfo_Origination_code")
                            ,sf.col("csEmpInfo_Zip")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsEmpInfoStrCol)

#dfsplitColFinal.show(1, False)

dfcsEmpInfo = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csEmpInfo_AddressExtraLine")=="$$$",None).otherwise(sf.col("csEmpInfo_AddressExtraLine")).alias("AddressExtraLine"), \
                sf.when(sf.col("csEmpInfo_AddressFirstLine")=="$$$",None).otherwise(sf.col("csEmpInfo_AddressFirstLine")).alias("AddressFirstLine"), \
                sf.when(sf.col("csEmpInfo_AddressSecondLine")=="$$$",None).otherwise(sf.col("csEmpInfo_AddressSecondLine")).alias("AddressSecondLine"), \
                sf.when(sf.col("csEmpInfo_FirstReportedDate")=="$$$",None).otherwise(sf.col("csEmpInfo_FirstReportedDate")).alias("FirstReportedDate"), \
                sf.when(sf.col("csEmpInfo_LastUpdatedDate")=="$$$",None).otherwise(sf.col("csEmpInfo_LastUpdatedDate")).alias("LastUpdatedDate"), \
                sf.when(sf.col("csEmpInfo_Name")=="$$$",None).otherwise(sf.col("csEmpInfo_Name")).alias("Name"), \
                sf.when(sf.col("csEmpInfo_Origination")=="$$$",None).otherwise(sf.col("csEmpInfo_Origination")).alias("Origination"), \
                sf.when(sf.col("csEmpInfo_Origination_code")=="$$$",None).otherwise(sf.col("csEmpInfo_Origination_code")).alias("Origination_code"), \
                sf.when(sf.col("csEmpInfo_Zip")=="$$$",None).otherwise(sf.col("csEmpInfo_Zip")).alias("Zip")) \
                .where(sf.col("AddressExtraLine").isNotNull() | \
                sf.col("AddressFirstLine").isNotNull() | \
                sf.col("AddressSecondLine").isNotNull() | \
                sf.col("FirstReportedDate").isNotNull() | \
                sf.col("LastUpdatedDate").isNotNull() | \
                sf.col("Name").isNotNull() | \
                sf.col("Origination").isNotNull() | \
                sf.col("Origination_code").isNotNull() | \
                sf.col("Zip").isNotNull() \
                )

dfcsEmpInfo.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)