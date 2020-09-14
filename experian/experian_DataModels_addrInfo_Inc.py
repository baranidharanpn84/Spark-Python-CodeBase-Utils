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
appNameSuffix = vendor + "_DataModels_addrInfo"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/addressInfo/"


dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfcsAddrInfo = df.select(df.colRegex("`csAddrInfo_[a-zA-Z0-9]+_\w*`"))

# dfcsAddrInfo.printSchema()

dfcsAddrInfoInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csAddrInfo_[0-9]+_\w*`"))

dfcsAddrInfoStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csAddrInfo_[a-zA-Z_]*`"))

#dfcsAddrInfoStr.show()

AddrInfoCnt = sorted([int(col.split("_")[1]) for col in dfcsAddrInfoInt.columns if col.split("_")[0] == "csAddrInfo" \
                      and col.split("_")[2] in ["CensusGeoCode","City","CountyCode","DwellingType","DwellingType_code","FirstReportedDate","HomeOwnership" \
                                               ,"HomeOwnership_code","LastReportingSubcode","LastUpdatedDate","Origination","Origination_code","State" \
                                               ,"StreetName","StreetPrefix","StreetSuffix","TimesReported","UnitID","UnitType","Zip"]])[-1]

#print(AddrInfoCnt)

for i in range(AddrInfoCnt):
    prefix = "csAddrInfo_"+str(i+1)
    if not prefix+"_CountyCode" in dfcsAddrInfoInt.columns:
        dfcsAddrInfoInt = dfcsAddrInfoInt.withColumn(prefix+"_CountyCode", sf.lit(None))

for i in range(AddrInfoCnt):
    rule = "csAddrInfo_"+str(i+1)
    dfcsAddrInfoInt = dfcsAddrInfoInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_CensusGeoCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_City"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_CountyCode"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_DwellingType"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_DwellingType_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_FirstReportedDate"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_HomeOwnership"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_HomeOwnership_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_LastReportingSubcode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_LastUpdatedDate"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Origination"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Origination_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_State"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_StreetName"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_StreetPrefix"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_StreetSuffix"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_TimesReported"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_UnitID"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_UnitType"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Zip"),sf.lit("$$$")), sf.lit("^") \
                            ) \
                            )

#dfcsAddrInfoInt.show(2,False)

dfcsAddrInfoInt = dfcsAddrInfoInt.withColumn( "csAddrInfoArray", sf.array([col for col in dfcsAddrInfoInt.columns if col.split("_")[0] == "newcsAddrInfo"]) )

dfexplode = dfcsAddrInfoInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csAddrInfoArray").alias("csAddrInfo") \
            )

dfsplitCol = dfexplode.withColumn("csAddrInfo_CensusGeoCode",sf.split("csAddrInfo","\^")[0]) \
                     .withColumn("csAddrInfo_City",sf.split("csAddrInfo","\^")[1]) \
                     .withColumn('csAddrInfo_CountyCode',sf.split("csAddrInfo","\^")[2]) \
                     .withColumn('csAddrInfo_DwellingType', sf.lit(None).cast(StringType())) \
                     .withColumn("csAddrInfo_DwellingType_code",sf.split("csAddrInfo","\^")[3]) \
                     .withColumn("csAddrInfo_FirstReportedDate",sf.split("csAddrInfo","\^")[4]) \
                     .withColumn('csAddrInfo_HomeOwnership', sf.lit(None).cast(StringType())) \
                     .withColumn("csAddrInfo_HomeOwnership_code",sf.split("csAddrInfo","\^")[5]) \
                     .withColumn("csAddrInfo_LastReportingSubcode",sf.split("csAddrInfo","\^")[6]) \
                     .withColumn("csAddrInfo_LastUpdatedDate",sf.split("csAddrInfo","\^")[7]) \
                     .withColumn('csAddrInfo_Origination', sf.lit(None).cast(StringType())) \
                     .withColumn("csAddrInfo_Origination_code",sf.split("csAddrInfo","\^")[8]) \
                     .withColumn("csAddrInfo_State",sf.split("csAddrInfo","\^")[9]) \
                     .withColumn("csAddrInfo_StreetName",sf.split("csAddrInfo","\^")[10]) \
                     .withColumn("csAddrInfo_StreetPrefix",sf.split("csAddrInfo","\^")[11]) \
                     .withColumn("csAddrInfo_StreetSuffix",sf.split("csAddrInfo","\^")[12]) \
                     .withColumn("csAddrInfo_TimesReported",sf.split("csAddrInfo","\^")[13]) \
                     .withColumn("csAddrInfo_UnitID",sf.split("csAddrInfo","\^")[14]) \
                     .withColumn("csAddrInfo_UnitType",sf.split("csAddrInfo","\^")[15]) \
                     .withColumn("csAddrInfo_Zip",sf.split("csAddrInfo","\^")[16]) \
                     .drop("csAddrInfo")

#dfsplitCol.show(10, False)

dfcsAddrInfoStr = dfcsAddrInfoStr.withColumn('csAddrInfo_CountyCode', sf.lit(None).cast(StringType())) \
                            .withColumn('csAddrInfo_DwellingType', sf.lit(None).cast(StringType())) \
                            .withColumn('csAddrInfo_HomeOwnership', sf.lit(None).cast(StringType())) \
                            .withColumn('csAddrInfo_Origination', sf.lit(None).cast(StringType())) \

dfcsAddrInfoStrCol = dfcsAddrInfoStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csAddrInfo_CensusGeoCode")
                            ,sf.col("csAddrInfo_City")
                            ,sf.col("csAddrInfo_CountyCode")
                            ,sf.col("csAddrInfo_DwellingType")
                            ,sf.col("csAddrInfo_DwellingType_code")
                            ,sf.col("csAddrInfo_FirstReportedDate")
                            ,sf.col("csAddrInfo_HomeOwnership")
                            ,sf.col("csAddrInfo_HomeOwnership_code")
                            ,sf.col("csAddrInfo_LastReportingSubcode")
                            ,sf.col("csAddrInfo_LastUpdatedDate")
                            ,sf.col("csAddrInfo_Origination")
                            ,sf.col("csAddrInfo_Origination_code")
                            ,sf.col("csAddrInfo_State")
                            ,sf.col("csAddrInfo_StreetName")
                            ,sf.col("csAddrInfo_StreetPrefix")
                            ,sf.col("csAddrInfo_StreetSuffix")
                            ,sf.col("csAddrInfo_TimesReported")
                            ,sf.col("csAddrInfo_UnitID")
                            ,sf.col("csAddrInfo_UnitType")
                            ,sf.col("csAddrInfo_Zip")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsAddrInfoStrCol)

#dfsplitColFinal.show(1, False)

dfcsAddrInfo = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csAddrInfo_CensusGeoCode")=="$$$",None).otherwise(sf.col("csAddrInfo_CensusGeoCode")).alias("CensusGeoCode"), \
                sf.when(sf.col("csAddrInfo_City")=="$$$",None).otherwise(sf.col("csAddrInfo_City")).alias("City"), \
                sf.when(sf.col("csAddrInfo_CountyCode")=="$$$",None).otherwise(sf.col("csAddrInfo_CountyCode")).alias("CountyCode"), \
                sf.when(sf.col("csAddrInfo_DwellingType")=="$$$",None).otherwise(sf.col("csAddrInfo_DwellingType")).alias("DwellingType"), \
                sf.when(sf.col("csAddrInfo_DwellingType_code")=="$$$",None).otherwise(sf.col("csAddrInfo_DwellingType_code")).alias("DwellingType_code"), \
                sf.when(sf.col("csAddrInfo_FirstReportedDate")=="$$$",None).otherwise(sf.col("csAddrInfo_FirstReportedDate")).alias("FirstReportedDate"), \
                sf.when(sf.col("csAddrInfo_HomeOwnership")=="$$$",None).otherwise(sf.col("csAddrInfo_HomeOwnership")).alias("HomeOwnership"), \
                sf.when(sf.col("csAddrInfo_HomeOwnership_code")=="$$$",None).otherwise(sf.col("csAddrInfo_HomeOwnership_code")).alias("HomeOwnership_code"), \
                sf.when(sf.col("csAddrInfo_LastReportingSubcode")=="$$$",None).otherwise(sf.col("csAddrInfo_LastReportingSubcode")).alias("LastReportingSubcode"), \
                sf.when(sf.col("csAddrInfo_LastUpdatedDate")=="$$$",None).otherwise(sf.col("csAddrInfo_LastUpdatedDate")).alias("LastUpdatedDate"), \
                sf.when(sf.col("csAddrInfo_Origination")=="$$$",None).otherwise(sf.col("csAddrInfo_Origination")).alias("Origination"), \
                sf.when(sf.col("csAddrInfo_Origination_code")=="$$$",None).otherwise(sf.col("csAddrInfo_Origination_code")).alias("Origination_code"), \
                sf.when(sf.col("csAddrInfo_State")=="$$$",None).otherwise(sf.col("csAddrInfo_State")).alias("State"), \
                sf.when(sf.col("csAddrInfo_StreetName")=="$$$",None).otherwise(sf.col("csAddrInfo_StreetName")).alias("StreetName"), \
                sf.when(sf.col("csAddrInfo_StreetPrefix")=="$$$",None).otherwise(sf.col("csAddrInfo_StreetPrefix")).alias("StreetPrefix"), \
                sf.when(sf.col("csAddrInfo_StreetSuffix")=="$$$",None).otherwise(sf.col("csAddrInfo_StreetSuffix")).alias("StreetSuffix"), \
                sf.when(sf.col("csAddrInfo_TimesReported")=="$$$",None).otherwise(sf.col("csAddrInfo_TimesReported")).alias("TimesReported"), \
                sf.when(sf.col("csAddrInfo_UnitID")=="$$$",None).otherwise(sf.col("csAddrInfo_UnitID")).alias("UnitID"), \
                sf.when(sf.col("csAddrInfo_UnitType")=="$$$",None).otherwise(sf.col("csAddrInfo_UnitType")).alias("UnitType"), \
                sf.when(sf.col("csAddrInfo_Zip")=="$$$",None).otherwise(sf.col("csAddrInfo_Zip")).alias("Zip")) \
                .where(sf.col("CensusGeoCode").isNotNull() | \
                sf.col("City").isNotNull() | \
                sf.col("CountyCode").isNotNull() | \
                sf.col("DwellingType").isNotNull() | \
                sf.col("DwellingType_code").isNotNull() | \
                sf.col("FirstReportedDate").isNotNull() | \
                sf.col("HomeOwnership").isNotNull() | \
                sf.col("HomeOwnership_code").isNotNull() | \
                sf.col("LastReportingSubcode").isNotNull() | \
                sf.col("LastUpdatedDate").isNotNull() | \
                sf.col("Origination").isNotNull() | \
                sf.col("Origination_code").isNotNull() | \
                sf.col("State").isNotNull() | \
                sf.col("StreetName").isNotNull() | \
                sf.col("StreetPrefix").isNotNull() | \
                sf.col("StreetSuffix").isNotNull() | \
                sf.col("TimesReported").isNotNull() | \
                sf.col("UnitID").isNotNull() | \
                sf.col("UnitType").isNotNull() | \
                sf.col("Zip").isNotNull() \
                )

dfcsAddrInfo.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)