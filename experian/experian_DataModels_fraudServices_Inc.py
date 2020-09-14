%%spark
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
appNameSuffix = vendor + "_DataModels_fraudServices"

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
srcfilePathAllCol = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/*/*/*"

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/fraudServices/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)
dfparquetAllCol = sparkSession.read.format("parquet").load(srcfilePathAllCol)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData.show(10,False)

dfcsFraudServices = df.select(df.colRegex("`csFraudServices_[a-zA-Z0-9]+_\w*`"))

# dfcsFraudServices.printSchema()

dfcsFraudServicesInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csFraudServices_[0-9]+_\w*`"))
dfcsFraudServIntAllCol = dfparquetAllCol.select(sf.col("id"), dfparquetAllCol.colRegex("`csFraudServices_[0-9]+_\w*`"))

dfcsFraudServicesStrInd = df.select(sf.col("id").alias("id_ind"), df.colRegex("`csFraudServices_[a-zA-Z_]+_\w*`"))
dfcsFraudServicesStrIndAllCol = dfparquetAllCol.select(dfparquetAllCol.colRegex("`csFraudServices_[a-zA-Z_]+_\w*`"))

dfcsFraudServicesStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csFraudServices_[a-zA-Z_]*`"))

#dfcsFraudServicesStr.show()

FraudServicesCnt = sorted([int(col.split("_")[1]) for col in dfcsFraudServicesInt.columns if col.split("_")[0] == "csFraudServices" \
                        and col.split("_")[2] in ["AddressCount","AddressDate","AddressErrorCode_code","DateOfBirth","DateOfDeath" \
                                            ,"Indicator_1","Indicator_2","Indicator_3","Indicator_4","Indicator_5","Indicator_6","Indicator_7","Indicator_8","Indicator_9" \
                                            ,"Indicator_10","Indicator_11","Indicator_12","SIC_code","SSNFirstPossibleIssuanceYear","SSNLastPossibleIssuanceYear","SocialCount" \
                                            ,"SocialDate","SocialErrorCode_code","Text","Type_code"]])[-1]

#print(FraudServicesCnt)

for col in dfcsFraudServIntAllCol.columns:
    instr = col.find('_', 17, 20)
    for i in range(FraudServicesCnt):
        rule = "csFraudServices_"+str(i+1)
        if not rule+"_"+col[instr+1:] in dfcsFraudServicesInt.columns:
            dfcsFraudServicesInt = dfcsFraudServicesInt.withColumn(rule+"_"+col[instr+1:], sf.lit(None))

for col in dfcsFraudServicesStrIndAllCol.columns:
    if not col in dfcsFraudServicesStrInd.columns:
        dfcsFraudServicesStrInd = dfcsFraudServicesStrInd.withColumn(col, sf.lit(None))

for i in range(FraudServicesCnt):
    rule = "csFraudServices_"+str(i+1)
    dfcsFraudServicesInt = dfcsFraudServicesInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_AddressCount"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_AddressDate"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_AddressErrorCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_AddressErrorCode_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_DateOfBirth"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_DateOfDeath"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_1"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_2"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_3"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_4"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_5"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_6"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_7"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_8"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_9"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_10"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_11"),sf.lit("null")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Indicator_12"),sf.lit("null")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_SIC"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SIC_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SSNFirstPossibleIssuanceYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SSNLastPossibleIssuanceYear"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SocialCount"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SocialDate"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_SocialErrorCode"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_SocialErrorCode_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Text"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Type_code"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#dfcsFraudServicesInt.show(2,False)

dfcsFraudServicesInt = dfcsFraudServicesInt.withColumn( "csFraudServicesArray", sf.array([col for col in dfcsFraudServicesInt.columns if col.split("_")[0] == "newcsFraudServices"]) )

dfexplode = dfcsFraudServicesInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csFraudServicesArray").alias("csFraudServices") \
            )

dfsplitCol = dfexplode.withColumn("csFraudServices_AddressCount",sf.split("csFraudServices","\^")[0]) \
                     .withColumn("csFraudServices_AddressDate",sf.split("csFraudServices","\^")[1]) \
                     .withColumn("csFraudServices_AddressErrorCode",sf.lit(None).cast(StringType())) \
                     .withColumn("csFraudServices_AddressErrorCode_code",sf.split("csFraudServices","\^")[2]) \
                     .withColumn("csFraudServices_DateOfBirth",sf.split("csFraudServices","\^")[3]) \
                     .withColumn("csFraudServices_DateOfDeath",sf.split("csFraudServices","\^")[4]) \
                     .withColumn("csFraudServices_Indicator_1",sf.split("csFraudServices","\^")[5]) \
                     .withColumn("csFraudServices_Indicator_2",sf.split("csFraudServices","\^")[6]) \
                     .withColumn("csFraudServices_Indicator_3",sf.split("csFraudServices","\^")[7]) \
                     .withColumn("csFraudServices_Indicator_4",sf.split("csFraudServices","\^")[8]) \
                     .withColumn("csFraudServices_Indicator_5",sf.split("csFraudServices","\^")[9]) \
                     .withColumn("csFraudServices_Indicator_6",sf.split("csFraudServices","\^")[10]) \
                     .withColumn("csFraudServices_Indicator_7",sf.split("csFraudServices","\^")[11]) \
                     .withColumn("csFraudServices_Indicator_8",sf.split("csFraudServices","\^")[12]) \
                     .withColumn("csFraudServices_Indicator_9",sf.split("csFraudServices","\^")[13]) \
                     .withColumn("csFraudServices_Indicator_10",sf.split("csFraudServices","\^")[14]) \
                     .withColumn("csFraudServices_Indicator_11",sf.split("csFraudServices","\^")[15]) \
                     .withColumn("csFraudServices_Indicator_12",sf.split("csFraudServices","\^")[16]) \
                     .withColumn("csFraudServices_SIC",sf.lit(None).cast(StringType())) \
                     .withColumn("csFraudServices_SIC_code",sf.split("csFraudServices","\^")[17]) \
                     .withColumn("csFraudServices_SSNFirstPossibleIssuanceYear",sf.split("csFraudServices","\^")[18]) \
                     .withColumn("csFraudServices_SSNLastPossibleIssuanceYear",sf.split("csFraudServices","\^")[19]) \
                     .withColumn("csFraudServices_SocialCount",sf.split("csFraudServices","\^")[20]) \
                     .withColumn("csFraudServices_SocialDate",sf.split("csFraudServices","\^")[21]) \
                     .withColumn("csFraudServices_SocialErrorCode",sf.lit(None).cast(StringType())) \
                     .withColumn("csFraudServices_SocialErrorCode_code",sf.split("csFraudServices","\^")[22]) \
                     .withColumn("csFraudServices_Text",sf.split("csFraudServices","\^")[23]) \
                     .withColumn("csFraudServices_Type_code",sf.split("csFraudServices","\^")[24]) \
                     .drop("csFraudServices")

dfsplitCol = dfsplitCol.withColumn("csFraudServices_Indicator",sf.concat(
                                    sf.coalesce(sf.col("csFraudServices_Indicator_1"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_2"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_3"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_4"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_5"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_6"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_7"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_8"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_9"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_10"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_11"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_12"),sf.lit("null")) ))

#dfsplitCol.show(10, False)

dfcsFraudServicesCol = dfsplitCol.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csFraudServices_AddressCount")
                            ,sf.col("csFraudServices_AddressDate")
                            ,sf.col("csFraudServices_AddressErrorCode")
                            ,sf.col("csFraudServices_AddressErrorCode_code")
                            ,sf.col("csFraudServices_DateOfBirth")
                            ,sf.col("csFraudServices_DateOfDeath")
                            ,sf.col("csFraudServices_Indicator")
                            ,sf.col("csFraudServices_SIC")
                            ,sf.col("csFraudServices_SIC_code")
                            ,sf.col("csFraudServices_SSNFirstPossibleIssuanceYear")
                            ,sf.col("csFraudServices_SSNLastPossibleIssuanceYear")
                            ,sf.col("csFraudServices_SocialCount")
                            ,sf.col("csFraudServices_SocialDate")
                            ,sf.col("csFraudServices_SocialErrorCode")
                            ,sf.col("csFraudServices_SocialErrorCode_code")
                            ,sf.col("csFraudServices_Text")
                            ,sf.col("csFraudServices_Type_code")
                            )

dfcsFraudServicesStrInd = dfcsFraudServicesStrInd.withColumn("csFraudServices_Indicator",sf.concat(
                                    sf.coalesce(sf.col("csFraudServices_Indicator_1"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_2"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_3"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_4"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_5"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_6"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_7"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_8"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_9"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_10"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_11"),sf.lit("null")), sf.lit(":"),\
                                    sf.coalesce(sf.col("csFraudServices_Indicator_12"),sf.lit("null")) ))

# dfcsFraudServicesStr = dfcsFraudServicesStr.withColumn('csFraudServices_AddressCount', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_AddressDate', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_AddressErrorCode', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_DateOfBirth', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_DateOfDeath', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SIC', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SSNFirstPossibleIssuanceYear', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SSNLastPossibleIssuanceYear', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SocialCount', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SocialDate', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_SocialErrorCode', sf.lit(None).cast(StringType())) \
                                        # .withColumn('csFraudServices_Text', sf.lit(None).cast(StringType()))

dfcsFraudServicesStr = dfcsFraudServicesStr.withColumn('csFraudServices_AddressErrorCode', sf.lit(None).cast(StringType())) \
                                        .withColumn('csFraudServices_SIC', sf.lit(None).cast(StringType())) \
                                        .withColumn('csFraudServices_SocialErrorCode', sf.lit(None).cast(StringType()))

dfcsFraudServicesStrCol = dfcsFraudServicesStr.join(dfcsFraudServicesStrInd,(dfcsFraudServicesStr.id == dfcsFraudServicesStrInd.id_ind),'left_outer') \
                            .select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,dfcsFraudServicesStr.csFraudServices_AddressCount
                            ,dfcsFraudServicesStr.csFraudServices_AddressDate
                            ,dfcsFraudServicesStr.csFraudServices_AddressErrorCode
                            ,dfcsFraudServicesStr.csFraudServices_AddressErrorCode_code
                            ,dfcsFraudServicesStr.csFraudServices_DateOfBirth
                            ,dfcsFraudServicesStr.csFraudServices_DateOfDeath
                            ,dfcsFraudServicesStrInd.csFraudServices_Indicator
                            ,dfcsFraudServicesStr.csFraudServices_SIC
                            ,dfcsFraudServicesStr.csFraudServices_SIC_code
                            ,dfcsFraudServicesStr.csFraudServices_SSNFirstPossibleIssuanceYear
                            ,dfcsFraudServicesStr.csFraudServices_SSNLastPossibleIssuanceYear
                            ,dfcsFraudServicesStr.csFraudServices_SocialCount
                            ,dfcsFraudServicesStr.csFraudServices_SocialDate
                            ,dfcsFraudServicesStr.csFraudServices_SocialErrorCode
                            ,dfcsFraudServicesStr.csFraudServices_SocialErrorCode_code
                            ,dfcsFraudServicesStr.csFraudServices_Text
                            ,dfcsFraudServicesStr.csFraudServices_Type_code
                            )

dfsplitColFinal = dfcsFraudServicesCol.union(dfcsFraudServicesStrCol)

#dfsplitColFinal.show(1, False)

dfcsFraudServices = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csFraudServices_AddressCount")=="$$$",None).otherwise(sf.col("csFraudServices_AddressCount")).alias("AddressCount"), \
                sf.when(sf.col("csFraudServices_AddressDate")=="$$$",None).otherwise(sf.col("csFraudServices_AddressDate")).alias("AddressDate"), \
                sf.when(sf.col("csFraudServices_AddressErrorCode")=="$$$",None).otherwise(sf.col("csFraudServices_AddressErrorCode")).alias("AddressErrorCode"), \
                sf.when(sf.col("csFraudServices_AddressErrorCode_code")=="$$$",None).otherwise(sf.col("csFraudServices_AddressErrorCode_code")).alias("AddressErrorCode_code"), \
                sf.when(sf.col("csFraudServices_DateOfBirth")=="$$$",None).otherwise(sf.col("csFraudServices_DateOfBirth")).alias("DateOfBirth"), \
                sf.when(sf.col("csFraudServices_DateOfDeath")=="$$$",None).otherwise(sf.col("csFraudServices_DateOfDeath")).alias("DateOfDeath"), \
                sf.when(sf.col("csFraudServices_Indicator").like("%$$$%") | sf.col("csFraudServices_Indicator").like("%null:null:null:null:null:null:null:null:null:null:null:null%"),None).otherwise(sf.col("csFraudServices_Indicator")).alias("Indicator"), \
                # sf.when(sf.col("csFraudServices_Indicator")=="$$$" | sf.col("csFraudServices_Indicator")=="null:null:null:null:null:null:null:null:null:null:null:null",None).otherwise(sf.col("csFraudServices_Indicator")).alias("Indicator"), \
                sf.when(sf.col("csFraudServices_SIC")=="$$$",None).otherwise(sf.col("csFraudServices_SIC")).alias("SIC"), \
                sf.when(sf.col("csFraudServices_SIC_code")=="$$$",None).otherwise(sf.col("csFraudServices_SIC_code")).alias("SIC_code"), \
                sf.when(sf.col("csFraudServices_SSNFirstPossibleIssuanceYear")=="$$$",None).otherwise(sf.col("csFraudServices_SSNFirstPossibleIssuanceYear")).alias("SSNFirstPossibleIssuanceYear"), \
                sf.when(sf.col("csFraudServices_SSNLastPossibleIssuanceYear")=="$$$",None).otherwise(sf.col("csFraudServices_SSNLastPossibleIssuanceYear")).alias("SSNLastPossibleIssuanceYear"), \
                sf.when(sf.col("csFraudServices_SocialCount")=="$$$",None).otherwise(sf.col("csFraudServices_SocialCount")).alias("SocialCount"), \
                sf.when(sf.col("csFraudServices_SocialDate")=="$$$",None).otherwise(sf.col("csFraudServices_SocialDate")).alias("SocialDate"), \
                sf.when(sf.col("csFraudServices_SocialErrorCode")=="$$$",None).otherwise(sf.col("csFraudServices_SocialErrorCode")).alias("SocialErrorCode"), \
                sf.when(sf.col("csFraudServices_SocialErrorCode_code")=="$$$",None).otherwise(sf.col("csFraudServices_SocialErrorCode_code")).alias("SocialErrorCode_code"), \
                sf.when(sf.col("csFraudServices_Text")=="$$$",None).otherwise(sf.col("csFraudServices_Text")).alias("Text"), \
                sf.when(sf.col("csFraudServices_Type_code")=="$$$",None).otherwise(sf.col("csFraudServices_Type_code")).alias("Type_code")) \
                .where(sf.col("AddressCount").isNotNull() | \
                sf.col("AddressDate").isNotNull() | \
                sf.col("AddressErrorCode").isNotNull() | \
                sf.col("AddressErrorCode_code").isNotNull() | \
                sf.col("DateOfBirth").isNotNull() | \
                sf.col("DateOfDeath").isNotNull() | \
                sf.col("Indicator").isNotNull() | \
                sf.col("SIC").isNotNull() | \
                sf.col("SIC_code").isNotNull() | \
                sf.col("SSNFirstPossibleIssuanceYear").isNotNull() | \
                sf.col("SSNLastPossibleIssuanceYear").isNotNull() | \
                sf.col("SocialCount").isNotNull() | \
                sf.col("SocialDate").isNotNull() | \
                sf.col("SocialErrorCode").isNotNull() | \
                sf.col("SocialErrorCode_code").isNotNull() | \
                sf.col("Text").isNotNull() | \
                sf.col("Type_code").isNotNull()
                )

dfcsFraudServices.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)