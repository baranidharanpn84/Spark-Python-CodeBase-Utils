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
appNameSuffix = vendor + "_DataModels_riskModel"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskModel/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)
dfparquetAllCol = sparkSession.read.format("parquet").load(srcfilePathAllCol)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfcsRiskModel= df.select(df.colRegex("`csRiskModel_[a-zA-Z0-9]+_\w*`"))

# dfcsRiskModel.printSchema()

dfcsRiskModelInt = df.select(df.colRegex("`csRiskModel_[0-9]+_\w*`"))

dfcsRiskModelStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csRiskModel_[a-zA-Z_]*`"))
dfcsRiskModelStrAllCol = dfparquetAllCol.select(dfparquetAllCol.colRegex("`csRiskModel_[a-zA-Z_]*`"))

#dfcsRiskModelStr.show()

RiskModelCnt = sorted([int(col.split("_")[1]) for col in dfcsRiskModelInt.columns if col.split("_")[0] == "csRiskModel" \
                        and col.split("_")[2] in ["Evaluation","Evaluation_code","ModelIndicator","ModelIndicator_code","Score","ScoreFactorCodeFour","ScoreFactorCodeOne" \
                                                ,"ScoreFactorCodeThree","ScoreFactorCodeTwo"]])[-1]

#print(RiskModelCnt)

for col in dfcsRiskModelStrAllCol.columns:
    if not col in dfcsRiskModelStr.columns:
        dfcsRiskModelStr = dfcsRiskModelStr.withColumn(col, sf.lit(None))

for i in range(RiskModelCnt):
    rule = "csRiskModel_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            # sf.coalesce(sf.col(rule+"_Evaluation"),sf.lit("$$$")), sf.lit("^"), \
                            sf.coalesce(sf.col(rule+"_Evaluation_code"),sf.lit("$$$")), sf.lit("^"), \
                            # sf.coalesce(sf.col(rule+"_ModelIndicator"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ModelIndicator_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Score"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ScoreFactorCodeFour"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ScoreFactorCodeOne"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ScoreFactorCodeThree"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_ScoreFactorCodeTwo"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#df.show(2,False)

df = df.withColumn( "csRiskModelArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsRiskModel"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csRiskModelArray").alias("csRiskModel") \
            )

dfsplitCol = dfexplode.withColumn("csRiskModel_Evaluation",sf.lit(None).cast(StringType())) \
                     .withColumn("csRiskModel_Evaluation_code",sf.split("csRiskModel","\^")[0]) \
                     .withColumn("csRiskModel_ModelIndicator",sf.lit(None).cast(StringType())) \
                     .withColumn("csRiskModel_ModelIndicator_code",sf.split("csRiskModel","\^")[1]) \
                     .withColumn("csRiskModel_Score",sf.split("csRiskModel","\^")[2]) \
                     .withColumn("csRiskModel_ScoreFactorCodeFour",sf.split("csRiskModel","\^")[3]) \
                     .withColumn("csRiskModel_ScoreFactorCodeOne",sf.split("csRiskModel","\^")[4]) \
                     .withColumn("csRiskModel_ScoreFactorCodeThree",sf.split("csRiskModel","\^")[5]) \
                     .withColumn("csRiskModel_ScoreFactorCodeTwo",sf.split("csRiskModel","\^")[6]) \
                     .drop("csRiskModel")

#dfsplitCol.show(10, False)

# dfcsRiskModelStr = dfcsRiskModelStr.withColumn('csRiskModel_Evaluation', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_Evaluation_code', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ModelIndicator', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ModelIndicator_code', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_Score', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ScoreFactorCodeFour', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ScoreFactorCodeOne', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ScoreFactorCodeThree', sf.lit(None).cast(StringType())) \
                            # .withColumn('csRiskModel_ScoreFactorCodeTwo', sf.lit(None).cast(StringType())) \

dfcsRiskModelStr = dfcsRiskModelStr.withColumn('csRiskModel_Evaluation', sf.lit(None).cast(StringType())) \
                            .withColumn('csRiskModel_ModelIndicator', sf.lit(None).cast(StringType())) \

dfcsRiskModelStrCol = dfcsRiskModelStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csRiskModel_Evaluation")
                            ,sf.col("csRiskModel_Evaluation_code")
                            ,sf.col("csRiskModel_ModelIndicator")
                            ,sf.col("csRiskModel_ModelIndicator_code")
                            ,sf.col("csRiskModel_Score")
                            ,sf.col("csRiskModel_ScoreFactorCodeFour")
                            ,sf.col("csRiskModel_ScoreFactorCodeOne")
                            ,sf.col("csRiskModel_ScoreFactorCodeThree")
                            ,sf.col("csRiskModel_ScoreFactorCodeTwo")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsRiskModelStrCol)

#dfsplitColFinal.show(1, False)

dfcsRiskModel= dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csRiskModel_Evaluation")=="$$$",None).otherwise(sf.col("csRiskModel_Evaluation")).alias("Evaluation"), \
                sf.when(sf.col("csRiskModel_Evaluation_code")=="$$$",None).otherwise(sf.col("csRiskModel_Evaluation_code")).alias("Evaluation_code"), \
                sf.when(sf.col("csRiskModel_ModelIndicator")=="$$$",None).otherwise(sf.col("csRiskModel_ModelIndicator")).alias("ModelIndicator"), \
                sf.when(sf.col("csRiskModel_ModelIndicator_code")=="$$$",None).otherwise(sf.col("csRiskModel_ModelIndicator_code")).alias("ModelIndicator_code"), \
                sf.when(sf.col("csRiskModel_Score")=="$$$",None).otherwise(sf.col("csRiskModel_Score")).alias("Score"), \
                sf.when(sf.col("csRiskModel_ScoreFactorCodeFour")=="$$$",None).otherwise(sf.col("csRiskModel_ScoreFactorCodeFour")).alias("ScoreFactorCodeFour"), \
                sf.when(sf.col("csRiskModel_ScoreFactorCodeOne")=="$$$",None).otherwise(sf.col("csRiskModel_ScoreFactorCodeOne")).alias("ScoreFactorCodeOne"), \
                sf.when(sf.col("csRiskModel_ScoreFactorCodeThree")=="$$$",None).otherwise(sf.col("csRiskModel_ScoreFactorCodeThree")).alias("ScoreFactorCodeThree"), \
                sf.when(sf.col("csRiskModel_ScoreFactorCodeTwo")=="$$$",None).otherwise(sf.col("csRiskModel_ScoreFactorCodeTwo")).alias("ScoreFactorCodeTwo")) \
                .where(sf.col("Evaluation").isNotNull() | \
                sf.col("Evaluation_code").isNotNull() | \
                sf.col("ModelIndicator").isNotNull() | \
                sf.col("ModelIndicator_code").isNotNull() | \
                sf.col("Score").isNotNull() | \
                sf.col("ScoreFactorCodeFour").isNotNull() | \
                sf.col("ScoreFactorCodeOne").isNotNull() | \
                sf.col("ScoreFactorCodeThree").isNotNull() | \
                sf.col("ScoreFactorCodeTwo").isNotNull() \
                )

dfcsRiskModel.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)