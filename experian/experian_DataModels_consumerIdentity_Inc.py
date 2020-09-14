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
appNameSuffix = vendor + "_DataModels_consumerIdentity"

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

tgtfilePath = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/consumerIdentity/"

dfparquet = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquet.withColumn("year",sf.split("createdDate","\-")[0]) \
          .withColumn("month",sf.split("createdDate","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

# dfbaseData = df.select([col for col in df.columns])

# dfbaseData.show(10,False)

dfcsConsumerIdentity = df.select(df.colRegex("`csConsumerIdentity_[a-zA-Z0-9]+_\w*`"))

# dfcsConsumerIdentity.printSchema()

dfcsConsumerIdentityInt = df.select(df.colRegex("`csConsumerIdentity_[0-9]+_\w*`"))

dfcsConsumerIdentityStr = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"), df.colRegex("`csConsumerIdentity_[a-zA-Z_]*`"))

#dfcsConsumerIdentityStr.show()

ConsumerIdentityCnt = sorted([int(col.split("_")[1]) for col in dfcsConsumerIdentityInt.columns if col.split("_")[0] == "csConsumerIdentity" \
                        and col.split("_")[2] in ["Name_First","Name_Gen","Name_Middle","Name_SecondSurname","Name_Surname","Name_Type" \
                                            ,"Name_Type_code","YOB"]])[-1]

#print(ConsumerIdentityCnt)

for i in range(ConsumerIdentityCnt):
    rule = "csConsumerIdentity_"+str(i+1)
    df = df.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_Name_First"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name_Gen"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name_Middle"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name_SecondSurname"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name_Surname"),sf.lit("$$$")), sf.lit("^"),\
                            # sf.coalesce(sf.col(rule+"_Name_Type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_Name_Type_code"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_YOB"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

#df.show(2,False)

df = df.withColumn( "csConsumerIdentityArray", sf.array([col for col in df.columns if col.split("_")[0] == "newcsConsumerIdentity"]) )

dfexplode = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("csConsumerIdentityArray").alias("csConsumerIdentity") \
            )

dfsplitCol = dfexplode.withColumn("csConsumerIdentity_Name_First",sf.split("csConsumerIdentity","\^")[0]) \
                     .withColumn("csConsumerIdentity_Name_Gen",sf.split("csConsumerIdentity","\^")[1]) \
                     .withColumn("csConsumerIdentity_Name_Middle",sf.split("csConsumerIdentity","\^")[2]) \
                     .withColumn("csConsumerIdentity_Name_SecondSurname",sf.split("csConsumerIdentity","\^")[3]) \
                     .withColumn("csConsumerIdentity_Name_Surname",sf.split("csConsumerIdentity","\^")[4]) \
                     .withColumn("csConsumerIdentity_Name_Type",sf.lit(None).cast(StringType())) \
                     .withColumn("csConsumerIdentity_Name_Type_code",sf.split("csConsumerIdentity","\^")[5]) \
                     .withColumn("csConsumerIdentity_YOB",sf.split("csConsumerIdentity","\^")[6]) \
                     .drop("csConsumerIdentity")

#dfsplitCol.show(10, False)

dfcsConsumerIdentityStr = dfcsConsumerIdentityStr.withColumn('csConsumerIdentity_Name_Type', sf.lit(None).cast(StringType()))

dfcsConsumerIdentityStrCol = dfcsConsumerIdentityStr.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day")
                            ,sf.col("csConsumerIdentity_Name_First")
                            ,sf.col("csConsumerIdentity_Name_Gen")
                            ,sf.col("csConsumerIdentity_Name_Middle")
                            ,sf.col("csConsumerIdentity_Name_SecondSurname")
                            ,sf.col("csConsumerIdentity_Name_Surname")
                            ,sf.col("csConsumerIdentity_Name_Type")
                            ,sf.col("csConsumerIdentity_Name_Type_code")
                            ,sf.col("csConsumerIdentity_YOB")
                            )

dfsplitColFinal = dfsplitCol.union(dfcsConsumerIdentityStrCol)

#dfsplitColFinal.show(1, False)

dfcsConsumerIdentity = dfsplitColFinal.select(sf.col("id").alias("id"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("csConsumerIdentity_Name_First")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_First")).alias("csConsumerIdentity_Name_First"), \
                sf.when(sf.col("csConsumerIdentity_Name_Gen")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_Gen")).alias("csConsumerIdentity_Name_Gen"), \
                sf.when(sf.col("csConsumerIdentity_Name_Middle")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_Middle")).alias("csConsumerIdentity_Name_Middle"), \
                sf.when(sf.col("csConsumerIdentity_Name_SecondSurname")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_SecondSurname")).alias("csConsumerIdentity_Name_SecondSurname"), \
                sf.when(sf.col("csConsumerIdentity_Name_Surname")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_Surname")).alias("csConsumerIdentity_Name_Surname"), \
                sf.when(sf.col("csConsumerIdentity_Name_Type")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_Type")).alias("csConsumerIdentity_Name_Type"), \
                sf.when(sf.col("csConsumerIdentity_Name_Type_code")=="$$$",None).otherwise(sf.col("csConsumerIdentity_Name_Type_code")).alias("csConsumerIdentity_Name_Type_code"), \
                sf.when(sf.col("csConsumerIdentity_YOB")=="$$$",None).otherwise(sf.col("csConsumerIdentity_YOB")).alias("csConsumerIdentity_YOB")) \
                .where(sf.col("csConsumerIdentity_Name_First").isNotNull() | \
                sf.col("csConsumerIdentity_Name_Gen").isNotNull() | \
                sf.col("csConsumerIdentity_Name_Middle").isNotNull() | \
                sf.col("csConsumerIdentity_Name_SecondSurname").isNotNull() | \
                sf.col("csConsumerIdentity_Name_Surname").isNotNull() | \
                sf.col("csConsumerIdentity_Name_Type").isNotNull() | \
                sf.col("csConsumerIdentity_Name_Type_code").isNotNull() | \
                sf.col("csConsumerIdentity_YOB").isNotNull() \
                )

dfcsConsumerIdentity.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePath)