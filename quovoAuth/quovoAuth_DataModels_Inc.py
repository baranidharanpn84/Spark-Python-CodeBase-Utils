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

def get_recent_dir (prefix_input):
    while True:
        result = client.list_objects(Bucket=bucket, Prefix=prefix_input, Delimiter='/',MaxKeys=1500)
        if result.get('CommonPrefixes') == None:
            last_dir = prefix_input
            return last_dir
        else:
            last_dir = sorted([prefix.get('Prefix') for prefix in result.get('CommonPrefixes')])[-1]
            prefix_input = last_dir

    return prefix_input

##########################################################################################################

def blank_as_null(x):
    return when(sf.col(x) != "", sf.col(x)).otherwise(None)

concat_udf = sf.udf(lambda cols: "~".join([x if x is not None else "" for x in cols]), StringType())

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
appNameSuffix = vendor + "Spark_DataModels"

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

tgtfilePathBD = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/baseData/"

tgtfilePathAcc = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/accounts/"

dfparquetSrc = sparkSession.read.format("parquet").load(srcfilePath)

df = dfparquetSrc.withColumn("year",sf.split("createdDate","\-")[0]) \
                .withColumn("month",sf.split("createdDate","\-")[1]) \
                .withColumn("day",sf.split((sf.split((sf.split("createdDate","\-")[2]),"T")[0])," ")[0])

dfbaseData = df.select([col for col in df.columns])

dfauthAccntInt = df.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day"),df.colRegex("`auth_accounts_[0-9]+_\w*`"))

dfauthAccntInt = reduce(lambda dfauthAccntInt, idx: dfauthAccntInt.withColumnRenamed(list(dfauthAccntInt.schema.names)[idx],
                                                 list(dfauthAccntInt.schema.names)[idx].replace("auth_accounts","authAccounts")),
            range(len(list(dfauthAccntInt.schema.names))),
            dfauthAccntInt)

authAccntCnt = sorted([int(col.split("_")[1]) for col in dfauthAccntInt.columns if col.split("_")[0] == "authAccounts" \
                        and col.split("_")[2] in ["category","id","routing","type"]])[-1]

# authAccntCnt = sorted([int(col.split("_")[1]) for col in dfauthAccntInt.columns if col.split("_")[0] == "authAccounts" \
                        # and col.split("_")[2] in ["account_name","account_nickname","account_number","available_balance","category","id","routing","type","type_confidence","present_balance"]])[-1]

# print(authAccntCnt)

for i in range(authAccntCnt):
    prefix = "authAccounts_"+str(i+1)
    if not prefix+"_account_number" in dfauthAccntInt.columns:
        dfauthAccntInt = dfauthAccntInt.withColumn(prefix+"_account_number", sf.lit(None))

for i in range(authAccntCnt):
    rule = "authAccounts_"+str(i+1)
    dfauthAccntInt = dfauthAccntInt.withColumn("new" + rule, \
                        sf.concat(
                            sf.coalesce(sf.col(rule+"_account_name"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_account_nickname"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_account_number"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_available_balance"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_category"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_id"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_routing"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_type"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_type_confidence"),sf.lit("$$$")), sf.lit("^"),\
                            sf.coalesce(sf.col(rule+"_present_balance"),sf.lit("$$$")), sf.lit("^")\
                            ) \
                            )

# dfauthAccntInt.show(2,False)

dfauthAccntInt = dfauthAccntInt.withColumn( "authAccountsArray", sf.array([col for col in dfauthAccntInt.columns if col.split("_")[0] == "newauthAccounts"]) )

dfexplode = dfauthAccntInt.select(sf.col("id"),sf.col("year"),sf.col("month"),sf.col("day") \
            ,sf.explode_outer("authAccountsArray").alias("authAccounts") \
            )

dfsplitCol = dfexplode.withColumn("authAccounts_account_name",sf.split("authAccounts","\^")[0]) \
                     .withColumn("authAccounts_account_nickname",sf.split("authAccounts","\^")[1]) \
                     .withColumn('authAccounts_account_number',sf.split("authAccounts","\^")[2]) \
                     .withColumn("authAccounts_available_balance",sf.split("authAccounts","\^")[3]) \
                     .withColumn("authAccounts_category",sf.split("authAccounts","\^")[4]) \
                     .withColumn("authAccounts_id",sf.split("authAccounts","\^")[5]) \
                     .withColumn("authAccounts_routing",sf.split("authAccounts","\^")[6]) \
                     .withColumn("authAccounts_type",sf.split("authAccounts","\^")[7]) \
                     .withColumn("authAccounts_type_confidence",sf.split("authAccounts","\^")[8]) \
                     .withColumn("authAccounts_present_balance",sf.split("authAccounts","\^")[9]) \
                     .drop("authAccounts")

# dfsplitCol.show(10, False)

dfBD = dfbaseData.withColumn("_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("_id")]))) \
                       .withColumn("owner_details_address_data_city", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_city")]))) \
                       .withColumn("owner_details_address_data_state", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_state")]))) \
                       .withColumn("owner_details_address_data_street", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_street")]))) \
                       .withColumn("owner_details_address_data_zip", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_zip")]))) \
                       .withColumn("owner_details_address_data_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_id")]))) \
                       .withColumn("owner_details_address_data_error_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_error_id")]))) \
                       .withColumn("owner_details_address_data_error_message", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_data_error_message")]))) \
                       .withColumn("owner_details_address_dob_error_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_dob_error_id")]))) \
                       .withColumn("owner_details_address_dob_error_message", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_dob_error_message")]))) \
                       .withColumn("owner_details_address_email_data", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_email_data")]))) \
                       .withColumn("owner_details_address_email_error_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_email_error_id")]))) \
                       .withColumn("owner_details_address_email_error_message", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_email_error_message")]))) \
                       .withColumn("owner_details_address_owner_names_data_1", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_owner_names_data_1")]))) \
                       .withColumn("owner_details_address_owner_names_error_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_owner_names_error_id")]))) \
                       .withColumn("owner_details_address_owner_names_error_message", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_owner_names_error_message")]))) \
                       .withColumn("owner_details_address_phone_data", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_phone_data")]))) \
                       .withColumn("owner_details_address_phone_error_id", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_phone_error_id")]))) \
                       .withColumn("owner_details_address_phone_error_message", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("owner_details_address_phone_error_message")]))) \

                       # .withColumn("account_name", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("account_name")]))) \
                       # .withColumn("account_nickname", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("account_nickname")]))) \
                       # .withColumn("account_number", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("account_number")]))) \
                       # .withColumn("available_balance", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("available_balance")]))) \
                       # .withColumn("category", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("category")]))) \
                       # .withColumn("present_balance", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("present_balance")]))) \
                       # .withColumn("routing", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("routing")]))) \
                       # .withColumn("type", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("type")]))) \
                       # .withColumn("type_confidence", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("type_confidence")]))) \
                       # .withColumn("wire_routing", concat_udf(sf.array([col for col in dfbaseData.columns if col.endswith("wire_routing")]))) \

dfBDFinal = dfBD.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day")
                    ,sf.col("applicantId"),sf.col("applicationSource"),sf.col("loanApplicationId"),sf.col("mvpApplicantId"),sf.col("noHit"),sf.col("successful")
                    ,sf.col("timestamp"),sf.col("updatedAt"),sf.col("userId"),sf.col("createdDate"),sf.col("createdDatePT"),sf.col("auth_country_code"),sf.col("auth_id")
                    ,sf.col("auth_institution_id"),sf.col("auth_institution_name"),sf.col("auth_last_good_auth"),sf.col("auth_user_id"),sf.col("auth_username")
                    # ,sf.col("account_name"),sf.col("account_nickname"),sf.col("account_number"),sf.col("available_balance"),sf.col("category")
                    ,sf.col("_id"),sf.col("owner_details_address_data_city"),sf.col("owner_details_address_data_state"),sf.col("owner_details_address_data_street"),sf.col("owner_details_address_data_zip")
                    ,sf.col("owner_details_address_data_id"),sf.col("owner_details_address_data_error_id"),sf.col("owner_details_address_data_error_message")
                    ,sf.col("owner_details_address_dob_error_id"),sf.col("owner_details_address_dob_error_message")
                    ,sf.col("owner_details_address_email_data"),sf.col("owner_details_address_email_error_id"),sf.col("owner_details_address_email_error_message")
                    ,sf.col("owner_details_address_owner_names_data_1"),sf.col("owner_details_address_owner_names_error_id"),sf.col("owner_details_address_owner_names_error_message")
                    ,sf.col("owner_details_address_phone_data"),sf.col("owner_details_address_phone_error_id"),sf.col("owner_details_address_phone_error_message"))
                    # ,sf.col("present_balance"),sf.col("routing"),sf.col("type"),sf.col("type_confidence"),sf.col("wire_routing"))

dfauthAccnt = dfsplitCol.select(sf.col("id").alias("mongoId"),sf.col("year"),sf.col("month"),sf.col("day"), \
                sf.when(sf.col("authAccounts_account_name")=="$$$",None).otherwise(sf.col("authAccounts_account_name")).alias("accountName"), \
                sf.when(sf.col("authAccounts_account_nickname")=="$$$",None).otherwise(sf.col("authAccounts_account_nickname")).alias("accountNickName"), \
                sf.when(sf.col("authAccounts_account_number")=="$$$",None).otherwise(sf.col("authAccounts_account_number")).alias("accountNumber"), \
                sf.when(sf.col("authAccounts_available_balance")=="$$$",None).otherwise(sf.col("authAccounts_available_balance")).alias("availableBalance"), \
                sf.when(sf.col("authAccounts_category")=="$$$",None).otherwise(sf.col("authAccounts_category")).alias("category"), \
                sf.when(sf.col("authAccounts_id")=="$$$",None).otherwise(sf.col("authAccounts_id")).alias("accountId"), \
                sf.when(sf.col("authAccounts_routing")=="$$$",None).otherwise(sf.col("authAccounts_routing")).alias("routing"), \
                sf.when(sf.col("authAccounts_type")=="$$$",None).otherwise(sf.col("authAccounts_type")).alias("type"), \
                sf.when(sf.col("authAccounts_type_confidence")=="$$$",None).otherwise(sf.col("authAccounts_type_confidence")).alias("typeConfidence"), \
                sf.when(sf.col("authAccounts_present_balance")=="$$$",None).otherwise(sf.col("authAccounts_present_balance")).alias("presentBalance")) \
                .where(sf.col("accountName").isNotNull() | \
                sf.col("accountNickName").isNotNull() | \
                sf.col("accountNumber").isNotNull() | \
                sf.col("availableBalance").isNotNull() | \
                sf.col("category").isNotNull() | \
                sf.col("accountId").isNotNull() | \
                sf.col("routing").isNotNull() | \
                sf.col("type").isNotNull() | \
                sf.col("typeConfidence").isNotNull() | \
                sf.col("presentBalance").isNotNull() \
                )

dfBDFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathBD)
                   
dfauthAccnt.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathAcc)