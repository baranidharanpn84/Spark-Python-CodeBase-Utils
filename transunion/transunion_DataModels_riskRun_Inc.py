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
import calendar

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

# def concat_udf(*cols):
    # return sf.concat(*[sf.coalesce(c, sf.lit("")) for c in cols])

concat_udf = sf.udf(lambda cols: "".join([x if x is not None else "" for x in cols]), StringType())

##########################################################################################################

#Create function and register udf
def funcTZConversion (ip_dt_str,from_tz,to_tz):
    fmt = "%Y-%m-%d %H:%M:%S"
    datetime_obj_naive = datetime.strptime(ip_dt_str,fmt)
    datetime_obj_tz = timezone(from_tz).localize(datetime_obj_naive)
    tgt_tz = timezone(to_tz)
    tgt_dtime = datetime_obj_tz.astimezone(tgt_tz)
    return tgt_dtime.strftime(fmt)

udf_TZConversion = udf(funcTZConversion)

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
start_date = sys.argv[5]
enriched_path = sys.argv[6]
appNameSuffix = vendor + "_DataModels_riskRun"

year = start_date.split('-',1)[0]
month = start_date.split('-',2)[1]
day = start_date.split('-',3)[2]

date_format = date(int(year), int(month), int(day))
days_in_month = calendar.monthrange(date_format.year, date_format.month)[1]
end_date = str(date_format + timedelta(days=int(days_in_month)))

# If you run in pyspark, ignore sc =SparkContext(). Else if you run via spark-submit, uncomment this.
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

prefix_rmi = "gold/fdm/risk/risk_model_input/"
rmi_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rmi).split("/")[:-3]) + "/*/*/*"

prefix_rms = "gold/fdm/risk/risk_model_score/"
rms_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_rms).split("/")[:-3]) + "/*/*/*"

prefix_cp = "gold/fdm/risk/credit_profile/"
cp_path = "s3://" + bucket + "/" + "/".join(get_recent_dir(prefix_cp).split("/")[:-3]) + "/*/*/*"

dfrmi = sparkSession.read.format("parquet").load(rmi_path)

dfrmsSrc = sparkSession.read.format("parquet").load(rms_path)
dfrmsSrc = dfrmsSrc.withColumn("date_created_utc",sf.to_timestamp(udf_TZConversion(sf.regexp_replace(dfrmsSrc.date_created,"T"," ").cast("string"),sf.lit("US/Pacific"),sf.lit("UTC")),"yyyy-MM-dd HH:mm:ss"))
dfrms = dfrmsSrc.where((sf.col("date_created_utc") >= sf.lit(start_date)) & (sf.col("date_created_utc") < sf.lit(end_date)))

# dfrms.count()

dfcp = sparkSession.read.format("parquet").load(cp_path)
dfcpTU = dfcp.filter(sf.col("bureau").isin(["TU","TRANSUNION"]))

srcfilePathID = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/id/*/*/*"
srcfilePathVM = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/vantageModel/*/*/*"
srcfilePathVM3 = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/vantageModel3/*/*/*"

tgtfilePathRR = "s3://" + bucket + "/" + enriched_path + vendor + "/DataModels/riskRun/"

dfparquetID = sparkSession.read.format("parquet").load(srcfilePathID)
dfparquetVM = sparkSession.read.format("parquet").load(srcfilePathVM)
dfparquetVM3 = sparkSession.read.format("parquet").load(srcfilePathVM3)

dfrmsFinal = dfrms.withColumn("year",sf.split("date_created_utc","\-")[0]) \
          .withColumn("month",sf.split("date_created_utc","\-")[1]) \
          .withColumn("day",sf.split((sf.split((sf.split("date_created_utc","\-")[2]),"T")[0])," ")[0])

# dfrmsFinal.show(10, False)

# dfbaseData = df.select([col for col in df.columns])

# dfcpTU.printSchema()
# dfrmi.printSchema()
# dfrmsFinal.printSchema()

dfbaseDataVMs = dfparquetID.join(dfparquetVM,(dfparquetID.id == dfparquetVM.id),'left_outer') \
                    .join(dfparquetVM3,(dfparquetID.id == dfparquetVM3.id),'left_outer') \
                    .select(dfparquetID.id.alias("id") \
                    ,dfparquetID.credit_profile_id.alias("er_tu_credit_profile_id") \
                    ,dfparquetID.client_id.alias("client_id") \
                    ,dfparquetID.mvp_client_id.alias("mvp_client_id") \
                    ,dfparquetID.mvp_applicant_id.alias("mvp_applicant_id") \
                    ,dfparquetID.loan_application_id.alias("loan_application_id") \
                    ,dfparquetID.mvp_loan_application_id.alias("mvp_loan_application_id") \
                    ,dfparquetID.pull_credit_type.alias("er_pull_credit_type") \
                    ,dfparquetID.date_created_utc.alias("er_date_created_utc") \
                    ,dfparquetID.date_created_pt.alias("er_date_created") \
                    ,sf.regexp_replace(dfparquetVM.results,"\+","").alias("vm_results") \
                    ,sf.regexp_replace(dfparquetVM3.results,"\+","").alias("vm3_results"))

dfjoinFinal = dfrmsFinal.join(dfrmi,(dfrmi.id == dfrmsFinal.input_id),'left_outer') \
                    .join(dfcpTU,(dfrmi.trans_union_credit_profile_id == dfcpTU.id),'left_outer') \
                    .join(dfbaseDataVMs,(dfcpTU.mongo_id == dfbaseDataVMs.id),'left_outer') \
                    .select(dfrmsFinal.loan_application_id.alias("loan_application_id") \
                    ,dfrmsFinal.applicant_id.alias("applicant_id") \
                    ,dfrmsFinal.application_source.alias("application_source") \
                    ,dfrmsFinal.id.alias("rms_id") \
                    ,dfrmsFinal.date_created_utc.alias("rms_date_created_utc") \
                    ,dfrmsFinal.date_created.alias("rms_date_created") \
                    ,dfrmsFinal.score_type.alias("rms_score_type") \
                    ,dfrmi.id.alias("rmi_id") \
                    ,dfrmi.trans_union_credit_profile_id.alias("rmi_tu_credit_profile_id") \
                    ,dfrmi.date_created.alias("rmi_date_created") \
                    ,dfcpTU.date_created.alias("cp_date_created") \
                    ,dfcpTU.client_id.alias("cp_client_id") \
                    ,dfcpTU.applicant_id.alias("cp_applicant_id") \
                    ,dfcpTU.pull_credit_type.alias("cp_pull_credit_type") \
                    ,dfcpTU.hard_pull_mongo_id.alias("cp_hard_pull_mongo_id") \
                    ,dfcpTU.mongo_id.alias("cp_mongo_id") \
                    ,dfcpTU.vantage_score.alias("cp_tu_vantage_v1") \
                    ,dfcpTU.vantage_score3.alias("cp_tu_vantage_v3") \
                    ,dfbaseDataVMs.er_date_created_utc \
                    ,dfbaseDataVMs.er_date_created \
                    ,dfbaseDataVMs.er_tu_credit_profile_id \
                    ,dfbaseDataVMs.id.alias("er_mongo_id") \
                    ,dfbaseDataVMs.er_pull_credit_type \
                    ,dfbaseDataVMs.vm_results.alias("er_tu_vantage_v1") \
                    ,dfbaseDataVMs.vm3_results.alias("er_tu_vantage_v3") \
                    ,dfrmsFinal.year \
                    ,dfrmsFinal.month \
                    ,dfrmsFinal.day)

# dfbaseDataVMs.createOrReplaceTempView("dfbaseDataVMs_sql")
# dfjoinFinal.createOrReplaceTempView("dfjoinFinal_sql")

# spark.sql("select * from dfbaseDataVMs_sql where id = '1487277'").show(100, False)

# spark.sql("select * from dfjoinFinal_sql where rms_id = '30871135'").show(100, False)

dfjoinFinal.repartition(sf.col("year"),sf.col("month"),sf.col("day")) \
                   .write.format("parquet") \
                   .partitionBy("year","month","day") \
                   .mode("overwrite") \
                   .save(tgtfilePathRR)