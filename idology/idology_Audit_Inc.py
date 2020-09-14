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
import uuid
import psycopg2
import sys

##########################################################################################################

def redshift_s3_copy_tbl():
    sql_del_tbl = "delete from "+table_name+" where vendor_name = '"+vendor+"' and date_created_utc = '"+year+"-"+month+"';"
    sql_s3_redshift = "copy "+table_name+" from 's3://"+bucket+"/"+enriched_path+"Audits/"+vendor+"/' access_key_id '"+access_key+"' secret_access_key '"+secret_key+"' format as parquet;"
    conn = psycopg2.connect(host=host_name, dbname=db_name, port=port, user=user_name, password=pwd)
    cur = conn.cursor();
    # Begin your transaction
    cur.execute("begin;")
    cur.execute(sql_del_tbl)
    cur.execute(sql_s3_redshift)
    # Commit your transaction
    cur.execute("commit;")
    print(vendor + " audit count refresh is complete")

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
gold_path = sys.argv[7]
appNameSuffix = vendor + "_auditCount"

year = date.split('-',1)[0]
month = date.split('-',2)[1]
day = date.split('-',3)[2]

host_name = sys.argv[8]
db_name = sys.argv[9]
port = sys.argv[10]
user_name = sys.argv[11]
pwd = sys.argv[12]
table_name = sys.argv[13]

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
                # .config("spark.sql.parquet.mergeSchema", "true")
                .config("spark.sql.parquet.mergeSchema", "false")
                .config("spark.sql.caseSensitive","true")
                .config("spark.sql.shuffle.partitions","5")
                .config("spark.sql.sources.partitionOverwriteMode","dynamic")
                .getOrCreate())

client = boto3.client('s3',aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-west-2")

srcfilePathParquetGold = "s3://" + bucket + "/" + gold_path + vendor + "/timestamp_date="+year+"-"+month+"-"+day+""
srcfilePathParquet = "s3://" + bucket + "/" + enriched_path + vendor + "/Parquet/year=" + year + "/month=" + month + "/day=" + day +""

tgtfilePathAudits = "s3://" + bucket + "/" + enriched_path + "Audits/" + vendor + "/"

dfparquetGold = sparkSession.read.format("parquet").load(srcfilePathParquetGold)

dfparquet = sparkSession.read.format("parquet").load(srcfilePathParquet)

dfparquetGold = dfparquetGold.withColumn("gold_date_created_utc",sf.from_unixtime(dfparquetGold.timestamp/1000,'YYYY-MM-dd').substr(1, 7)) \
                            .where((dfparquetGold.cipPullSuccess == "true") | (dfparquetGold.fraudPullSuccess == "true"))

dfparquet = dfparquet.withColumn("enriched_date_created_utc",sf.regexp_replace(dfparquet.createdDate,"T"," ").substr(1, 7))

# dfparquetGoldStg = dfparquetGold.groupBy(dfparquetGold.date_created_utc).count().distinct().orderBy(sf.asc("date_created_utc"))
# dfparquetGoldFinal = dfparquetGoldStg.select(dfparquetGoldStg.date_created_utc, dfparquetGoldStg.count)

dfparquetGoldStg = dfparquetGold.groupBy(dfparquetGold.gold_date_created_utc).agg(countDistinct("_id").alias("gold_cnt_id")).orderBy(sf.asc("gold_date_created_utc"))

dfparquetGoldFinal = dfparquetGoldStg.select(dfparquetGoldStg.gold_date_created_utc, dfparquetGoldStg.gold_cnt_id)

# dfparquetGoldFinal.show(1, False)

dfparquetStg = dfparquet.groupBy(dfparquet.enriched_date_created_utc).agg(countDistinct("id").alias("enriched_cnt_id")).orderBy(sf.asc("enriched_date_created_utc"))

dfparquetFinal = dfparquetStg.select(dfparquetStg.enriched_date_created_utc, dfparquetStg.enriched_cnt_id)

# dfparquetFinal.show(1, False)

dfjoin = dfparquetGoldFinal.join(dfparquetFinal,(dfparquetGoldFinal.gold_date_created_utc == dfparquetFinal.enriched_date_created_utc),'left_outer') \
                    .select(sf.lit(vendor).alias("vendor_name"), \
                    dfparquetGoldFinal.gold_date_created_utc.alias("date_created_utc"), \
                    sf.coalesce(dfparquetGoldFinal.gold_cnt_id, sf.lit(0)).alias("gold_cnt_id"), \
                    sf.coalesce(dfparquetFinal.enriched_cnt_id, sf.lit(0)).alias("enriched_cnt_id"), \
                    sf.coalesce((dfparquetGoldFinal.gold_cnt_id - dfparquetFinal.enriched_cnt_id),sf.lit(0)).alias("gold_enriched_cnt_diff"),\
                    sf.lit(current_timestamp()).alias("current_timestamp"))

# dfjoin.show(1, False)

dfjoin.write.format("parquet") \
                   .mode("overwrite") \
                   .save(tgtfilePathAudits)

redshift_s3_copy_tbl();