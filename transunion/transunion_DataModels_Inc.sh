#!/bin/bash

echo $2 "- DataModels - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Spark execution stopped"

echo $2 "- DataModels - tradeLine - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_tradeLine_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_tradeLine_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - tradeLine - Spark execution stopped"

echo $2 "- DataModels - syntheticFraud - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_syntheticFraud_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_syntheticFraud_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - syntheticFraud - Spark execution stopped"

echo $2 "- DataModels - Stage hRFA - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Stage_hRFA_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Stage_hRFA_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Stage hRFA - Spark execution stopped"

echo $2 "- DataModels - hRFA - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_hRFA_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_hRFA_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - hRFA - Spark execution stopped"

echo $2 "- DataModels - Stage CC1 - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Stage_CC_1_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Stage_CC_1_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Stage CC1 - Spark execution stopped"

echo $2 "- DataModels - Stage CC2 - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Stage_CC_2_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Stage_CC_2_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Stage CC2 - Spark execution stopped"

echo $2 "- DataModels - Stage CC3 - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Stage_CC_3_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Stage_CC_3_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Stage CC3 - Spark execution stopped"

echo $2 "- DataModels - creditorContact - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_creditorContact_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_creditorContact_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - creditorContact - Spark execution stopped"

echo $2 "- DataModels - inquiry - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_inquiry_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_inquiry_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - inquiry - Spark execution stopped"

echo $2 "- DataModels - riskRun - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_riskRun_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_riskRun_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - riskRun - Spark execution stopped"

echo $2 "- DataModels - collection - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_collection_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_collection_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - collection - Spark execution stopped"
