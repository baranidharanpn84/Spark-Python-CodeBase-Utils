#!/bin/bash

echo $2 "- DataModels - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Spark execution stopped"

echo $2 "- DataModels - AddrInfo - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_addrInfo_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_addrInfo_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - AddrInfo - Spark execution stopped"

echo $2 "- DataModels - consumerIdentity - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_consumerIdentity_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_consumerIdentity_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - consumerIdentity - Spark execution stopped"

echo $2 "- DataModels - empInfo - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_empInfo_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_empInfo_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - empInfo - Spark execution stopped"

echo $2 "- DataModels - fraudServices - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_fraudServices_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_fraudServices_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - fraudServices - Spark execution stopped"

echo $2 "- DataModels - infoMsg - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_infoMsg_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_infoMsg_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - infoMsg - Spark execution stopped"

echo $2 "- DataModels - inquiry - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_inquiry_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_inquiry_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - inquiry - Spark execution stopped"

echo $2 "- DataModels - publicRecord - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_publicRecord_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_publicRecord_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - publicRecord - Spark execution stopped"

echo $2 "- DataModels - riskModel - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_riskModel_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_riskModel_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - riskModel - Spark execution stopped"

echo $2 "- DataModels - SSN - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_SSN_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_SSN_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - SSN - Spark execution stopped"

echo $2 "- DataModels - statement - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_statement_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_statement_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - statement - Spark execution stopped"

echo $2 "- DataModels - tradeLine - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_tradeLine_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_tradeLine_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - tradeLine - Spark execution stopped"

echo $2 "- DataModels - riskRun - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_riskRun_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_riskRun_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - riskRun - Spark execution stopped"