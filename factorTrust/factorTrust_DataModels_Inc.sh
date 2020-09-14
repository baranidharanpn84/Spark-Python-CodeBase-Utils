#!/bin/bash

echo $2 "- DataModels - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_DataModels_Inc.py" $3 $4 $5 $2 $6 $7

spark-submit --deploy-mode client $1$2"/"$2_DataModels_Inc.py $3 $4 $5 $2 $6 $7

echo $2 "- DataModels - Spark execution stopped"