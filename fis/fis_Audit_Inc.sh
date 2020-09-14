#!/bin/bash

echo $2 "- Audit - Spark execution Started"

echo "spark-submit --deploy-mode client "$1$2"/"$2"_Audit_Inc.py" $3 $4 $5 $2 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14}

spark-submit --deploy-mode client $1$2"/"$2_Audit_Inc.py $3 $4 $5 $2 $6 $7 $8 $9 ${10} ${11} ${12} ${13} ${14}

echo $2 "- Audit - Spark execution stopped"