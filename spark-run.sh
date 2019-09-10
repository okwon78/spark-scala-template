#!/bin/bash

spark-submit --master yarn \
             --deploy-mode cluster \
             --class SparkApp \
             /home/hadoop/spark-scala-template/spark-scala-template-assembly-0.1.jar 
