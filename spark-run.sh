#!/bin/bash

spark-submit --master yarn \
             --deploy-mode cluster \
             --class SparkApp \
             /path/to/example.jar