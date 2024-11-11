#!/bin/bash

#TODO: introduce parameters, like table name, partitions
sbt package && /home/harry/apps/spark-3.3.1-bin-hadoop3/bin/spark-submit \
  --class "example.ReadPGJDBC" \
  --master "local"\
  --conf spark.eventLog.enabled=true \
  --num-executors 1 \
  --executor-cores 1 \
  /home/harry/workspace/spark3io/target/spark3io-1.0.jar