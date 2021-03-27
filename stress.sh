#!/bin/bash

# Check an argument was given
if [ -z ${1+x} ]; then 
  echo "Please pass an input file";
  exit 1
fi

# Check the target file exists
if [ ! -f stress_${1}/${1}.csv ]; then
   echo "File ${1}.csv does not exist."
   exit 1
fi

rm -rf .docker-data/stats/${1}.csv

{
  read
  while IFS=, read -r producer_replicas producer_batch_serial producer_batch_parallel producer_max_partitions consumer1_replicas consumer1_batch_parallel consumer2_replicas consumer2_batch_parallel
  do
    STRESS_PRODUCER_REPLICAS=$producer_replicas \
    STRESS_PRODUCER_BATCH_SERIAL=$producer_batch_serial \
    STRESS_PRODUCER_BATCH_PARALLEL=$producer_batch_parallel \
    STRESS_PRODUCER_MAX_PARTITIONS=$producer_max_partitions \
    STRESS_CONSUMER1_REPLICAS=$consumer1_replicas \
    STRESS_CONSUMER1_BATCH_PARALLEL=$consumer1_batch_parallel \
    STRESS_CONSUMER2_REPLICAS=$consumer2_replicas \
    STRESS_CONSUMER2_BATCH_PARALLEL=$consumer2_batch_parallel \
    make ${1}-run
  done 
} < stress_${1}/${1}.csv