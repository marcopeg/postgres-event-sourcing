#!/bin/bash

rm -rf .docker-data/stats

{
  read
  while IFS=, read -r producer_replicas producer_batch_serial producer_batch_parallel producer_max_partitions consumer1_replicas consumer1_batch_parallel consumer2_replicas consumer2_batch_parallel
  do
    PARTITIONS_PRODUCER_REPLICAS=$producer_replicas \
    PARTITIONS_PRODUCER_BATCH_SERIAL=$producer_batch_serial \
    PARTITIONS_PRODUCER_BATCH_PARALLEL=$producer_batch_parallel \
    PARTITIONS_PRODUCER_MAX_PARTITIONS=$producer_max_partitions \
    PARTITIONS_CONSUMER1_REPLICAS=$consumer1_replicas \
    PARTITIONS_CONSUMER1_BATCH_PARALLEL=$consumer1_batch_parallel \
    PARTITIONS_CONSUMER2_REPLICAS=$consumer2_replicas \
    PARTITIONS_CONSUMER2_BATCH_PARALLEL=$consumer2_batch_parallel \
    make partitions-run
  done 
} < partitions.csv