#!/bin/bash -l
cd /home/andre/metrics/cluster_price

# module load python/3.7.3
/nfs/home/andre/metrics/cluster_price/env/bin/python /nfs/home/andre/metrics/cluster_price/cluster_price.py

# copy data
cp *.csv /nfs/scratch/admin/metrics_data/
