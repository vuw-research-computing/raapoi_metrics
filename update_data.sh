#!/bin/bash -l
cd /home/andre/metrics/raapoi_metrics


# module load python/3.7.3
/nfs/home/andre/metrics/raapoi_metrics/env/bin/python /nfs//home/andre/metrics/raapoi_metrics/cluster_price.py

# copy data
cp *.csv /nfs/scratch/admin/metrics_data/
