#!/bin/bash -l
cd /home/andre/metrics/raapoi_metrics


# module load python/3.7.3
/nfs/home/andre/metrics/raapoi_metrics/env/bin/python /nfs//home/andre/metrics/raapoi_metrics/cluster_price.py

# copy data
cp *.csv /nfs/scratch/admin/metrics_data/

# Push data to onedrive for sharing
rclone copy --progress --transfers 8 /nfs/scratch/admin/metrics_data/all_jobs_new_calc.csv hdrive:/raapoi_metrics/
