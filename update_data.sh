#!/bin/bash -l
cd /home/andre/metrics/raapoi_metrics


# module load python/3.7.3
/nfs/home/andre/metrics/raapoi_metrics/env/bin/python /nfs/home/andre/metrics/raapoi_metrics/cluster_price.py

# copy data
cp *.csv /nfs/scratch/admin/metrics_data/

# Push data to onedrive for sharing
/home/software/apps/rclone/1.54.1/rclone --config /home/andre/.config/rclone/rclone.conf copy --progress --transfers 8 /nfs/scratch/admin/metrics_data/all_jobs_new_calc.csv hdrive:/raapoi_metrics/

# Push data to onedrive for sharing
/home/software/apps/rclone/1.54.1/rclone --config /home/andre/.config/rclone/rclone.conf copy --progress --transfers 8 /nfs/scratch/admin/metrics_data/userdates.txt hdrive:/raapoi_metrics/