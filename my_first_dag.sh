#!/bin/bash
echo "extract_transform_and_load"
cut -d":" -f1,3,6 /etc/passwd > /home/project/airflow/dags/extracted-data.txt

tr ":" "," < /home/project/airflow/dags/extracted-data.txt > /home/project/airflow/dags/transformed-data.csv
