#!/bin/bash
echo "extract_transform_load"
# cut command to extract the fields timestamp and visitorid writes to a new file extracted.txt
cut -f1,4 -d"#" /home/project/airflow/dags/web-server-access-log.txt > /home/project/airflow/dags/extracted.txt

# tr command to transform by capitalizing the visitorid.
tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt

# tar command to compress the extracted and transformed data.
tar -czvf /home/project/airflow/dags/log.tar.gz /home/project/airflow/dags/capitalized.txt
