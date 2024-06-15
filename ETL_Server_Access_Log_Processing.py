from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#Defining DAG args ass default settings
default_args = {
    'owner': 'your_name_here',
    'start_date': days_ago(0),
    'email': ['your_email_here'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL Server Access Log Processing',
    schedule_interval=timedelta(days=1),
)

# define the download task
extract = BashOperator(
    task_id='download',
    bash_command='curl -o "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt web-server-access-log.txt" -o web-server-access-log.txt',
    dag=dag,
)

# define the task 'extract'
extract = BashOperator(
    task_id='extract',
    bash_command='cut -f1,4 -d"#" web-server-access-log.txt > /home/project/airflow/dags/extracted.txt',
    dag=dag,
)

# define the task 'transform'
transform = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /home/project/airflow/dags/extracted.txt > /home/project/airflow/dags/capitalized.txt',
    dag=dag,
)

# define the task 'load'
load = BashOperator(
    task_id='load',
    bash_command='zip log.zip capitalized.txt' ,
    dag=dag,
)

# define task pipeline
download >> extract >> transform >> load

## the following command will submit the DAG
#cp  ETL_Server_Access_Log_Processing.py $AIRFLOW_HOME/dags

## this command will verify the DAG is submitted on the web UI or the CLI
#airflow dags listWrite a DAG named ETL_Server_Access_Log_Processing.py.

Create the imports block.
Create the DAG Arguments block. You can use the default settings
Create the DAG definition block. The DAG should run daily.
Create the download task. The download task must download the server access log file which is available at the URL:
1
curl -o https://cf-courses-data.s3.us.cloud-object-s
