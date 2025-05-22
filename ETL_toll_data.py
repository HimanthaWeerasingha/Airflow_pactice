# Import airflow libraries
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

#Define DAG arguments
default_args = {
    'owner' : 'Himantha Weerasingha',
    'start_date' : datetime(2025, 5, 22, 10, 7),
    'email' : ['himantha42@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retry' : 1,
    'retry_delay' : timedelta(minutes = 5)
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args = default_args,
    description = 'Apache Airflow Final Assignment',
    schedule_interval='*/5 * * * *',
    catchup=False
)

#Task for unzip data
unzip_data = BashOperator(
    task_id = 'Unzip_data',
    bash_command = 'tar -xzf /root/airflow/dags/dataset/tolldata.tgz -C /root/airflow/dags/dataset/',
    #bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag
)

folder_path='/root/airflow/dags/dataset/'

# Task to extract data from vehicle-data.csv and save extracted data in to csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1,2,3,4 /root/airflow/dags/dataset/vehicle-data.csv > /root/airflow/dags/dataset/csv_data.csv',
    dag = dag
)

# Task to extract data from tollplaza-data.tsv and save into csv file
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = f"cut -f5-7 /root/airflow/dags/dataset/tollplaza-data.tsv | tr '\t' ',' > {folder_path}tsv_data.csv",
    dag = dag
)

# Task to extract data from payment-data.txt and save to csv file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = f"awk '{{ $1=$1; print $10\",\"$11 }}' /root/airflow/dags/dataset/payment-data.txt > {folder_path}fixed_width_data.csv",
    dag = dag
)

# Task to consolidate extracted data from  previous tasks
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = f'paste -d, {folder_path}csv_data.csv {folder_path}tsv_data.csv {folder_path}fixed_width_data.csv > {folder_path}extracted_data.csv',
    dag = dag
)

# task to transform data
transform_data = BashOperator(
    task_id = "transform_data",
    bash_command = f'tr "[:lower:]" "[:upper:]" < {folder_path}extracted_data.csv > {folder_path}transformed_data.csv',
    dag = dag
)


## Define pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data








