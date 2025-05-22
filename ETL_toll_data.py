# Import airflow libraries
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

#Define DAG arguments
default_args = {
    'owner' : 'Himantha Weerasingha',
    'start_date' : days_ago(0),
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
    schedule_interval=timedelta(days=1)
)

#Task for unzip data
unzip_data = BashOperator(
    task_id = 'Unzip_data',
    bash_command = 'tar -xzf <file location for the zip file>',
    #bash_command = 'tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz',
    dag = dag
)

# Task to extract data from vehicle-data.csv and save extracted data in to csv file
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1,2,3,4 vehicle-data.csv >> csv_data.csv',
    dag = dag
)

# Task to extract data from tollplaza-data.tsv and save into csv file
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = "cut -f5-7 tollplaza-data.tsv | tr '\t' ','>> tsv_data.csv",
    dag = dag
)

# Task to extract data from payment-data.txt and save to csv file
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = "awk '{ $1=$1; print $10\",\"$11 }' payment-data.txt >> fixed_width_data.csv",
    dag = dag
)

# Task to consolidate extracted data from  previous tasks
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d, csv_data.csv tsv_data.csv fixed_width_data.csv >> extracted_data.csv',
    dag = dag
)

# task to transform data
transform_data = BashOperator(
    task_id = "transform_data",
    bash_command = 'tr "[:lower:]" "[:upper:]" < extracted_data.csv > transformed_data.csv',
    dag = dag
)


## Define pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data








