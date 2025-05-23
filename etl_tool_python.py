
# Import airflow libraries and other neccessary libraries

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from etl_utils import etl_functions


## Setup DAG arguments
default_args = {
    'owner' : 'Himantha Weerasingha',
    'start_date' : days_ago(0),
    'email' : ['himantha42@gmail.com'],
    'retries' : 1,
    'retry_delay' : timedelta(minutes = 5)
}


dag = DAG(
    'ETL_too_data_Python',
    default_args = default_args,
    description = "ETL pipeline which use python operator",
    schedule_interval = timedelta(minutes = 10),
    catchup=False
)

## Define tasks

# Task to download dataset
download_task = PythonOperator(
    task_id = 'download_dataset',
    python_callable=etl_functions.download_dataset,
    ops_args = ['https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'],
    dag=dag
)

# Task to untar dataset
untar_task = PythonOperator(
    task_id = 'unzip_data',
    python_callable = etl_functions.untar_dataset,
    dag=dag
)

# Task to extract data from csv file
extract_csv_task = PythonOperator(
    task_id = 'extract_data_from_csv',
    python_callable = etl_functions.extract_data_from_csv,
    dag=dag
)

# Task to extract data from tsv file
extract_tsv_task = PythonOperator(
    task_id = 'extract_data_from_tsv',
    python_callable = etl_functions.extract_data_from_tsv,
    dag=dag
)

# Task to extract data from fixed width file
extract_fixedwidth_task = PythonOperator(
    task_id = 'extract_data_from_fixed_width',
    python_callable = etl_functions.extract_data_from_fixed_width,
    dag=dag
)

# Task to consolidate data
consolidate_task = PythonOperator(
    task_id = 'consolidate_data',
    python_callable = etl_functions.consolidate_data,
    dag = dag
)

transform_task = PythonOperator(
    task_id = 'transform_data',
    python_callable = etl_functions.transform_data,
    dag = dag
)

## Define pipeline
download_task >> untar_task >> extract_csv_task >> extract_tsv_task >> extract_fixedwidth_task >> consolidate_task >> transform_task



