"# This repository include several apache air flow practice projects. each project develop in new branch" 
"ETL airflow using python operator" 


Airflow project structure should looks like this

airflow_home/

│

├── dags/

│   ├── my_etl_dag.py            <-- Your DAG file

│   ├── etl_utils/               <-- Custom module for reusable functions

│   │   ├── __init__.py          <-- Marks this folder as a module

│   │   ├── datasets/            <-- folder to save outputs and save download

│   │   └── my_etl_functions.py  <-- Your ETL logic (Python callables)
