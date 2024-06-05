from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

task1 = """D:;cd actual_proj;
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 check_data_arrival.py;
"""

task2 = """D:;cd actual_proj;
spark-submir --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 spark_job_etl.py
"""

task3 = """ echo "Completely processed. No new records found" 
"""

#Initiate DAGs
dag = DAG(
    'dag',
    description='trigger spark jobs',
    schedule_interval= '0 7 * * *',
    start_date= datetime(2023, 8, 19)
)

task_1= BashOperator(
    task_id='check_data_arrival',
    bash_command=task1,
    dag=dag,
)

task_2=BashOperator(
    task_id='spark_job',
    bash_command="task2",
    dag=dag,
)

task_3 = BashOperator(
    task_id='task3',
    bash_command=task3,
    dag=dag,
)

task_1 >> [task_2, task_3]