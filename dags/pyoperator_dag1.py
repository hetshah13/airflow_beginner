from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def greet(name, ti):
    FirstName = ti.xcom_pull(task_ids ='get_name', key ='FirstName')
    a = ti.xcom_pull(task_ids ='get_name', key ='LastName')
    print(f"Hello! {FirstName} {a}...,",f"Have a good day {name}")

def get_name(ti):
    ti.xcom_push(key='FirstName', value ='Het')
    ti.xcom_push(key='LastName', value ='Shah')


with DAG(
    dag_id="Py_DAG_v2",
    description ='Python Operator Dag 1',
    start_date= datetime(2024,9,27,5),
    schedule_interval = '@daily'
    ) as dag:
    Task1 = PythonOperator(
        task_id = "greeting",
        python_callable = greet,
        op_kwargs = {'name': 'Het'}
    )

    Task2 = PythonOperator(
        task_id = "get_name",
        python_callable = get_name)
    
    Task2 >> Task1