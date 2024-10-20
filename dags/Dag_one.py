from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag1",
    description ='My First dag - Bash Operator',
    start_date= datetime(2024,10,19,5),
    schedule_interval = '@daily'
    ) as dag:
    task1 = BashOperator(
        task_id = 'First_Task',
        bash_command = "echo Hey! there "
    )
    task2 =BashOperator(
        task_id = 'second_task',
        bash_command = "echo its second task"
    )

    task3 = BashOperator(
        task_id = 'Third_task',
        bash_command = 'echo this is third task !'
    )

    task1.set_downstream(task2)
    task1.set_downstream(task3)