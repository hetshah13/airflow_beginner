from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag2",
    description ='A Bit-Shift Method for task dependency',
    start_date= datetime(2024,9,27,5),
    schedule_interval = '@daily'
    ) as dag:
    task1 = BashOperator(
        task_id = 'First_Task',
        bash_command = "echo Hey! this is first task "
    )
    task2 =BashOperator(
        task_id = 'second_task',
        bash_command = "echo its second task"
    )

    task3 = BashOperator(
        task_id = 'Third_task',
        bash_command = 'echo this is third task !'
    )

    task1 >> task2 >> task3