from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Default arguments
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hello_world_dag_from_git',
    default_args=default_args,
    description='A Hello World DAG synced from Git',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['example', 'git-sync'],
)

def print_hello():
    """Simple function to print hello message"""
    print("Hello World from Git-synced DAG!")
    print("This DAG is automatically synced from GitHub")
    return "Git sync working successfully"

def print_git_info():
    """Function to print git sync info"""
    print("This DAG demonstrates GitSync functionality")
    print("DAGs are automatically pulled from GitHub repository")
    return "GitSync demo completed"

# Task 1: Python operator
hello_task = PythonOperator(
    task_id='hello_git_task',
    python_callable=print_hello,
    dag=dag,
)

# Task 2: Bash operator
bash_task = BashOperator(
    task_id='bash_git_task',
    bash_command='echo "Hello from Git-synced Bash task!" && echo "Repository sync working!"',
    dag=dag,
)

# Task 3: Git info task
git_info_task = PythonOperator(
    task_id='git_info_task',
    python_callable=print_git_info,
    dag=dag,
)

# Set task dependencies
hello_task >> bash_task >> git_info_task