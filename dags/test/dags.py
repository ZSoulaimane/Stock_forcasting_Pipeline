import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.hive.transfers.mysql_to_hive import MySqlToHiveOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'dezyre',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['test@example.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    dag_id='Test_Slack',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description='executing hdfs commands ',
)

print_date = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag=dag
)

SLACK_CONN = 'slack'
slack_msg = "Your task has finished"
slack_webhook = BaseHook.get_connection(SLACK_CONN).password

notify = SlackWebhookOperator(
    task_id = 'slack_notification',
    http_conn_id = 'slack',
    webhook_token = slack_webhook,
    message = slack_msg,
    username = 'airflow',
    dag=dag
)

print_date >> notify