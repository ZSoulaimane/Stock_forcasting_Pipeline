from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from dag_utils.slack import alert_slack_channel, slack_dag_end
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash_operator import BashOperator

default_args = {
'owner'                 : 'airflow',
'description'           : 'slack testing',
'depend_on_past'        : False,
'start_date'            : datetime(2021, 10, 1),
'email_on_failure'      : False,
'email_on_retry'        : False,
'retries'               : 0,
'on_failure_callback': alert_slack_channel
}


def my_fun():
    print("---------------------")
    print("THIS IS A SLACK MESSAGE DAG")
    #raise an error to trigger slack alert
    raise Exception('ERROR in the pipeline!') 

with DAG('slack_notify',
         default_args=default_args,
         schedule_interval="0 5 * * *",
         catchup=False) as dag:
     
    t0 = PythonOperator(
            task_id="dummy",
            python_callable=my_fun
            )
    
    date = BashOperator(
    task_id = 'print_date',
    bash_command = 'date',
    dag=dag
)
    
    slack_end = PythonOperator(
            task_id='Notify_Slack_DAG_success',
            python_callable=slack_dag_end,
            trigger_rule=TriggerRule.ALL_SUCCESS) #only triggers if all previous connecting tasks finish successfully

t0 >> date >> slack_end