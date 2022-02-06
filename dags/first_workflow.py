from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
#from airflow.operators.sftp_operator import SFTPOperator
from airflow.operators.mysql_operator import MySqlOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.apache.hive.transfers.mysql_to_hive import MySqlToHiveOperator
from dag_utils.slack import alert_slack_channel, slack_dag_end
from airflow.utils.trigger_rule import TriggerRule

from epandas import launchDay
from epandas import launchWeek
from epandas import launchMonth
from epandas import launchYear
from BuildingDBS import launchdb
from BuildingDBSDay import launchdbday
from BuildingDBSWeek import launchdbweek
from BuildingDBSMonth import launchdbmonth
from BuildingDBSYear import launchdbyear

import os;
from datetime import datetime



ROOT_PATH = os.getcwd()



def _branch(arg = 'day'):
    switcher = { 'day' : 'ExtractDay',
                 'week' : 'ExtractWeek',
                 'month' : 'ExtractMonth',
                 'year' : 'ExtractYear'}
    return switcher.get(arg, "invalid timeframe")



def _start():
    desc = os.path.isfile(ROOT_PATH + '/airflow/data/stockdb.sql')
    if (desc): return 'Trigger_branch'
    return "ExtractStock"

def my_fun():
    print("---------------------")
    print("THIS IS A SLACK MESSAGE DAG")
    #raise an error to trigger slack alert
    raise Exception('ERROR in the pipeline!') 
    


default_args = {
            'owner': 'Airflow',
            'start_date': datetime(2021, 1 ,1),
            'email_on_failure'      : False,
            'email_on_retry'        : False,
            'retries'               : 0,
            'on_failure_callback': alert_slack_channel
}   
    



with DAG(dag_id='Work', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    
    
    # EXTRACTING
    extract_stock = PythonOperator(
        task_id = "ExtractStock",
        python_callable = launchDay,
        op_kwargs = {"start" : "2021-10-01", "end" : "2021-12-31"},
        provide_context = True
    )
        
        
    extract_day = PythonOperator(
        task_id = "ExtractDay",
        python_callable = launchDay,
        op_kwargs = {"start" : "2021-12-01", "end" : "2021-12-07"},
        provide_context = True
    )
    
    
    extract_week = PythonOperator(
        task_id = "ExtractWeek",
        python_callable = launchWeek,
        op_kwargs = {"start" : "2020-12-01", "end" : "2020-12-07"},
        provide_context = True
    )
        
        
    extract_month = PythonOperator(
        task_id = "ExtractMonth",
        python_callable = launchMonth,
        op_kwargs = {"start" : "2020-12-01", "end" : "2020-12-07"},
        provide_context = True
    )


    extract_year = PythonOperator(
        task_id = "ExtractYear",
        python_callable = launchYear,
        op_kwargs = {"start" : "2020-12-01", "end" : "2020-12-07"},
        provide_context = True
    )
    
    # Uploading data_base
    
    Stock_DB = PythonOperator(
        task_id = "StockDB",
        python_callable = launchdb,
        provide_context = True
    )

    upload_day = PythonOperator(
        task_id = "uploaday",
        python_callable = launchdbday,
        provide_context = True
    )

    upload_week = PythonOperator(
        task_id = "uploadweek",
        python_callable = launchdbweek,
        provide_context = True
    )

    upload_month = PythonOperator(
        task_id = "uploadmonth",
        python_callable = launchdbmonth,
        provide_context = True
    )

    upload_year = PythonOperator(
        task_id = "uploadyear",
        python_callable = launchdbyear,
        provide_context = True
    )

    # Dump Database
    dump_base = BashOperator(
        task_id='dump_database',
        bash_command='mysqldump -u root stockdb > '+ ROOT_PATH+ '/airflow/data/stockdb.sql',
        trigger_rule = 'none_failed_or_skipped'
    )



    # Existance of SQL File
    # check_files = BashOperator(
    #     task_id='file_exist',
    #     bash_command='cd ' + ROOT_PATH+ '/airflow/data/; ls'
    # )
    
    # check_file = BashOperator(
    #     task_id='check_file',
    #     bash_command='ls'
    # )
    
    
    
    #Trigger
    choose_branch = BranchPythonOperator(
        task_id = 'Trigger_branch',
        python_callable = _branch,
        trigger_rule = 'none_failed_or_skipped'
    )
    
    
    
    check_choose = BranchPythonOperator(
        task_id = 'check_choose',
        python_callable = _start,
        trigger_rule = 'none_failed_or_skipped'
    )
    
    
    
    # Data Ingestion
    # Ingestion = BashOperator(
    #     task_id='Ingestion_step',
    # #    bash_command='sqoop import --connect jdbc:mysql://localhost:3306/stockdb --username soule --password soule --table stock --hive-import'
    #     bash_command = 'sqoop import-all-tables --connect jdbc:mysql://localhost:3306/stockdb --username soule --password soule --hive-database stockdb --hive-import'
    # )
    
    Ingestion_3 = BashOperator(
        task_id='Ingestion_step3',
        bash_command = 'sqoop import --connect jdbc:mysql://localhost:3306/stockdb --username soule --password soule -hive-database stockdb --table {} --hive-import'.format('baseday')
    )

    Ingestion_2 = BashOperator(
        task_id='Ingestion_step2',
        bash_command = 'sqoop import --connect jdbc:mysql://localhost:3306/stockdb --username soule --password soule -hive-database stockdb --table {} --hive-import'.format('dateday')
    )
    
    Ingestion_stock = BashOperator(
        task_id='Ingest_stock',
        bash_command = 'sqoop import --connect jdbc:mysql://localhost:3306/stockdb --username soule --password soule -hive-database stockdb --table {} --hive-import'.format('stock')
    )
    
    
    
    
    
    # Fail/Succed Message
#     email = EmailOperator(
#         task_id='send_email',
#         to='Soulaimane.studies@gmail.com',
#         subject='Airflow Alert',
#         html_content=""" <h3>Email Test</h3> """,
#         dag=dag
# )
    
    # Transfer File to Hadoop sandbox
    
    # transfer = SFTPOperator(
    #     task_id="test_sftp",
    #     ssh_conn_id="ssh_default",
    #     local_filepath="ROOT_PATH+ '/airflow/data/aymane.sql",
    #     remote_filepath="/home/maria_dev/aymane.sql",
    #     operation="put",
    #     create_intermediate_dirs=True,
    #     dag=dag
    # )
    
    # ssh_conn = SSHOperator(
    #     ssh_conn_id='ssh_default',
    #     task_id='test_ssh',
    #     command="""
    #         echo 'Hello World'
    #         """
    # )

    # hdp_loading = PythonOperator(
    #     task_id = "launch",
    #     python_callable = launch,
    #     provide_context = True
    # )
    
    
    #Slack System
    slack_end = PythonOperator(
        task_id='Notify_Slack_DAG_success',
        python_callable=slack_dag_end,
        trigger_rule=TriggerRule.ALL_SUCCESS) #only triggers if all previous connecting tasks finish successfully




    
   # check_choose >> Stock_DB >> choose_branch >> [extract_day, extract_week, extract_month, extract_year]
    
    check_choose >> [extract_stock, choose_branch]
    
    
    extract_stock >> Stock_DB >> Ingestion_stock >> choose_branch >> [extract_day, extract_week, extract_month, extract_year]
    
    
    extract_day >> upload_day >> dump_base
    extract_week >> upload_week >> dump_base
    extract_month >> upload_month >> dump_base
    extract_year >> upload_year >> dump_base
    
    
    dump_base >> Ingestion_2 >> Ingestion_3 >> slack_end
    
  #  check_file >> Stock_DB >> data_uploading >> run_this >> check_files >> t1
    # check_file >> data_cleaning >> [data_uploading , check_files] 