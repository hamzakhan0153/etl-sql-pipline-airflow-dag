from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from etl.extract import extract_data_from_source
from etl.load import dump_data_to_mysql,dump_googlesheet
from etl.transform import transform_data
from airflow.sensors.filesystem import FileSensor
from airflow.providers.smtp.operators.smtp import EmailOperator




with DAG(
    dag_id = 'consumer_financial_etl',
    description="Airflow Orchestration for Consumer Financial Complaints",
    schedule= '0 0 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kai", "batch9"]
) as dag:
    
    
    
    extract_task = PythonOperator(
        task_id = 'extract_data_from_source',
        python_callable=extract_data_from_source

    )

    dump_mysql_task = PythonOperator(
        task_id='dump_data_to_mysql',
        python_callable=dump_data_to_mysql
    )

    transform_task = PythonOperator(
        task_id = 'transform_data',
        python_callable=transform_data
    )

    check_file_task = FileSensor(
        task_id='check_consumer_financial_csv',
        filepath='/opt/airflow/dags/etl/consumer_complaints_transformed.csv',
        poke_interval=10,
        timeout=300,
        fs_conn_id='fs_default'
    )

    load_task = PythonOperator(
        task_id = 'dump_googlesheet',
        python_callable=dump_googlesheet
    )

    send_email_task = EmailOperator(
        task_id = 'send_googlesheet_url_via_email',
        to="hamzakhan69g@gmail.com",                   # recipient email
        subject="Airflow 3 EmailOperator Test",     # email subject
        html_content="<h3>Hello from Airflow 3!  here is the pdf link: https://docs.google.com/spreadsheets/d/16PWUMwS-xqA_6hOY0CWjJP5pyaPG70wQAT2VZSk0Dk8/edit?gid=0#gid=0   </h3>",  # email body
        conn_id="smtp_default"                      # make sure this connection is set correctly
    )

    extract_task >> dump_mysql_task >> transform_task >> check_file_task >> load_task >> send_email_task