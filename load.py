from airflow.providers.mysql.hooks.mysql import MySqlHook
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import pandas as pd
from datetime import datetime




MYSQL_CONN_ID = "my_mysql_conn"

def format_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def dump_data_to_mysql(**kwargs):
    
    records = kwargs['ti'].xcom_pull(task_ids='extract_data_from_source')

    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS Consumer_Complaints (
        complaint_id BIGINT PRIMARY KEY,
        product VARCHAR(255),
        complaint_what_happened TEXT,
        date_sent_to_company DATETIME,
        issue VARCHAR(255),
        sub_product VARCHAR(255),
        zip_code VARCHAR(20),
        tags VARCHAR(255),
        has_narrative BOOLEAN,
        timely VARCHAR(20),
        consumer_consent_provided VARCHAR(50),
        company_response VARCHAR(255),
        submitted_via VARCHAR(50),
        company VARCHAR(255),
        date_received DATETIME,
        state VARCHAR(10),
        consumer_disputed VARCHAR(50),
        company_public_response TEXT,
        sub_issue VARCHAR(255)
    );
    """
    hook.run(create_table_sql)
   
    rows = []
    for record in records:
        source = record["_source"]
        rows.append((
            source["complaint_id"],
            source["product"],
            source["complaint_what_happened"],
            format_date(source["date_sent_to_company"]),
            source["issue"],
            source["sub_product"],
            source["zip_code"],
            source["tags"],
            source["has_narrative"],
            source["timely"],
            source["consumer_consent_provided"],
            source["company_response"],
            source["submitted_via"],
            source["company"],
            format_date(source["date_received"]),
            source["state"],
            source["consumer_disputed"],
            source["company_public_response"],
            source["sub_issue"],
        ))

    hook.insert_rows(
        table="fruits",
        rows=rows,
        target_fields=[
            "complaint_id","product","complaint_what_happened",
            "date_sent_to_company","issue","sub_product","zip_code",
            "tags","has_narrative","timely","consumer_consent_provided",
            "company_response","submitted_via","company","date_received","state","consumer_disputed",
            "company_public_response","sub_issue"
        ]
    )



def dump_googlesheet():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = '/opt/airflow/dags/etl/cde-etl-aacc5ea7c754.json'

    credentials = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )

    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()

    sheet_id = '16PWUMwS-xqA_6hOY0CWjJP5pyaPG70wQAT2VZSk0Dk8'
    csv_path = '/opt/airflow/dags/etl/consumer_complaints_transformed.csv'
    df = pd.read_csv(csv_path)

    df = df.fillna("")

    values = [df.columns.tolist()] + df.values.tolist()

    body = {'values': values}

    sheet.values().clear(
        spreadsheetId=sheet_id,
        range="Sheet1"
    ).execute()

    sheet.values().update(
        spreadsheetId=sheet_id,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body=body
    ).execute()

    print("Google Sheet updated successfully")
