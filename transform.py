from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import os


MYSQL_CONN_ID = "my_mysql_conn"

def transform_data():
    hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    df = hook.get_pandas_df(sql='SELECT * FROM fruits')
    
    print(f"DEBUG: Found {len(df)} rows of data")
    
    if not df.empty:
        df = df.drop(columns=['complaint_what_happened','date_sent_to_company','zip_code','tags','has_narrative','consumer_consent_provided','consumer_disputed','company_public_response'], errors='ignore')
        df['date_received'] = df['date_received'].dt.strftime('%m-%Y')
        df = df.groupby(['product','issue','sub_product','timely','company_response','submitted_via','company','date_received','state','sub_issue'],
                   dropna=False
                   )['complaint_id'].nunique().reset_index(name='common_complaints')
        
        base_path = os.path.dirname(__file__) 
        save_path = os.path.join(base_path, 'consumer_complaints_transformed.csv')
        
        df.to_csv(save_path, index=False)
        print("File saved")
        return "Success"
    else:
        print("No data")
        return "No Data"

