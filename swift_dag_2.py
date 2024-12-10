import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import re

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago

def get_latest_csv_from_bucket(**kwargs):

    # Initialize GCS hook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Source bucket name
    bucket_name = 'swift_data'
    
    # List blobs in the bucket
    blobs = gcs_hook.list(bucket_name, prefix='', delimiter='/')
    
    # Filter for CSV files and get the most recent
    csv_files = [blob for blob in blobs if blob.endswith('.csv')]
    
    if not csv_files:
        raise ValueError("No CSV files found in the bucket")
    
    # Get the most recent CSV file 
    latest_csv = max(csv_files)
    
    # Local temporary path to save the file
    local_path = f'/tmp/{os.path.basename(latest_csv)}'
    
    # Download the file
    gcs_hook.download(bucket_name=bucket_name, 
                      object_name=latest_csv, 
                      filename=local_path)
    
    # Store the file path in XCom for next task
    kwargs['ti'].xcom_push(key='input_file', value=local_path)
    
    return local_path

def process_csv(**kwargs):
    # Retrieve the input file path from XCom
    input_file = kwargs['ti'].xcom_pull(key='input_file', task_ids='get_latest_csv')
    
    # Read the CSV
    df = pd.read_csv(input_file)
    
    # Basic data validation checks
    print("Dataset Information:")
    print(df.info())
    
    # Check for missing values
    missing_values = df.isnull().sum()
    print("\nMissing Values:")
    print(missing_values)
    
    # Add anomaly detection logic
    # 1. Add anomaly column, default to 'No'
    df['anomaly'] = 'No'
    
    # 2. Future Dates Detection
    try:
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y-%m-%d %H:%M:%S')
        current_time = pd.Timestamp.now()
        future_dates_mask = df['timestamp'] > current_time
        df.loc[future_dates_mask, 'anomaly'] = 'Yes'
    except Exception as e:
        print(f"Error processing timestamp: {str(e)}")
        raise
    
    # 3. High Amount Detection (over 1 million)
    high_amount_mask = df['amount'] > 1000000
    df.loc[high_amount_mask, 'anomaly'] = 'Yes'
    
    # 4. BIC Code Formatting Validation
    def is_valid_bic(bic):
        if not isinstance(bic, str):
            return False
        
        # Regex pattern for strict BIC validation
        bic_pattern = r'^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}XXX$'
        return bool(re.match(bic_pattern, bic))
    
    # Check sender_bic
    invalid_sender_bic_mask = ~df['sender_bic'].apply(is_valid_bic)
    df.loc[invalid_sender_bic_mask, 'anomaly'] = 'Yes'
    
    # Check receiver_bic
    invalid_receiver_bic_mask = ~df['receiver_bic'].apply(is_valid_bic)
    df.loc[invalid_receiver_bic_mask, 'anomaly'] = 'Yes'
    
    # 5. Currency Validation
    valid_currencies = ['USD', 'EUR', 'JPY', 'GBP', 'CNY']
    invalid_currency_mask = ~df['currency'].isin(valid_currencies)
    df.loc[invalid_currency_mask, 'anomaly'] = 'Yes'
    
    # 6. Drop rows with missing values (as in the original function)
    df_cleaned = df.dropna()
    
    # Print anomaly summary
    print("\nAnomaly Summary:")
    print(df['anomaly'].value_counts())
    
    # Output processed file path
    output_file = f'/tmp/processed_{os.path.basename(input_file)}'
    df_cleaned.to_csv(output_file, index=False)
    
    # Store the output file path in XCom
    kwargs['ti'].xcom_push(key='processed_file', value=output_file)
    
    return output_file

def upload_to_processed_bucket(**kwargs):

    # Initialize GCS hook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Retrieve the processed file path from XCom
    processed_file = kwargs['ti'].xcom_pull(key='processed_file', task_ids='process_csv')
    
    # Destination bucket and object name
    destination_bucket = 'swift_data_processed'
    destination_object = f'processed_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Upload the file
    gcs_hook.upload(
        bucket_name=destination_bucket,
        object_name=destination_object,
        filename=processed_file
    )
    
    print(f"Uploaded {processed_file} to {destination_bucket}/{destination_object}")

def get_latest_csv_from_bucket2(**kwargs):

    # Initialize GCS hook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Source bucket name
    bucket_name = 'swift_data_processed'
    
    # List blobs in the bucket
    blobs = gcs_hook.list(bucket_name, prefix='', delimiter='/')
    
    # Filter for CSV files and get the most recent
    csv_files = [blob for blob in blobs if blob.endswith('.csv')]
    
    if not csv_files:
        raise ValueError("No CSV files found in the bucket")
    
    # Get the most recent CSV file 
    latest_csv = max(csv_files)
    
    # Local temporary path to save the file
    local_path = f'/tmp/{os.path.basename(latest_csv)}'
    
    # Download the file
    gcs_hook.download(bucket_name=bucket_name, 
                      object_name=latest_csv, 
                      filename=local_path)
    
    # Store the file path in XCom for next task
    kwargs['ti'].xcom_push(key='input_file', value=local_path)
    
    return local_path


def data_analysis_task(**kwargs):
    try:
        # Retrieve the input file path from XCom
        input_file = kwargs['ti'].xcom_pull(key='input_file', task_ids='get_processed_csv')
        
        # Check if input file exists
        if not input_file or not os.path.exists(input_file):
            raise ValueError(f"Input file not found: {input_file}")

        # Read the CSV
        df = pd.read_csv(input_file)

        # Convert timestamp - use more flexible parsing
        df['timestamp'] = pd.to_datetime(df['timestamp'], infer_datetime_format=True)

        # Filter out anomalies
        df_clean = df[df['anomaly'] == 'No']

        # Month mapping
        month_map = {
            1: 'January', 2: 'February', 3: 'March', 4: 'April',
            5: 'May', 6: 'June', 7: 'July', 8: 'August',
            9: 'September', 10: 'October', 11: 'November', 12: 'December'
        }

        # Extract month names
        df_clean['month'] = df_clean['timestamp'].dt.month.map(month_map)

        # Count monthly transaction volumes
        monthly_volume = df_clean['month'].value_counts().reindex(month_map.values(), fill_value=0)

        # Identify top 5 senders and receivers overall
        top_5_senders = df_clean['sender_bic'].value_counts().head(5)
        top_5_receivers = df_clean['receiver_bic'].value_counts().head(5)

        # Create a monthly summary DataFrame
        monthly_summary = pd.DataFrame({
            'month': monthly_volume.index,
            'monthly_transaction_volume': monthly_volume.values
        })

        # Save monthly summary and top senders/receivers in a single file
        output_file = f'/tmp/monthly_summary_{os.path.basename(input_file)}'
        with open(output_file, 'w') as f:
            # Save the monthly summary
            f.write("Monthly Transaction Volume Summary:\n")
            monthly_summary.to_csv(f, index=False)
            f.write("\n")
            
            # Append the top senders and receivers
            f.write("Top 5 Senders:\n")
            top_5_senders.to_csv(f, header=["count"])
            f.write("\n")
            
            f.write("Top 5 Receivers:\n")
            top_5_receivers.to_csv(f, header=["count"])
        
        # Push the file path to XCom
        kwargs['ti'].xcom_push(key='monthly_summary_file', value=output_file)

        print(f"Summary saved to: {output_file}")
        return output_file

    except Exception as e:
        print(f"Error in data analysis task: {e}")
        raise
    
def upload_to_final_bucket(**kwargs):

    # Initialize GCS hook
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    
    # Retrieve the file path from XCom 
    processed_file = kwargs['ti'].xcom_pull(key='monthly_summary_file', task_ids='data_analysis')
        
    # Destination bucket and object name
    destination_bucket = 'swift_data_final'
    destination_object = f'monthly_summary{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
    # Upload the file
    gcs_hook.upload(
        bucket_name=destination_bucket,
        object_name=destination_object,
        filename=processed_file
    )

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'swift_data_processing_final',
    default_args=default_args,
    description='Process Swift Data from GCS Bucket',
    schedule_interval='5 9 * * *',
    catchup=False
) as dag:
    
    # Tasks
    get_latest_csv = PythonOperator(
        task_id='get_latest_csv',
        python_callable=get_latest_csv_from_bucket,
        provide_context=True
    )
    
    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv,
        provide_context=True
    )
    
    upload_to_processed = PythonOperator(
        task_id='upload_to_processed_bucket',
        python_callable=upload_to_processed_bucket,
        provide_context=True
    )
    
    get_processed_csv = PythonOperator(
        task_id='get_processed_csv',
        python_callable=get_latest_csv_from_bucket2,
        provide_context=True
    )

    data_analysis = PythonOperator(
        task_id='data_analysis',
        python_callable=data_analysis_task,
        provide_context=True
    )

    upload_to_final_bucket_task = PythonOperator(
        task_id='upload_to_final_bucket',
        python_callable=upload_to_final_bucket,
        provide_context=True
    )
    # Task dependencies
    get_latest_csv >> process_csv_task >> upload_to_processed >> get_processed_csv >> data_analysis >> upload_to_final_bucket_task