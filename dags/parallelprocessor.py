from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.provider.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import S3ToSnowflakeOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import json
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False, # if the task is dependent on the previous task
    'email_on_failure': False, # if the task fails, send an email
    'email_on_retry': False, # if the task is retried, send an email
    'retries': 3, # number of retries the task will do before failing
    'retry_delay': timedelta(minutes=10), # retry delay means if the task fails, it will retry after 10 minutes
    'concurrency': 2, # number of tasks to run at a time
    'max_active_runs': 1 # number of dags to run at a time
}

with DAG(
    'dynamic_web_scraping_to_snowflake',
    default_agrs=default_args,
    description='Robust and complex end-to-end web scraping pipeline DAG with Dynamic Task Management',
    schedule_interval=timedelta(days=1), # schedule interval of 1 day: it will run once a day
    start_date=datetime(2023, 8, 29), # start date of the DAG
    catchup=False, # if the DAG should catchup or not: if True, it will run all the tasks that it missed
) as dag:
    start = DummyOperator(task_id='start'),
    api_endpoints = ['api/v1/data1', 'api/v1/data2', 'api/v1/data3']

    # The line retrieves the data returned from a previous task that made an API call. The task ID is dynamically generated based on the API endpoint, allowing for flexible task management in the DAG. The retrieved data is then processed further in the extract_data_from_api function.

    def extract_data_from_api(api_endpoint, **kwargs): # kwargs are the arguments that are passed to the function
        response = kwargs['ti'].xcom_pull(task_ids=f'call_api_{api_endpoint}') # pull the response from the task
        # shorthand for Task Instance. It is an object provided by Airflow that contains information and methods related to the current task instance, including the ability to push and pull XComs (cross-communication messages between tasks).

        # xcom_pull: method is used to retrieve data (known as XComs) from a different task. XComs (short for “cross-communications”) are a mechanism that allows tasks to share information with each other in Airflow.
        data = json.loads(response) # convert the response to json
        return data
    
    call_api = SimpleHttpOperator(
        task_id='call_api',
        method='GET',
        http_conn_id='my_api_connection',
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.text, # A lambda function that processes the response from the API. In this case, it extracts the response text.
        log_response=True
    ).expand(endpoint=api_endpoints) # expand is used to dynamically create multiple tasks from a single operator. It creates one task per API endpoint in the api_endpoints list. Iterates over the api_endpoints list and generates separate tasks for each endpoint, effectively making an API call to each one.

    extract_data = PythonOperator.partial( # partial allows you to define a template for the task but not fully instantiate it yet. It’s like setting up a base configuration that will be finalized later with additional details
        task_id='extract_data',
        python_callable=extract_data_from_api,
        provide_context=True
    ).expand(op_kwargs=[{'api_endpoint': endpoint} for endpoint in api_endpoints]) # specifies the keyword arguments that will be passed to each instance of the task. Here, a list comprehension is used to create a dictionary for each endpoint, with the key 'api_endpoint' and the corresponding endpoint URL as the value

    # Expand method expects a list of dictionaries for op_kwargs, where each dictionary corresponds to the keyword arguments to be passed to a specific task instance.

    # When each task is executed, the dictionary {'api_endpoint': endpoint} is unpacked, and the value of endpoint is passed to the api_endpoint parameter in extract_data_from_api.

    def normalize_data(**kwargs):
        data_list = []
        for endpoint in api_endpoints:
            data = kwargs['ti'].xcom_pull(task_ids=f'extract_data')
            df = pd.json_normalize(data)
            data_list.append(df)
        
        combined_df = pd.concat(data_list, ignore_index=True)

        # data validation
        if combined_df.empty:
            raise ValueError('No data available')
        
        normalized_file_path = '/tmp/normalized_data.csv' # path to save the normalized data
        combined_df.to_csv(normalized_file_path, index=False) # save the data to a csv file

        return normalized_file_path
    
    normalize_data = PythonOperator(
        task_id = 'normalize_data',
        python_callable = normalize_data, # call the normalize_data function
        provide_context=True # provide the context to the function: the context contains information about the task instance
    )

    def partitioned_upload_to_s3(normalized_data_path, **kwargs):
        partition_folder = f'data/year={datetime.now().year}/month={datetime.now().month}/day={datetime.now().day}'
        s3_key = f'{partition_folder}/normalized_data.csv'
        return s3_key
    
    partitioned_upload_to_s3_task = PythonOperator(
        task_id='partitioned_upload_to_s3',
        python_callable=partitioned_upload_to_s3,
        op_kwargs={'normalized_data_path': '/tmp/normalized_data.csv'}, # pass the normalized data path to the function
    )

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id='upload_to_s3',
        src='/tmp/normalized_data.csv',
        dest='my_s3_bucket',
        key="{{ ti.xcom_pull(task_ids='partitioned_upload_to_s3') }}", # retrieve the S3 key from the previous task
        aws_conn_id='my_aws_connection'
    )

    load_into_snowflake = S3ToSnowflakeOperator(
        task_id='load_into_snowflake',
        snowflake_conn_id='my_snowflake_connection', # connection to Snowflake: it is defined in the Airflow UI
        s3_key = "{{ ti.xcom_pull(task_ids='partitioned_upload_to_s3') }}", # retrieve the S3 key from the previous task
        table='my_table',
        schema='my_schema',
        stage= 'my_stage', # Snowflake stage where the data will be loaded
        file_format='(TYPE=CSV, FIELD_OPTIONALLY_ENCLOSED_BY=\'"\', SKIP_HEADER=1)', # file format of the data: CSV with a header
    )

    def cleanup_temp_files(**kwargs):
        os.remove('/tmp/normalized_data.csv') # remove the normalized data file

    cleanup_task = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_temp_files # call the cleanup_temp_files function
    )

    success_notification = EmailOperator(
        task_id='success_notification',
        to='your_email@example.com',
        subject='DAG {{dag.dag_id}} Succeeded',
        html_content='The DAG {{dag.dag_id}} has completed successfully.',
        trigger_rule='all_success' # trigger the task only if all the tasks are successful
    )

    failure_notification = EmailOperator(
        task_id='failure_notification',
        to='your_email@example.com',
        subject='DAG {{dag.dag_id}} Failed',
        html_content='The DAG {{dag.dag_id}} has failed.',
        trigger_rule='one_failed' # trigger the task if any of the tasks fail
    )
    

    end = DummyOperator(task_id='end') # end task: it is a dummy task that signifies the end of the DAG

    start >> call_api >> extract_data >> normalize_data
    normalize_data >> partitioned_upload_to_s3_task >> upload_to_s3 >> load_into_snowflake
    load_into_snowflake >> cleanup_task
    cleanup_task >> success_notification
    cleanup_task >> end

    start >> failure_notification
