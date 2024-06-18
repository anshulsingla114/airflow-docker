from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import googleapiclient.discovery
import pandas as pd
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Placeholder function for data extraction
def extract_data():
    try:
        # Initialize YouTube Data API client
        youtube = googleapiclient.discovery.build('youtube', 'v3', developerKey='AIzaSyAcBIKEVB4ZN3u09_eSL21qb9QlwdYVAaY')

        # Make API request to retrieve comments from a specific video
        request = youtube.commentThreads().list(
            part='snippet',
            videoId='i_23KUAEtUM',
            maxResults=100
        )
        response = request.execute()

        # Process response and extract relevant data
        comments = [item['snippet']['topLevelComment']['snippet']['textDisplay'] for item in response['items']]

        logging.info("Data extraction successful")
        return comments

    except Exception as e:
        logging.error(f"Error in extract_data: {str(e)}")
        raise

# Placeholder function for data transformation
def transform_data(ti):
    try:
        # Retrieve the extracted data passed by Airflow
        extracted_data = ti.xcom_pull(task_ids='extract_data_task')
        if not extracted_data:
            raise ValueError("No data received from extract_data_task")

        # Transform the extracted data (e.g., convert to DataFrame, perform operations)
        transformed_data = pd.DataFrame(extracted_data, columns=['comment'])

        # Placeholder: Add any transformation logic here (e.g., cleaning, aggregation, etc.)
        transformed_data['comment_length'] = transformed_data['comment'].apply(len)

        logging.info("Data transformation successful")
        # Convert DataFrame to list of dictionaries for XCom
        return transformed_data.to_dict(orient='records')

    except Exception as e:
        logging.error(f"Error in transform_data: {str(e)}")
        raise

# Placeholder function for data loading
def load_to_csv(ti):
    try:
        # Retrieve the transformed data passed by Airflow
        transformed_data = ti.xcom_pull(task_ids='transform_data_task')
        if not transformed_data:
            raise ValueError("No data received from transform_data_task")

        # Convert the list of dictionaries back to a DataFrame
        transformed_df = pd.DataFrame(transformed_data)

        # Ensure output directory exists or create it
        output_dir = '/opt/airflow/output'
        if not os.path.exists(output_dir):
            logging.info(f"Creating directory: {output_dir}")
            os.makedirs(output_dir)

        # Verify the directory was created
        if not os.path.exists(output_dir):
            raise FileNotFoundError(f"Directory {output_dir} does not exist after attempt to create it")

        # Save the transformed data to a CSV file
        output_path = os.path.join(output_dir, 'youtube_comments5.csv')
        logging.info(f"Attempting to save CSV file at: {output_path}")
        transformed_df.to_csv(output_path, index=False)
        logging.info(f"CSV file saved successfully at: {output_path}")

    except Exception as e:
        logging.error(f"Error in load_to_csv: {str(e)}")
        raise

# Define the DAG object
with DAG(
    'data_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    schedule_interval=None,  # Set the schedule interval to None for manual triggering
) as dag:
    # Define the extraction task
    extract_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
    )

    # Define the transformation task
    transform_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data,
    )

    # Define the loading task
    load_task = PythonOperator(
        task_id='load_to_csv_task',
        python_callable=load_to_csv,
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
