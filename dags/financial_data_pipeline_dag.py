from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Extract function to fetch stock prices and financial news
def extract():
    print("Starting the extract function")
    alpha_vantage_api_key = 'YOUR_ALPHA_VANTAGE_API_KEY'
    news_api_key = 'YOUR_NEWS_API_KEY'
    
    # Fetch stock prices for Reliance Industries Limited (RELIANCE.BSE) from Alpha Vantage
    stock_symbol = 'RELIANCE.BSE'
    stock_url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={alpha_vantage_api_key}'
    stock_response = requests.get(stock_url)
    
    if stock_response.status_code != 200:
        raise Exception(f"Error fetching stock data: {stock_response.status_code}")
    
    stock_data = stock_response.json()
    
    if 'Time Series (Daily)' not in stock_data:
        raise Exception("Stock data format error")
    
    # Convert stock data to DataFrame
    stock_df = pd.DataFrame.from_dict(stock_data['Time Series (Daily)'], orient='index')
    stock_df = stock_df.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    })
    stock_df.index = pd.to_datetime(stock_df.index)
    stock_df = stock_df.sort_index()

    # Add date column from the index
    stock_df['date'] = stock_df.index

    # Fetch financial news from NewsAPI
    news_url = f'https://newsapi.org/v2/everything?q=india+finance&apiKey={news_api_key}'
    news_response = requests.get(news_url)
    
    if news_response.status_code != 200:
        raise Exception(f"Error fetching news data: {news_response.status_code}")
    
    news_data = news_response.json()
    
    if 'articles' not in news_data:
        raise Exception("News data format error")
    
    # Convert news data to DataFrame
    articles = news_data['articles']
    news_df = pd.DataFrame(articles)

    # Add date column from the 'publishedAt' field
    news_df['date'] = pd.to_datetime(news_df['publishedAt']).dt.date
    
    # Ensure the directory exists
    output_dir = '/opt/airflow/files'
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory exists: {os.path.exists(output_dir)}")
    print(f"Output directory is writable: {os.access(output_dir, os.W_OK)}")

    print("Saving stock data to CSV")
    stock_csv_path = os.path.join(output_dir, 'stocks.csv')
    news_csv_path = os.path.join(output_dir, 'news.csv')
    print(f"Stock CSV Path: {stock_csv_path}")
    print(f"News CSV Path: {news_csv_path}")

    stock_df.to_csv(stock_csv_path, index=False)
    news_df.to_csv(news_csv_path, index=False)
    print(f"Stock CSV exists: {os.path.exists(stock_csv_path)}")
    print(f"News CSV exists: {os.path.exists(news_csv_path)}")
    print("Extract function completed successfully")

# Transform function to calculate financial metrics and perform basic text analysis
def transform():
    print("Starting the transform function")
    input_dir = '/opt/airflow/files'
    
    stock_csv_path = os.path.join(input_dir, 'stocks.csv')
    news_csv_path = os.path.join(input_dir, 'news.csv')
    
    print(f"Checking existence of stock CSV at: {stock_csv_path}")
    print(f"Stock CSV exists: {os.path.exists(stock_csv_path)}")
    
    print(f"Checking existence of news CSV at: {news_csv_path}")
    print(f"News CSV exists: {os.path.exists(news_csv_path)}")

    if not os.path.exists(stock_csv_path):
        raise Exception(f"Stock CSV not found at {stock_csv_path}")
    
    if not os.path.exists(news_csv_path):
        raise Exception(f"News CSV not found at {news_csv_path}")
    
    # Load CSV files
    stock_df = pd.read_csv(stock_csv_path)
    news_df = pd.read_csv(news_csv_path)
    
    # Perform some transformation - Example: Adding a new column
    stock_df['average'] = (stock_df['open'] + stock_df['close']) / 2
    
    # Save the transformed data back to CSV
    transformed_stock_csv_path = os.path.join(input_dir, 'transformed_stocks.csv')
    stock_df.to_csv(transformed_stock_csv_path, index=False)
    print(f"Transformed stock CSV saved at: {transformed_stock_csv_path}")
    print(f"Transform function completed successfully")

# Load function to save the transformed data
def load():
    # Define paths for the transformed data
    transformed_stocks_path = '/opt/airflow/files/transformed_stocks.csv'
    transformed_news_path = '/opt/airflow/files/transformed_news.csv'

    # Read the transformed data from the CSV files
    if os.path.exists(transformed_stocks_path):
        transformed_stocks_df = pd.read_csv(transformed_stocks_path)
        print("Transformed Stock Data:")
        print(transformed_stocks_df.head())
    else:
        print(f"File {transformed_stocks_path} does not exist.")

    if os.path.exists(transformed_news_path):
        transformed_news_df = pd.read_csv(transformed_news_path)
        print("Transformed News Data:")
        print(transformed_news_df.head())
    else:
        print(f"File {transformed_news_path} does not exist.")


# Define the DAG
dag = DAG(
    'financial_data_pipeline',
    default_args=default_args,
    description='A simple financial data pipeline DAG',
    schedule_interval=None,  # Set to None to disable automatic scheduling
)

# Define the tasks
t1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Set the task dependencies
t1 >> t2 >> t3
