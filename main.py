from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd

def extract_data(urls, file_name):
    for url_details in urls:
        url = url_details['url']
        source = url_details['source']
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # get links (example, not used in file saving)
        links = [link.get('href') for link in soup.find_all('a', href=True)]

        # get titles and descriptions
        articles = soup.find_all('article')
        article_data = []
        for idx, article in enumerate(articles):
            title = article.find('h2').text.strip() if article.find('h2') else None
            description = article.find('p').text.strip() if article.find('p') else None
            article_data.append({'id': idx + 1, 'title': title, 'description': description, 'source': source})

        # Append data to CSV
        mode = 'a' if os.path.exists(file_name) else 'w'
        with open(file_name, mode, newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'title', 'description', 'source']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if mode == 'w':
                writer.writeheader()
            for article in article_data:
                writer.writerow(article)

        print(f"Data appended from {source} to {file_name}")

def transform_data(input_file, output_file):
    # Perform data cleaning and transformation
    data = pd.read_csv(input_file)
    data = data.drop_duplicates()
    data.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

def load():
    os.system('cd /home/kali/airflow/dags/MLOPsAssignment && dvc add cleaned_dataset.csv')
    os.system('cd /home/kali/airflow/dags/MLOPsAssignment && git add /home/kali/airflow/dags/Assignment02_MLOPS/cleaned_dataset.csv.dvc')
    os.system('cd /home/kali/airflow/dags/MLOPsAssignment && git commit -m "Updated data through airflow"')
    os.system('cd /home/kali/airflow/dags/MLOPsAssignment && dvc push')

# Define URLs
urls = [
    {'url': 'https://www.dawn.com/', 'source': 'dawn.com'},
    {'url': 'https://www.bbc.com/', 'source': 'bbc.com'}
]

file_name = "/home/kali/airflow/dags/MLOPsAssignment/dataset.csv"
input_file = file_name
output_file = "/home/kali/airflow/dags/MLOPsAssignment/cleaned_dataset.csv"

with DAG("my-dag", start_date=datetime.now(), schedule="@daily", catchup=False) as dag:

    extractingDataTask = PythonOperator(
        task_id="Extracting_Data",
        python_callable=extract_data,
        op_kwargs={'urls': urls, 'file_name': file_name},
    )

    transformingDataTask = PythonOperator(
        task_id="Transforming_Data",
        python_callable=transform_data,
        op_kwargs={'input_file': input_file, 'output_file': output_file},
    )

    loadingDataTask = PythonOperator(
        task_id="Loading_Data",
        python_callable=load,
    )

    gitPushTask = BashOperator(
        task_id="Git_Push",
        bash_command="git -C /home/kali/airflow/dags/MLOPsAssignment push origin master",
    )

    extractingDataTask >> transformingDataTask >> loadingDataTask >> gitPushTask
