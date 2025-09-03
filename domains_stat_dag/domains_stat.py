import requests
import pandas as pd
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'https://storage.yandexcloud.net/kc-startda/top-1m.csv'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

def get_data():
    top_doms = pd.read_csv(TOP_1M_DOMAINS)
    top_data = top_doms.to_csv(index=False)
    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def top_10_zones():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    df['zone'] = df['domain'].apply(lambda x: x.split('.')[-1])
    top_zones = df['zone'].value_counts().head(10)
    with open('top_10_zones.csv', 'w') as f:
        f.write(top_zones.to_csv(header=True))


def longest_domain():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    max_len = df['domain'].str.len().max()
    longest = df[df['domain'].str.len() == max_len].sort_values('domain').iloc[0]
    with open('longest_domain.csv', 'w') as f:
        f.write(f"{longest['domain']}, length: {max_len}\n")


def airflow_rank():
    df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    row = df[df['domain'] == 'airflow.com']
    rank = row['rank'].iloc[0] if not row.empty else 'Not found'
    with open('airflow_rank.csv', 'w') as f:
        f.write(f"airflow rank: {rank}\n")


def print_data(ds):
    date = ds

    for file in ['top_10_zones.csv', 'longest_domain.csv', 'airflow_rank.csv']:
        with open(file, 'r') as f:
            data = f.read()
        print(f"Data from {file} for {date}:")
        print(data)

default_args = {
    'owner': 'denis-golovanov-bbe3569',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 8, 30),
}

schedule_interval = '@daily'

dag = DAG(
    'denis_golovanov_bbe3569_top_1m',
    default_args=default_args,
    schedule_interval=schedule_interval
)

t1 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)

t2 = PythonOperator(
    task_id='top_10_zones',
    python_callable=top_10_zones,
    dag=dag
)

t3 = PythonOperator(
    task_id='longest_domain',
    python_callable=longest_domain,
    dag=dag
)

t4 = PythonOperator(
    task_id='airflow_com_rank',
    python_callable=airflow_rank,
    dag=dag
)

t5 = PythonOperator(
    task_id='print_data',
    python_callable=print_data,
    dag=dag
)

t1 >> [t2, t3, t4] >> t5
