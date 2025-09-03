import pandas as pd
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

LINK = "https://drive.google.com/uc?export=download&id=1_krkAf04wFo3sLbXQKT-LRKFKbJzqEAi"
FILE = "vgsales.csv"

def get_data():
    df = pd.read_csv(LINK)
    df.to_csv(FILE, index=False)


def best_selling_game_world():
    df = pd.read_csv(FILE)
    df = df[df['Year'] == 1996]
    best_game = df.loc[df['Global_Sales'].idxmax(), ['Name', 'Global_Sales']]
    with open("best_game_world.csv", "w") as f:
        f.write(f"{best_game['Name']} ({best_game['Global_Sales']}M)\n")


def best_selling_genres_europe():
    df = pd.read_csv(FILE)
    df = df[df['Year'] == 1996]
    genre_sales = df.groupby('Genre', as_index=False)['EU_Sales'].sum()
    max_sales = genre_sales['EU_Sales'].max()
    top_genres = genre_sales[genre_sales['EU_Sales'] == max_sales]
    with open("best_genres_europe.csv", "w") as f:
        f.write(top_genres.to_csv(index=False))


def best_platform_na():
    df = pd.read_csv(FILE)
    df = df[df['Year'] == 1996]
    platform_counts = df[df['NA_Sales'] > 1].groupby('Platform').size().reset_index(name='count')
    max_count = platform_counts['count'].max()
    top_platforms = platform_counts[platform_counts['count'] == max_count]
    with open("best_platform_na.csv", "w") as f:
        f.write(top_platforms.to_csv(index=False))


def best_publisher_jp():
    df = pd.read_csv(FILE)
    df = df[df['Year'] == 1996]
    publisher_sales = df.groupby('Publisher', as_index=False)['JP_Sales'].mean()
    max_avg = publisher_sales['JP_Sales'].max()
    top_publishers = publisher_sales[publisher_sales['JP_Sales'] == max_avg]
    with open("best_publisher_jp.csv", "w") as f:
        f.write(top_publishers.to_csv(index=False))


def eu_vs_jp_count():
    df = pd.read_csv(FILE)
    df = df[df['Year'] == 1996]
    count = (df['EU_Sales'] > df['JP_Sales']).sum()
    with open("eu_vs_jp.csv", "w") as f:
        f.write(f"{count}\n")


def print_data(ds):
    date = ds
    files = [
        "best_game_world.csv",
        "best_genres_europe.csv",
        "best_platform_na.csv",
        "best_publisher_jp.csv",
        "eu_vs_jp.csv",
    ]
    for file in files:
        with open(file, "r") as f:
            data = f.read()
        print(f"Data from {file} for {date}:")
        print(data)



default_args = {
    "owner": "denis-golovanov-bbe3569",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 8, 30),
}

schedule_interval = "@daily"

dag = DAG(
    "denis_golovanov_bbe3569_vgsales_1996",
    default_args=default_args,
    schedule_interval=schedule_interval,
)

t1 = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id="best_selling_game_world",
    python_callable=best_selling_game_world,
    dag=dag,
)

t3 = PythonOperator(
    task_id="best_selling_genres_europe",
    python_callable=best_selling_genres_europe,
    dag=dag,
)

t4 = PythonOperator(
    task_id="best_platform_na",
    python_callable=best_platform_na,
    dag=dag,
)

t5 = PythonOperator(
    task_id="best_publisher_jp",
    python_callable=best_publisher_jp,
    dag=dag,
)

t6 = PythonOperator(
    task_id="eu_vs_jp_count",
    python_callable=eu_vs_jp_count,
    dag=dag,
)

t7 = PythonOperator(
    task_id="print_data",
    python_callable=print_data,
    dag=dag,
)

t1 >> [t2, t3, t4, t5, t6] >> t7
