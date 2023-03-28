from airflow.decorators import dag, task
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import xmltodict
import pendulum
import requests
import os


# Initiate Pipeline schedule
@dag(
    dag_id='podcast_summary',
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 3, 24),
    catchup=False,
)
def podcast_summary():
    
    # Airflow operator which create table in SQLite Database (table_name = episodes, connection_id = podcast)
    create_table_sqlite_task = SqliteOperator(
        task_id="create_table_sqlite",
        sql=r"""
                CREATE TABLE if not exists episodes (
                    episode_link TEXT PRIMARY KEY,
                    title TEXT,
                    filename TEXT,
                    date_of_publish TEXT,
                    description TEXT,
                    transcript TEXT
                );
                """,
        sqlite_conn_id='podcast'
    )

    # Extract Marketplace podcast metadata in XML format
    @task()
    def get_episodes():
        data = requests.get('https://www.marketplace.org/feed/podcast/marketplace/')
        feed = xmltodict.parse(data.text)
        episodes = feed["rss"]["channel"]["item"]
        print(f"Found {len(episodes)} episodes.")
        return episodes

    # load metadata to SQLite Database
    @task()
    def load_episodes(episodes):
        sh = SqliteHook(sqlite_conn_id='podcast')
        stored = sh.get_pandas_df("SELECT * FROM episodes;")
        new_ep = []
        for ep in episodes:
            if ep['link'] not in stored['episode_link'].values:
                filename = f"{ep['link'].split('/')[-1]}.mp3"
                new_ep.append([ep['link'], ep['title'], filename, ep['pubDate'], ep['description']])
        sh.insert_rows(table='episodes', rows=new_ep,
                       target_fields=['episode_link', 'title', 'filename', 'date_of_publish', 'description'])

    # Download podcast mp3 file to Local in episodes folder
    @task()
    def download_episodes(episodes):
        for ep in episodes:
            filename = f"{ep['link'].split('/')[-1]}.mp3"
            url = ep['enclosure']['@url']
            audio_path = os.path.join('dags/episodes', filename)
            if not os.path.exists(audio_path):
                print(f"downloading {filename}")
                audio = requests.get(url)
                with open(audio_path, 'wb+') as f:
                    f.write(audio.content)

    # Make Pipeline DAG
    podcast_ep = get_episodes()
    create_table_sqlite_task.set_downstream(podcast_ep)
    load_episodes(podcast_ep)
    download_episodes(podcast_ep)


summary = podcast_summary()
