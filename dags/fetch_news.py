from datetime import datetime
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

my_dag = DAG("fetch_news",
             start_date=datetime(2022, 3, 10, 00, 00),
             schedule_interval="@daily",
             template_searchpath='/tmp_{{ ds }}_{{ next_ds }}',
             catchup=True,
             max_active_runs=1)


def fmt_ins(column_str: str) -> str:
    '''Format string to format accable by postgress insert'''
    if column_str is None:
        return 'null'
    column_str = column_str.replace("'", '"')
    column_str = "'" + column_str + "'"
    return column_str


def _prepare_insert(json_file, sql_insert_file, delete_date, **context):
    data = None
    sql_out = f"delete from SA.NEWS_COMPANY where cast('{delete_date}' as DATE) = cast(PUBLISH_DATE as DATE);\n"
    with open(json_file) as f:
        data = json.load(f)
    for art in data['articles']:
        COMPANY = fmt_ins('nvidia')
        SOURCE_NAME = fmt_ins(art['source']['name'])
        AUTHOR = fmt_ins(art['author'])
        TITLE = fmt_ins(art['title'])
        DESCRIPTION = fmt_ins(art['description'])
        CONTENT = fmt_ins(art['content'])
        URL = fmt_ins(art['url'])
        IMG_URL = fmt_ins(art['urlToImage'])
        PUBLISH_DATE = fmt_ins(art['publishedAt'])
        sql_out += f'''insert into SA.NEWS_COMPANY(COMPANY, SOURCE_NAME, AUTHOR, TITLE, DESCRIPTION, CONTENT, URL, IMG_URL, PUBLISH_DATE) 
                    values({COMPANY},{SOURCE_NAME},{AUTHOR},{TITLE},{DESCRIPTION},{CONTENT},{URL},{IMG_URL},cast({PUBLISH_DATE} as TIMESTAMP));\n'''
    with open(sql_insert_file, 'w') as f:
        f.write(sql_out)
    print(sql_insert_file)
    print(sql_out)


def _execute_query_with_hook(sql_file, **context):
    seq = ''
    with open(sql_file, 'r') as f:
        seq = '\n'.join(f.readlines())
    hook = PostgresHook(postgres_conn_id='news_in')
    hook.run(sql=seq)


start = DummyOperator(task_id="start", dag=my_dag)

dowload_desc = BashOperator(
    task_id="dowload_desc",
    bash_command="curl -o /tmp/news_nvidia_{{ ds }}_{{ next_ds }}.json "
                 "https://newsapi.org/v2/everything -G "
                 "-d from={{ ds }} "
                 "-d to={{ ds }} "
                 "-d language=en "
                 "-d q=nvidia "
                 "-d apiKey=1ddd1951b81c4e9eb662cc118c33ff6a",
    dag=my_dag
)

prepare_insert = PythonOperator(
    task_id="prepare_insert",
    python_callable=_prepare_insert,
    op_kwargs={
        'json_file': '/tmp/news_nvidia_{{ ds }}_{{ next_ds }}.json',
        'sql_insert_file': '/tmp/news_nvidia_{{ ds }}_{{ next_ds }}.sql',
        'delete_date': '{{ ds }}'
    }
)

write_to_postgres = PythonOperator(
    task_id="write_to_postgres",
    python_callable=_execute_query_with_hook,
    op_kwargs={
        'sql_file': '/tmp/news_nvidia_{{ ds }}_{{ next_ds }}.sql'
    }
)

is_last = LatestOnlyOperator(task_id='last_only', dag=my_dag)

start_reload_model = DummyOperator(task_id='start_reload_model')

load_model_links = DummyOperator(task_id='load_model_links')

load_news = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_news",
    sql='call DWH.load_news();',
)

load_url = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_url",
    sql='call DWH.load_url();',
)

load_company = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_company",
    sql='call DWH.LOAD_COMPANY();',
)

link_news_url = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="link_news_url",
    sql='call DWH.LINK_NEWS_URL();',
)

link_news_company = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="link_news_company",
    sql='call DWH.LINK_NEWS_COMPANY();',
)

clear_metadata = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="clear_metadata",
    sql='call DWH.CLEAR_METADATA();',
)

start >> dowload_desc >> prepare_insert >> write_to_postgres
[write_to_postgres, is_last] >> start_reload_model
start_reload_model >> [load_news, load_url, load_company] >> load_model_links
load_model_links >> [link_news_company, link_news_url] >> clear_metadata
