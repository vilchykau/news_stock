from datetime import datetime
import pandas as pd
import requests
import csv
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


#API key:  OI736XXBB9GJWUC3


def _download_csv(csv_file, **context) -> None:
    url_nvidia = "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=NVDA&apikey=OI736XXBB9GJWUC3&datatype=csv"
    df_nvidia = pd.read_csv(url_nvidia)
    df_nvidia['company'] = 'nvidia'
    df_nvidia.to_csv(csv_file)


def fmt_ins(column_str: str) -> str:
    '''Format string to format accable by postgress insert'''
    if column_str is None:
        return 'null'
    column_str = column_str.replace("'", '"')
    column_str = "'" + column_str + "'"
    return column_str


def _prepare_insert(csv_file, sql_insert_file, **context):
    # sql_out = f"delete from SA.NEWS_COMPANY where cast('{delete_date}' as DATE) = cast(PUBLISH_DATE as DATE);\n"
    sql_out = ""
    df = pd.read_csv(csv_file)
    for index, row in df.iterrows():
        company = fmt_ins(row['company'])
        trade_date = f"date '{row['timestamp']}'"
        opens = row['open']
        high = row['high']
        low = row['low']
        close = row['close']
        volume = row['volume']
        sql_out += f'insert into SA.stocks_company(company, trade_date, open, high, low, close, volume) values ({company},{trade_date},{opens},{high},{low},{close},{volume});\n'
    sql_out += '''delete from SA.stocks_company A
                 where exists(select 1
                 from SA.stocks_company B
                 where A.company = B.company
                 and A.trade_date = B.trade_date
                 and A.insert_dt < B.insert_dt);\n'''
    print(sql_out)
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


my_dag = DAG("fetch_stocks",
             start_date=datetime(2022, 5, 10, 00, 00),
             schedule_interval="@daily",
             template_searchpath='/tmp_{{ ds }}_{{ next_ds }}',
             catchup=True,
             max_active_runs=1)

start_operator = DummyOperator(
    task_id='start',
    dag=my_dag
)

dowload_desc = PythonOperator(
    task_id="dowload_stock",
    python_callable=_download_csv,
    op_kwargs={
        'csv_file': '/tmp/stocks_nvidia_{{ ds }}_{{ next_ds }}.csv'
    }
)

prepare_insert = PythonOperator(
    task_id="prepare_insert",
    python_callable=_prepare_insert,
    op_kwargs={
        'csv_file': '/tmp/stocks_nvidia_{{ ds }}_{{ next_ds }}.csv',
        'sql_insert_file': '/tmp/stocks_nvidia_{{ ds }}_{{ next_ds }}.sql',
    }
)

write_to_postgres = PythonOperator(
    task_id="write_to_postgres",
    python_callable=_execute_query_with_hook,
    op_kwargs={
        'sql_file': '/tmp/stocks_nvidia_{{ ds }}_{{ next_ds }}.sql'
    }
)

load_stocks = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_stocks",
    sql='call DWH.load_stocks();',
)

load_date = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_date",
    sql="call DWH.load_date('STOCKS');",
)

load_company = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="load_company",
    sql='call DWH.LOAD_COMPANY();',
)

link_anchors = DummyOperator(task_id='link_anchors')

link_stocks_date = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="link_stocks_date",
    sql="call DWH.link_stocks_date();",
)

link_stocks_company = PostgresOperator(
    postgres_conn_id='news_in',
    task_id="link_stocks_company",
    sql="call DWH.link_stocks_company();",
)

end = DummyOperator(task_id='end')


start_operator >> dowload_desc >> prepare_insert >> write_to_postgres
write_to_postgres >> [load_stocks, load_date, load_company] >> link_anchors
link_anchors >> [link_stocks_date, link_stocks_company] >> end
