import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

dag = DAG("test_postgress", start_date=datetime.datetime(2022, 3, 10, 00, 00),
             schedule_interval="@once", catchup=False
             ,max_active_runs = 1)

def _select_all_platforms(**context):
    hook = PostgresHook(postgres_conn_id='news_in')
    res = hook.get_pandas_df('select * from NEWS.TARGET_SEARCH;')
    # conn = hook.get_conn()
    # cursor = conn.cursor()    
    # res = cursor.execute("insert into NEWS.TARGET_SEARCH (QUERY) values ('apple');")
    # res = cursor.execute('select * from NEWS.TARGET_SEARCH;')
    print(res)

select_plat = python_task = PythonOperator(
    task_id="python_task",
    python_callable=_select_all_platforms,
    dag=dag
)
