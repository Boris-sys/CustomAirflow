import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

globalIris = pd.DataFrame

def get_iris_data():
    sql_stmt = "SELECT * FROM iris"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_localhost',
        schema='test'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def process_iris_data(ti):
    iris = ti.xcom_pull(task_ids=['get_iris_data'])
    if not iris:
        raise Exception('No data.')

    iris = pd.DataFrame(
        data=iris[0],
        columns=['iris_id', 'iris_sepal_length', 'iris_sepal_width',
                 'iris_petal_length', 'iris_petal_width', 'iris_variety']
    )
    iris = iris[
        (iris['iris_sepal_length'] > 5) &
        (iris['iris_sepal_width'] == 3) &
        (iris['iris_petal_length'] > 3) &
        (iris['iris_petal_width'] == 1.5)
    ]
    iris = iris.drop('iris_id', axis=1)
    globalIris = iris
    iris.to_csv(Variable.get('tmp_iris_csv_location'), index=False)

def csv_to_sql():
    globalIris.to_sql(name='to_sql_test', con='postgres://airflow:airflow@localhost/test', if_exists='replace', index=False)
    print("csv to sql")


with DAG(
    dag_id='postgres_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=5, day=13),
    catchup=False
) as dag:
    # 1. Get the Iris data from a table in Postgres
    task_get_iris_data = PythonOperator(
        task_id='get_iris_data',
        python_callable=get_iris_data,
        do_xcom_push=True
    )

    # 2. Process the Iris data
    task_process_iris_data = PythonOperator(
        task_id='process_iris_data',
        python_callable=process_iris_data
    )

    # 3. Truncate table in Postgres
    task_truncate_table = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres_localhost',
        sql="TRUNCATE TABLE iris_tgt"
    )

    # 4. Save to Postgres
    task_load_iris_data = PythonOperator(
        task_id='load_iris_data',
        python_callable=csv_to_sql
    )
    task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_load_iris_data

# import pandas as pd
# from datetime import datetime
# from airflow.models import DAG
# from airflow.operators.python import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.models import Variable
# from airflow.operators.bash import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator


# def get_iris_data():
#     sql_stmt = "SELECT * FROM iris"
#     pg_hook = PostgresHook(
#         postgres_conn_id='postgres_db',
#         schema='db_test'
#     )
#     pg_conn = pg_hook.get_conn()
#     cursor = pg_conn.cursor()
#     cursor.execute(sql_stmt)
#     return cursor.fetchall()


# def process_iris_data(ti):
#     iris = ti.xcom_pull(task_ids=['get_iris_data'])
#     if not iris:
#         raise Exception('No data.')

#     iris = pd.DataFrame(
#         data=iris[0],
#         columns=['iris_id', 'iris_sepal_length', 'iris_sepal_width',
#                  'iris_petal_length', 'iris_petal_width', 'iris_variety']
#     )
#     iris = iris[
#         (iris['iris_sepal_length'] > 5) &
#         (iris['iris_sepal_width'] == 3) &
#         (iris['iris_petal_length'] > 3) &
#         (iris['iris_petal_width'] == 1.5)
#     ]
#     iris = iris.drop('iris_id', axis=1)
#     iris.to_csv(Variable.get('tmp_iris_csv_location'), index=False)


# with DAG(
#     dag_id='postgres_db_dag',
#     schedule_interval='@daily',
#     start_date=datetime(year=2022, month=2, day=1),
#     catchup=False
# ) as dag:

#     # 1. Get the Iris data from a table in Postgres
#     task_get_iris_data = PythonOperator(
#         task_id='get_iris_data',
#         python_callable=get_iris_data,
#         do_xcom_push=True
#     )

#     # 2. Process the Iris data
#     task_process_iris_data = PythonOperator(
#         task_id='process_iris_data',
#         python_callable=process_iris_data
#     )

#     # 3. Truncate table in Postgres
#     task_truncate_table = PostgresOperator(
#         task_id='truncate_tgt_table',
#         postgres_conn_id='postgres_db',
#         sql="TRUNCATE TABLE iris_tgt"
#     )

#     # 4. Save to Postgres
#     task_load_iris_data = BashOperator(
#         task_id='load_iris_data',
#         bash_command=(
#             'psql -d db_test -U dradecic -c "'
#             'COPY iris_tgt(iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety) '
#             "FROM '/tmp/iris_processed.csv' "
#             "DELIMITER ',' "
#             'CSV HEADER"'
#         )
#     )
    
#     task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_load_iris_data

#rebuild after adding library to requirements.txt
#docker build . --tag extending_airflow:latest
#docker-compose up -d --no-deps --build airflow-webserver airflow-scheduler