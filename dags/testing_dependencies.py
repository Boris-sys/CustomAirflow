from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Boris',
    'retry': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"sklearn with version: {sklearn.__version__} ")


def get_matplotlib():
    import matplotlib
    print(f"matplotlib with version: {matplotlib.__version__}")

def get_pandas():
    import pandas
    print(f"pandas with version: {pandas.__version__}")

with DAG(
    default_args=default_args,
    dag_id="test_dependecies",
    start_date=datetime(2023, 5, 13),
    schedule_interval='@daily'
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='get_sklearn',
        python_callable=get_sklearn
    )
    
    task2 = PythonOperator(
        task_id='get_matplotlib',
        python_callable=get_matplotlib
    )

    task3 = PythonOperator(
        task_id='get_pandas',
        python_callable=get_pandas
    )

    task1 >> task2 >> task3




# from datetime import datetime, timedelta

# from airflow import DAG
# from airflow.operators.python import PythonOperator


# default_args = {
#     'owner': 'boris',
#     'retry': 5,
#     'retry_delay': timedelta(minutes=5)
# }

# def get_sklearn():
#     import sklearn
#     print(f"sklearn with version: {sklearn.__version__} ")


# def get_matplotlib():
#     import matplotlib
#     print(f"matplotlib with version: {matplotlib.__version__}")

# def get_pandas():
#     import pandas
#     print(f"pandas with version: {pandas.__version__}")

# with DAG(
#     default_args=default_args,
#     dag_id="test_dependecies",
#     start_date=datetime(2023, 5, 13),
#     schedule_interval='@daily'
# ) as dag:
#     task1 = PythonOperator(
#         task_id='get_sklearn',
#         python_callable=get_sklearn
#     )
    
#     task2 = PythonOperator(
#         task_id='get_matplotlib',
#         python_callable=get_matplotlib
#     )

#     task3 = PythonOperator(
#         task_id='get_pandas',
#         python_callable=get_pandas
#     )

#     task1 >> task2 >> task3