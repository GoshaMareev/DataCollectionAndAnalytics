"""
DAG для анализа зарплат в Data Science 2025
Вариант задания №13

Автор: Мареев Г.А.
Дата: 2025
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import shutil
import pandas as pd
import kagglehub

# Конфигурация по умолчанию для DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Создание DAG
dag = DAG(
    'ds_salaries_analysis_2025',
    default_args=default_args,
    description='ETL-пайплайн для анализа зарплат в Data Science 2025 года',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'data_science', 'salaries', 'kaggle', 'variant_13']
)


def extract_from_kaggle(**context):
    """
    Extract: Скачивание данных о зарплатах в Data Science с Kaggle
    """
    print("Начинаем извлечение данных о зарплатах в Data Science с Kaggle...")

    DATA_DIR = '/opt/airflow/dags/data'
    os.makedirs(DATA_DIR, exist_ok=True)

    kaggle_json_path = '/home/airflow/.kaggle/kaggle.json'
    if not os.path.exists(kaggle_json_path):
        raise FileNotFoundError(f"Файл kaggle.json не найден в {kaggle_json_path}.")
    print(f"kaggle.json найден в: {kaggle_json_path}")

    dataset_name = "arnabchaki/data-science-salaries-2025"
    print(f"Скачиваем датасет: {dataset_name}")

    try:
        path = kagglehub.dataset_download(dataset_name)
    except Exception as e:
        print(f"Ошибка при скачивании датасета: {e}")
        raise

    print(f"Данные скачаны в: {path}")

    # Найти CSV-файл (обычно это первый .csv в папке)
    csv_files = [f for f in os.listdir(path) if f.endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError("В датасете не найден CSV-файл.")
    
    source_file = os.path.join(path, csv_files[0])
    dest_file = os.path.join(DATA_DIR, "data_science_salaries_2025.csv")
    shutil.copy2(source_file, dest_file)

    print(f"Файл скопирован в: {dest_file}")
    context['task_instance'].xcom_push(key='data_file_path', value=dest_file)
    return dest_file


def load_raw_to_postgres(**context):
    """
    Load Raw: Загрузка сырых данных в PostgreSQL
    """
    print("Начинаем загрузку сырых данных в PostgreSQL...")
    data_file_path = context['task_instance'].xcom_pull(key='data_file_path', task_ids='extract_from_kaggle')

    df = pd.read_csv(data_file_path)
    print(f"Загружено {len(df)} записей из файла")

    # Приведение названий колонок к нижнему регистру
    df.columns = [col.lower() for col in df.columns]

    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')

    # Удаление старой таблицы
    postgres_hook.run("DROP TABLE IF EXISTS raw_ds_salaries;")

    create_table_sql = """
    CREATE TABLE raw_ds_salaries (
        id SERIAL PRIMARY KEY,
        work_year INTEGER,
        experience_level TEXT,
        employment_type TEXT,
        job_title TEXT,
        salary DOUBLE PRECISION,
        salary_currency TEXT,
        salary_in_usd DOUBLE PRECISION,
        employee_residence TEXT,
        remote_ratio INTEGER,
        company_location TEXT,
        company_size TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    postgres_hook.run(create_table_sql)

    # Вставка данных
    allowed_columns = [
        'work_year', 'experience_level', 'employment_type', 'job_title',
        'salary', 'salary_currency', 'salary_in_usd', 'employee_residence',
        'remote_ratio', 'company_location', 'company_size'
    ]
    df_filtered = df[allowed_columns]
    postgres_hook.insert_rows(
        table='raw_ds_salaries',
        rows=df_filtered.values.tolist(),
        target_fields=allowed_columns
    )
    print(f"Успешно загружено {len(df_filtered)} записей в raw_ds_salaries")


def transform_and_clean_data(**context):
    """
    Transform: Очистка и подготовка данных для витрины
    """
    print("Начинаем очистку и трансформацию данных...")
    postgres_hook = PostgresHook(postgres_conn_id='analytics_postgres')

    query = """
    SELECT 
        work_year, experience_level, employment_type, 
        job_title, company_size, salary_in_usd
    FROM raw_ds_salaries
    WHERE salary_in_usd IS NOT NULL 
      AND experience_level IN ('EN', 'MI', 'SE', 'EX')
      AND company_size IN ('S', 'M', 'L');
    """
    df = postgres_hook.get_pandas_df(query)

    print(f"Отфильтровано {len(df)} записей для витрины.")

    # Удаляем старую таблицу и зависимые объекты (VIEW)
    postgres_hook.run("DROP TABLE IF EXISTS stg_ds_salaries CASCADE;")

    create_staging_sql = """
    CREATE TABLE stg_ds_salaries (
        work_year INTEGER,
        experience_level TEXT,
        employment_type TEXT,
        job_title TEXT,
        company_size TEXT,
        salary_in_usd DOUBLE PRECISION
    );
    """
    postgres_hook.run(create_staging_sql)

    postgres_hook.insert_rows(
        table='stg_ds_salaries',
        rows=df.values.tolist(),
        target_fields=['work_year', 'experience_level', 'employment_type',
                       'job_title', 'company_size', 'salary_in_usd']
    )
    print(f"Трансформированные данные сохранены в stg_ds_salaries")


# --- Задачи DAG ---

extract_task = PythonOperator(
    task_id='extract_from_kaggle',
    python_callable=extract_from_kaggle,
    dag=dag
)

load_raw_task = PythonOperator(
    task_id='load_raw_to_postgres',
    python_callable=load_raw_to_postgres,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_and_clean_data',
    python_callable=transform_and_clean_data,
    dag=dag
)

create_datamart_task = PostgresOperator(
    task_id='create_datamart',
    postgres_conn_id='analytics_postgres',
    sql='datamart_variant_13.sql',
    dag=dag
)

# --- Зависимости ---
extract_task >> load_raw_task >> transform_task >> create_datamart_task