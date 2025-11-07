"""
DAG для анализа посещаемости врачей пациентами разных возрастных групп
Вариант задания №13

Автор: Мареев Г. А.
Дата: 2025
"""

from datetime import datetime, timedelta
import pandas as pd
import json
import sqlite3
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago

# Конфигурация по умолчанию для DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['test@example.com']
}

# Создание DAG
dag = DAG(
    'patient_doctor_visits_analysis_v13',
    default_args=default_args,
    description='Анализ посещаемости врачей пациентами разных возрастных групп',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['etl', 'healthcare', 'patients', 'doctors', 'variant_13']
)

# Пути к файлам данных
DATA_DIR = '/opt/airflow/dags/data'
DB_PATH = '/opt/airflow/patient_analysis.db'

def extract_patients_data(**context):
    """
    Extract: Чтение данных о пациентах из CSV файла
    """
    print("Начинаем извлечение данных о пациентах из CSV...")
    
    csv_path = os.path.join(DATA_DIR, 'patients.csv')
    
    try:
        # Чтение CSV файла
        patients_df = pd.read_csv(csv_path)
        print(f"Загружено {len(patients_df)} записей о пациентах")
        print("Первые 5 записей:")
        print(patients_df.head())
        
        # Сохранение данных для следующих задач
        patients_data = patients_df.to_dict('records')
        context['task_instance'].xcom_push(key='patients_data', value=patients_data)
        
        print("Данные о пациентах успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(patients_df)} записей о пациентах"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о пациентах: {str(e)}")
        raise

def extract_visits_data(**context):
    """
    Extract: Чтение данных о визитах из Excel файла
    """
    print("Начинаем извлечение данных о визитах из Excel...")
    
    excel_path = os.path.join(DATA_DIR, 'visits.xlsx')
    
    try:
        # Чтение Excel файла
        visits_df = pd.read_excel(excel_path)
        print(f"Загружено {len(visits_df)} записей о визитах")
        print("Первые 5 записей:")
        print(visits_df.head())
        
        # Сохранение данных для следующих задач
        visits_data = visits_df.to_dict('records')
        context['task_instance'].xcom_push(key='visits_data', value=visits_data)
        
        print("Данные о визитах успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(visits_df)} записей о визитах"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о визитах: {str(e)}")
        raise

def extract_diagnoses_data(**context):
    """
    Extract: Чтение данных о диагнозах из JSON файла
    """
    print("Начинаем извлечение данных о диагнозах из JSON...")
    
    json_path = os.path.join(DATA_DIR, 'diagnoses.json')
    
    try:
        # Чтение JSON файла
        with open(json_path, 'r', encoding='utf-8') as f:
            diagnoses_data = json.load(f)
        
        diagnoses_df = pd.DataFrame(diagnoses_data)
        print(f"Загружено {len(diagnoses_df)} записей о диагнозах")
        print("Первые 5 записей:")
        print(diagnoses_df.head())
        
        # Сохранение данных для следующих задач
        context['task_instance'].xcom_push(key='diagnoses_data', value=diagnoses_data)
        
        print("Данные о диагнозах успешно извлечены и сохранены в XCom")
        return f"Извлечено {len(diagnoses_df)} записей о диагнозах"
        
    except Exception as e:
        print(f"Ошибка при извлечении данных о диагнозах: {str(e)}")
        raise

def transform_data(**context):
    """
    Transform: Объединение данных и определение самого посещаемого специалиста по возрастным группам
    """
    print("Начинаем трансформацию данных...")
    
    try:
        # Получение данных из предыдущих задач
        patients_data = context['task_instance'].xcom_pull(key='patients_data', task_ids='extract_patients')
        visits_data = context['task_instance'].xcom_pull(key='visits_data', task_ids='extract_visits')
        diagnoses_data = context['task_instance'].xcom_pull(key='diagnoses_data', task_ids='extract_diagnoses')
        
        # Преобразование в DataFrame
        patients_df = pd.DataFrame(patients_data)
        visits_df = pd.DataFrame(visits_data)
        # diagnoses_df не используется напрямую, но можно добавить контекст при расширении
        
        print("Данные успешно получены из XCom")
        print(f"Пациенты: {len(patients_df)} записей")
        print(f"Визиты: {len(visits_df)} записей")
        
        # Объединение данных по patient_id
        merged_df = pd.merge(patients_df, visits_df, on='patient_id', how='inner')
        print(f"После объединения пациентов и визитов: {len(merged_df)} записей")
        
        # Группировка по возрастной группе и специальности врача
        grouped = merged_df.groupby(['age_group', 'doctor_specialty']).size().reset_index(name='visit_count')
        
        # Находим наиболее посещаемого врача в каждой возрастной группе
        idx = grouped.groupby('age_group')['visit_count'].idxmax()
        top_specialists = grouped.loc[idx].reset_index(drop=True)
        
        # Добавление дополнительных метрик
        total_per_group = merged_df.groupby('age_group').size().reset_index(name='total_visits')
        analysis_result = top_specialists.merge(total_per_group, on='age_group')
        analysis_result['percentage'] = (
            (analysis_result['visit_count'] / analysis_result['total_visits']) * 100
        ).round(2)
        
        # Переименование для ясности
        analysis_result.rename(columns={'doctor_specialty': 'top_doctor_specialty'}, inplace=True)
        
        print("Результаты анализа самого посещаемого специалиста по возрастным группам:")
        print(analysis_result)
        
        # Сохранение результатов для загрузки в БД
        result_data = analysis_result.to_dict('records')
        context['task_instance'].xcom_push(key='analysis_result', value=result_data)
        
        print("Трансформация данных завершена успешно")
        return f"Проанализировано {len(result_data)} возрастных групп"
        
    except Exception as e:
        print(f"Ошибка при трансформации данных: {str(e)}")
        raise
def load_to_database(**context):
    """
    Load: Загрузка результатов анализа в SQLite базу данных
    """
    print("Начинаем загрузку данных в базу данных...")
    
    try:
        # Получение результатов анализа
        analysis_data = context['task_instance'].xcom_pull(
            key='analysis_result', 
            task_ids='transform_data'
        )
        
        if not analysis_data:
            raise ValueError("Нет данных для загрузки в базу данных")
        
        # Создание DataFrame из результатов
        analysis_df = pd.DataFrame(analysis_data)
        
        # Подключение к SQLite базе данных
        conn = sqlite3.connect(DB_PATH)
        
        try:
            # Создание таблицы если она не существует
            create_table_query = """
            CREATE TABLE IF NOT EXISTS age_group_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                age_group TEXT NOT NULL,
                top_doctor_specialty TEXT NOT NULL,
                visit_count INTEGER NOT NULL,
                total_visits INTEGER NOT NULL,
                percentage REAL NOT NULL,
                analysis_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(create_table_query)
            
            # Очистка таблицы перед загрузкой новых данных
            conn.execute("DELETE FROM age_group_analysis")
            
            # Загрузка данных в таблицу
            analysis_df.to_sql('age_group_analysis', conn, if_exists='append', index=False)
            
            # Подтверждение транзакции
            conn.commit()
            
            print(f"Успешно загружено {len(analysis_df)} записей в базу данных")
            
            # Проверка загруженных данных
            verification_query = "SELECT * FROM age_group_analysis ORDER BY percentage DESC"
            result = pd.read_sql_query(verification_query, conn)
            print("Проверка загруженных данных:")
            print(result)
            
        finally:
            conn.close()
        
        print("Загрузка в базу данных завершена успешно")
        return f"Загружено {len(analysis_df)} записей в SQLite базу данных"
        
    except Exception as e:
        print(f"Ошибка при загрузке в базу данных: {str(e)}")
        raise

def generate_report(**context):
    """
    Генерация отчета с результатами анализа и сохранение в файл
    """
    print("Генерируем отчет с результатами анализа...")
    
    try:
        # Получение данных из базы данных
        conn = sqlite3.connect(DB_PATH)
        
        try:
            query = """
            SELECT 
                age_group,
                top_doctor_specialty,
                visit_count,
                total_visits,
                percentage
            FROM age_group_analysis 
            ORDER BY percentage DESC
            """
            
            result_df = pd.read_sql_query(query, conn)
            
            # Формирование отчета
            report = f"""ОТЧЕТ ПО АНАЛИЗУ ПОСЕЩАЕМОСТИ ВРАЧЕЙ ПО ВОЗРАСТНЫМ ГРУППАМ
================================================================

Дата анализа: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Общее количество возрастных групп: {len(result_df)}

РЕЗУЛЬТАТЫ ПО ВОЗРАСТНЫМ ГРУППАМ:
"""
            
            for _, row in result_df.iterrows():
                report += f"""
Возрастная группа: {row['age_group']}
- Самый посещаемый специалист: {row['top_doctor_specialty']}
- Количество визитов: {row['visit_count']}
- Всего визитов в группе: {row['total_visits']}
- Доля от общего числа: {row['percentage']:.2f}%
"""
            
            # Добавление общей статистики
            total_visits = result_df['total_visits'].sum()
            avg_percentage = result_df['percentage'].mean()
            
            report += f"""
ОБЩАЯ СТАТИСТИКА:
- Общее количество визитов: {total_visits:,}
- Средняя доля лидирующего специалиста: {avg_percentage:.2f}%

РЕКОМЕНДАЦИИ:
"""
            
            best_row = result_df.iloc[0]
            worst_row = result_df.iloc[-1]
            
            report += f"""- Наиболее востребованный специалист: "{best_row['top_doctor_specialty']}" 
  в группе '{best_row['age_group']}' ({best_row['percentage']:.2f}%)
- Менее однородное поведение в группе '{worst_row['age_group']}', где лидер имеет лишь {worst_row['percentage']:.2f}%
- Рекомендуется проанализировать причины выбора врачей в разных группах.
"""
            
            print("Отчет сгенерирован:")
            print(report)
            
            # Сохранение отчета в файл
            report_file_path = '/opt/airflow/patient_visit_analysis_report.txt'
            with open(report_file_path, 'w', encoding='utf-8') as f:
                f.write(report)
            print(f"Отчет сохранен в файл: {report_file_path}")
            
            # Сохранение CSV файла с данными
            csv_file_path = '/opt/airflow/patient_visit_analysis_data.csv'
            result_df.to_csv(csv_file_path, index=False, encoding='utf-8')
            print(f"Данные сохранены в CSV: {csv_file_path}")
            
            # Сохранение данных для email
            context['task_instance'].xcom_push(key='report', value=report)
            context['task_instance'].xcom_push(key='report_file_path', value=report_file_path)
            context['task_instance'].xcom_push(key='csv_file_path', value=csv_file_path)
            context['task_instance'].xcom_push(key='result_data', value=result_df.to_dict('records'))
            
        finally:
            conn.close()
            
        return "Отчет успешно сгенерирован и сохранен в файлы"
        
    except Exception as e:
        print(f"Ошибка при генерации отчета: {str(e)}")
        raise
# Определение задач DAG

# Extract задачи
extract_patients_task = PythonOperator(
    task_id='extract_patients',
    python_callable=extract_patients_data,
    dag=dag,
    doc_md="""
    ### Извлечение данных о пациентах
    Читает CSV файл с информацией о пациентах и их возрастных группах.
    """
)

extract_visits_task = PythonOperator(
    task_id='extract_visits',
    python_callable=extract_visits_data,
    dag=dag,
    doc_md="""
    ### Извлечение данных о визитах
    Читает Excel файл с данными о визитах пациентов и специальностях врачей.
    """
)

extract_diagnoses_task = PythonOperator(
    task_id='extract_diagnoses',
    python_callable=extract_diagnoses_data,
    dag=dag,
    doc_md="""
    ### Извлечение данных о диагнозах
    Читает JSON файл с данными о диагнозах пациентов.
    """
)

# Transform задача
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
    doc_md="""
    ### Трансформация данных
    Объединяет данные о пациентах и визитах, определяет самого посещаемого специалиста по возрастным группам.
    """
)

# Load задача
load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
    doc_md="""
    ### Загрузка в базу данных
    Сохраняет результаты анализа в SQLite базу данных.
    """
)

# Генерация отчета
report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
    doc_md="""
    ### Генерация отчета
    Создает детальный текстовый и CSV-отчет с результатами анализа посещаемости врачей.
    """
)

def send_email_with_attachments(**context):
    """
    Отправка email с прикрепленными файлами результатов
    """
    from airflow.utils.email import send_email
    import os
    
    try:
        # Получение данных из предыдущих задач
        report = context['task_instance'].xcom_pull(key='report', task_ids='generate_report')
        result_data = context['task_instance'].xcom_pull(key='result_data', task_ids='generate_report')
        
        # Формирование HTML содержимого с результатами
        html_content = f"""
        <h2>Анализ посещаемости врачей по возрастным группам завершен!</h2>
        
        <h3> Информация о выполнении:</h3>
        <ul>
            <li><strong>DAG:</strong> patient_doctor_visits_analysis_v13</li>
            <li><strong>Дата выполнения:</strong> {context['ds']}</li>
            <li><strong>Статус:</strong> Все задачи выполнены без ошибок</li>
            <li><strong>Результаты:</strong> Проанализированы визиты по возрастным группам</li>
        </ul>
        
        <h3> Краткие результаты:</h3>
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <tr style="background-color: #f2f2f2;">
                <th>Возрастная группа</th>
                <th>Специалист</th>
                <th>Визитов</th>
                <th>Доля (%)</th>
            </tr>
        """
        
        if result_data:
            for row in result_data:
                html_content += f"""
            <tr>
                <td>{row['age_group']}</td>
                <td>{row['top_doctor_specialty']}</td>
                <td>{row['visit_count']}</td>
                <td>{row['percentage']:.2f}</td>
            </tr>
                """
        
        html_content += """
        </table>
        
        <h3>Прикрепленные файлы:</h3>
        <ul>
            <li><strong>patient_visit_analysis_report.txt</strong> - Подробный текстовый отчет</li>
            <li><strong>patient_visit_analysis_data.csv</strong> - Данные в формате CSV</li>
        </ul>
        
        <p><em>Отчет также доступен в логах задачи generate_report в Airflow UI.</em></p>
        
        <hr>
        <p style="color: #666; font-size: 12px;">
            Это автоматическое уведомление от системы Apache Airflow<br>
            Время отправки: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </p>
        """
        
        # Подготовка файлов для отправки
        files = []
        report_file = '/opt/airflow/patient_visit_analysis_report.txt'
        csv_file = '/opt/airflow/patient_visit_analysis_data.csv'
        
        if os.path.exists(report_file):
            files.append(report_file)
            print(f"Добавлен файл для отправки: {report_file}")
        
        if os.path.exists(csv_file):
            files.append(csv_file)
            print(f"Добавлен файл для отправки: {csv_file}")
        
        # Отправка email
        send_email(
            to=['analyst@hospital.local'],
            subject='Результаты анализа: Кто самый посещаемый врач?',
            html_content=html_content,
            files=files
        )
        
        print("Email с результатами и прикрепленными файлами отправлен успешно!")
        return "Email отправлен с прикрепленными файлами"
        
    except Exception as e:
        print(f"Ошибка при отправке email: {str(e)}")
        # Отправляем уведомление без файлов
        send_email(
            to=['analyst@hospital.local'],
            subject='Анализ посещаемости врачей - Завершен (без файлов)',
            html_content=f"""
            <h3>Анализ посещаемости врачей завершен!</h3>
            <p>DAG: patient_doctor_visits_analysis_v13</p>
            <p>Дата выполнения: {context['ds']}</p>
            <p>Все задачи выполнены, но прикрепить файлы не удалось: {str(e)}</p>
            <p>Результаты доступны в логах.</p>
            """
        )
        raise

# Email уведомление с файлами
email_task = PythonOperator(
    task_id='send_email_notification',
    python_callable=send_email_with_attachments,
    dag=dag,
    doc_md="""
    ### Отправка email-уведомления
    Отправляет email с результатами анализа и прикрепленными файлами через MailHog.
    """
)

# Определение зависимостей между задачами
# Extract задачи выполняются параллельно
[extract_patients_task, extract_visits_task, extract_diagnoses_task] >> transform_task

# Transform -> Load -> Report -> Email (последовательно)
transform_task >> load_task >> report_task >> email_task