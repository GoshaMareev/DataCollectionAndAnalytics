#!/usr/bin/env python3
"""
Скрипт для проверки результатов анализа в SQLite базе данных
Работает как с локальной базой, так и с базой внутри Docker контейнера
"""

import sqlite3
import pandas as pd
import os
import subprocess
import sys

DB_PATH = 'patient_analysis.db'
CONTAINER_DB_PATH = '/opt/airflow/patient_analysis.db'

def check_docker_container():
    """Проверка наличия запущенного контейнера scheduler"""
    try:
        result = subprocess.run(['sudo', 'docker', 'ps', '--format', '{{.Names}}'], 
                              capture_output=True, text=True, check=True)
        containers = result.stdout.strip().split('\n')
        scheduler_containers = [c for c in containers if 'scheduler' in c]
        return scheduler_containers[0] if scheduler_containers else None
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None

def copy_db_from_container(container_name):
    """Копирование базы данных из контейнера на хост"""
    try:
        print(f"Копируем базу данных из контейнера {container_name}...")
        subprocess.run([
            'sudo', 'docker', 'cp', 
            f'{container_name}:{CONTAINER_DB_PATH}', 
            DB_PATH
        ], check=True)
        print("✅ База данных успешно скопирована из контейнера")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Ошибка при копировании базы данных: {e}")
        return False

def check_database():
    """Проверка результатов в базе данных"""
    
    # Сначала проверяем локальную базу данных
    if not os.path.exists(DB_PATH):
        print(f"База данных {DB_PATH} не найдена локально!")
        
        # Пытаемся найти и скопировать из контейнера
        container_name = check_docker_container()
        if container_name:
            print(f"Найден контейнер scheduler: {container_name}")
            if not copy_db_from_container(container_name):
                print("Не удалось скопировать базу данных из контейнера.")
                print("\n🔧 Попробуйте выполнить вручную:")
                print(f"sudo docker cp {container_name}:{CONTAINER_DB_PATH} {DB_PATH}")
                return
        else:
            print("Контейнер scheduler не найден!")
            print("Убедитесь, что DAG был выполнен успешно и контейнеры запущены.")
            return
    
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Проверка существования таблицы
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("Найденные таблицы в базе данных:")
        for table in tables:
            print(f"  - {table[0]}")
        
        if ('age_group_analysis',) not in tables:
            print("\nТаблица 'age_group_analysis' не найдена!")
            return
        
        # Чтение результатов анализа
        query = """
        SELECT 
            age_group,
            top_doctor_specialty,
            visit_count,
            total_visits,
            percentage,
            analysis_date
        FROM age_group_analysis 
        ORDER BY percentage DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        print(f"\nРЕЗУЛЬТАТЫ АНАЛИЗА ПОСЕЩАЕМОСТИ ВРАЧЕЙ ПО ВОЗРАСТНЫМ ГРУППАМ:")
        print("=" * 80)
        print(df.to_string(index=False))
        
        # Общая статистика
        total_visits = df['total_visits'].sum()
        avg_percentage = df['percentage'].mean()
        
        print(f"\nОБЩАЯ СТАТИСТИКА:")
        print(f"  Общее количество визитов: {total_visits:,}")
        print(f"  Средняя доля лидирующего специалиста: {avg_percentage:.2f}%")
        
        # Лучшая и типичная группа
        best_row = df.iloc[0]
        print(f"\nНАИБОЛЕЕ ЧАСТО ПОСЕЩАЕМЫЙ СПЕЦИАЛИСТ:")
        print(f"  '{best_row['top_doctor_specialty']}' в группе '{best_row['age_group']}' ({best_row['percentage']:.2f}%)")
        
        worst_row = df.iloc[-1]
        print(f"\nНАИМЕНЕЕ ЕДИНООБРАЗНОЕ ПОВЕДЕНИЕ:")
        print(f"  В группе '{worst_row['age_group']}' лидер имеет лишь {worst_row['percentage']:.2f}%")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка при проверке базы данных: {str(e)}")


def copy_result_files():
    """Копирование файлов результатов из контейнера"""
    container_name = check_docker_container()
    if not container_name:
        print("Контейнер scheduler не найден для копирования файлов!")
        return
    
    files_to_copy = [
        ('/opt/airflow/patient_visit_analysis_report.txt', 'patient_visit_analysis_report.txt'),
        ('/opt/airflow/patient_visit_analysis_data.csv', 'patient_visit_analysis_data.csv')
    ]
    print(f"\n📁 Копируем файлы результатов из контейнера {container_name}...")
    
    for container_path, local_path in files_to_copy:
        try:
            subprocess.run([
                'sudo', 'docker', 'cp', 
                f'{container_name}:{container_path}', 
                local_path
            ], check=True)
            print(f"✅ Скопирован файл: {local_path}")
            
            # Показываем размер файла
            if os.path.exists(local_path):
                size = os.path.getsize(local_path)
                print(f"   Размер файла: {size} байт")
                
        except subprocess.CalledProcessError:
            print(f"⚠️  Файл {container_path} не найден в контейнере")
    
    print("\n📄 Доступные файлы результатов:")
    for _, local_path in files_to_copy:
        if os.path.exists(local_path):
            print(f"✅ {local_path}")
        else:
            print(f"❌ {local_path} - не найден")

def show_help():
    """Показать справку по использованию скрипта"""
    print("""
🔍 СКРИПТ ПРОВЕРКИ РЕЗУЛЬТАТОВ АНАЛИЗА КОЭФФИЦИЕНТА УДЕРЖАНИЯ

Использование:
    python3 check_results.py [опция]

Опции:
    (без параметров)  - Проверить результаты в базе данных
    --files          - Скопировать файлы результатов из контейнера
    --help           - Показать эту справку

Примеры:
    python3 check_results.py           # Проверить базу данных
    python3 check_results.py --files   # Скопировать файлы результатов
    python3 check_results.py --help    # Показать справку

Файлы результатов:
    - retention_analysis_report.txt    # Подробный текстовый отчет
    - retention_analysis_data.csv      # Данные в формате CSV
    - mobile_apps_retention.db         # База данных SQLite

Примечание:
    Скрипт автоматически ищет контейнер scheduler и копирует файлы из него.
    Требуются права sudo для работы с Docker.
    """)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == '--files':
            copy_result_files()
        elif sys.argv[1] == '--help':
            show_help()
        else:
            print(f"Неизвестная опция: {sys.argv[1]}")
            print("Используйте --help для справки")
    else:
        check_database()
