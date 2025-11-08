# Л+П №4. Разработка аналитического дашборда для бизнес-кейса 
Вариант 13.
Студент: Мареев Г.А.

## Описание проекта

Полный цикл аналитики данных для анализа зарплат рынка Data Science:
- Извлечение данных с Kaggle
- Загрузка в PostgreSQL
- Создание аналитической витрины
- Визуализация в Apache Superset

## Метрики и показатели

*    **Средняя зарплата (USD)** — ключевой индикатор уровня доходов в Data Science
*    **Средняя з/п по уровню опыта** — анализ карьерного роста и влияния квалификации
*    **Доли компаний по размеру** — оценка структуры работодателей (S, M, L)
*    **Комбинированная визуализация**: средняя зарплата (столбцы) и количество вакансий (линия) по `job_title` — выявление востребованных и высокооплачиваемых ролей
*    **Зависимость зарплаты от года** — анализ динамики изменения доходов на рынке труда


## Логика расчета показателей

**Исходные данные**

*  *   **Kaggle Dataset**: [`arnabchaki/data-science-salaries-2025`](https://www.kaggle.com/datasets/arnabchaki/data-science-salaries-2025) — данные о зарплатах в Data Science
*   **Ключевые поля**:
    * `work_year` — год работы
    * `experience_level` — уровень опыта (EN=Entry, MI=Mid, SE=Senior, EX=Executive)
    * `employment_type` — тип занятости (FT=Full-time)
    * `job_title` — должность
    * `company_size` — размер компании (S=Small, M=Medium, L=Large)
    * `salary_in_usd` — зарплата в долларах США

**Бизнес-логика расчета**

```
`AVG(salary_in_usd)` — средняя зарплата
`COUNT(*)` — количество вакансий
```

**Ожидаемый результат.** Интерактивный дашборд с визуализацией ключевых метрик рынка Data Science, позволяющий анализировать тенденции оплаты труда в зависимости от опыта, должности, размера компании и временного периода.

## Подробная архитектура решения

### Общая схема системы

![Архитектура](./img/arch.jpg)


### ETL-процесс

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Kaggle    │───▶│   Airflow   │───▶│ PostgreSQL │──▶│  Superset   │
│   Dataset   │    │   Extract   │    │   Load      │    │ Visualize   │
│             │    │   Transform │    │   Transform │    │             │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

![Схема конвеера ETL](./img/conveer.jpg)

### Подключение Airflow к PostgreSQL

**Строка подключения**: `postgresql://airflow:airflow@postgres:5432/airflow`

**Параметры подключения**:
- **Host**: postgres
- **Port**: 5432
- **Database**: airflow
- **Username**: airflow
- **Password**: airflow

### Настройка коннектора analytics_postgres

**Настройки для коннектора analytics_postgres**

| Поле в Airflow | Значение | Откуда взято (из docker-compose.yml) |
|---|---|---|
| Connection Id | analytics_postgres | Имя, используемое в DAG |
| Connection Type | Postgres | Тип базы данных |
| Host | analytics_postgres | Имя сервиса в Docker Compose |
| Schema | analytics | Из environment: POSTGRES_DB=analytics |
| Login | analytics | Из environment: POSTGRES_USER=analytics |
| Password | analytics | Из environment: POSTGRES_PASSWORD=analytics |
| Port | 5432 | Внутренний порт контейнера |

**Пошаговая инструкция**:
1. Зайдите в Airflow UI: http://localhost:8080
2. Перейдите в Admin -> Connections
3. Нажмите синюю кнопку + ("Add a new record")
4. Заполните поля формы согласно таблице выше
5. Нажмите кнопку Test (должно появиться "Connection successfully tested")
6. Нажмите Save

## Технологический стек

### Основные компоненты

*   **Apache Airflow 2.5.0** — оркестрация ETL-процессов, управление задачами
*   **PostgreSQL 12** — хранение данных и аналитическая витрина
*   **Apache Superset 3.1.1** — интерактивная визуализация и дашборды (стабильная версия)
*   **Kaggle API** — извлечение данных с платформы Kaggle
*   **pgAdmin 4** — веб-интерфейс для администрирования PostgreSQL
*   **Redis 7** — кэширование и сессии для Superset
*   **Docker & Docker Compose** — контейнеризация и оркестрация сервисов

### Python библиотеки

*   **pandas** — обработка и анализ данных
*   **kaggle** — работа с Kaggle API
*   **kagglehub** — скачивание датасетов
*   **psycopg2-binary** — подключение к PostgreSQL
*   **apache-airflow-providers-postgres** — интеграция Airflow с PostgreSQL

### Инфраструктура

*   **Кастомный Dockerfile** — образ Airflow с установленным gcc для компиляции пакетов
*   **Docker Volumes** — постоянное хранение данных
*   **Docker Networks** — изолированная сеть для сервисов

## Быстрый старт

### 1. Настройка Kaggle API
```bash
chmod +x setup_kaggle.sh
./setup_kaggle.sh
```

### 2. Запуск проекта
```bash
    sudo docker compose up -d
```

### 3. Проверка статуса

```bash
sudo docker compose ps
```

### 4. Доступ к сервисам
- **Airflow**: http://localhost:8080 (admin/admin)
- **pgAdmin**: http://localhost:5050 (admin@admin.com/admin)
- **Superset**: http://localhost:8088 (admin/admin)

### 5. Работа с данными
1. В Airflow запустите DAG `ds_salaries_analysis_2025`
2. Убедитесь, что DAG выполнился успешно (статус **Success**)
3. В Superset подключитесь к базе `analytics_postgres`
4. Создайте датасет на основе VIEW `ds_salaries_dashboard`
5. Постройте визуализации и соберите дашборд "Зарплаты в Data Science 2025"



### 6. Сборка дашборда

![Сборка дашборда](./img/ds-dashboard.jpg)

Отлично, вы на правильном пути! Судя по вашему скриншоту, вы почти создали фильтр. Superset подсвечивает красным единственное, чего не хватает — **имени фильтра**.

Вот скорректированная и полная инструкция на основе вашего изображения:
![dashboard](./img/ds-dashboard-filter.jpg)

### 7. Очистка окружения
```bash
chmod +x cleanup.sh
sudo ./cleanup.sh
```

## Структура проекта

```
lpw_4/
├── dags/                          # Основная директория для DAG-файлов Airflow
│   ├── data/                      # Временная папка с примером данных (для отладки)
│   │   └── data_science_salaries_2025.csv
│   ├── data_science_salaries_2025_dag.py  # Основной DAG: ETL для анализа зарплат DS
│   ├── kaggle_test_dag.py         # Тестовый DAG для проверки подключения к Kaggle
│   └── datamart_variant_13.sql    # SQL-скрипт для создания VIEW ds_salaries_dashboard
├── docker-compose.yml             # Конфигурация всех сервисов (Airflow, Superset, Postgres и др.)
├── Dockerfile                     # Кастомный образ Airflow с установленными зависимостями
├── superset.Dockerfile            # Dockerfile для настройки Apache Superset
├── requirements.txt               # Список Python-зависимостей (pandas, kagglehub и т.д.)
├── setup_kaggle.sh                # Bash-скрипт для настройки .kaggle/ и прав доступа
├── cleanup.sh                     # Скрипт для остановки и удаления контейнеров и сетей
├── img/                           # Папка с изображениями для документации
│   ├── arch.jpg                   # Архитектура решения
│   ├── conveer.jpg                # Схема ETL-конвейера
│   ├── ds-dashboard.jpg           # Скриншот финального дашборда
│   ├── ds-dashboard-filter.jpg    # Дашборд с активным фильтром
│   ├── create_conn_superset_sql_01.jpg
│   └── create_conn_superset_sql_02.jpg
└── README.md                      # Данный файл — подробная документация по проекту
```

## Структура данных
![pg1](./img/pg1.png)
![pg2](./img/pg2.png)
![pg_raw](./img/pg_raw.png)
![pg_stg](./img/pg_stg.png)

### Таблица stg_ds_salaries
Основная таблица с обогащенными данными о зарплатах в Data Science:

| Поле | Тип | Описание |
|------|-----|--------|
| work_year | INTEGER | Год работы |
| experience_level | TEXT | Уровень опыта: EN (Entry), MI (Mid), SE (Senior), EX (Executive) |
| employment_type | TEXT | Тип занятости (например, FT — полная ставка) |
| job_title | TEXT | Должность специалиста |
| company_size | TEXT | Размер компании: S (Small), M (Medium), L (Large) |
| salary_in_usd | DOUBLE PRECISION | Зарплата в долларах США (приведена к USD) |

### Представление на основе таблиц


## Устранение неполадок

### Проблемы с правами доступа
```bash
sudo usermod -aG docker $USER
newgrp docker
```

### Проблемы с Kaggle API
```bash
./setup_kaggle.sh
```

### Проблемы с компиляцией
```bash
sudo docker compose build
sudo docker compose up -d
```



