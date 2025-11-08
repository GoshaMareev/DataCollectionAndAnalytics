-- Создание базы данных для анализа производителей автомобилей
-- Задание: Производители, Модели, Продажи — расчет выручки по странам

-- Таблица производителей (brand)
CREATE TABLE IF NOT EXISTS manufacturers (
    brand_id VARCHAR(10) PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица моделей автомобилей
CREATE TABLE IF NOT EXISTS models (
    model_name VARCHAR(200) PRIMARY KEY,
    brand_id VARCHAR(10) REFERENCES manufacturers(brand_id),
    price DECIMAL(12, 2) CHECK (price >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица продаж
CREATE TABLE IF NOT EXISTS sales (
    sale_id SERIAL PRIMARY KEY,
    model_name VARCHAR(200) REFERENCES models(model_name),
    units_sold INTEGER CHECK (units_sold >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_models_brand_id ON models(brand_id);
CREATE INDEX IF NOT EXISTS idx_sales_model_name ON sales(model_name);
CREATE INDEX IF NOT EXISTS idx_manufacturers_country ON manufacturers(country);

-- Представление: консолидированные данные по моделям и производителям
CREATE OR REPLACE VIEW car_sales_consolidated AS
SELECT 
    m.model_name,
    m.price,
    s.units_sold,
    m.price * s.units_sold AS revenue,
    br.brand_name,
    br.country
FROM models m
JOIN sales s ON m.model_name = s.model_name
JOIN manufacturers br ON m.brand_id = br.brand_id;

-- Представление: выручка по странам-производителям
CREATE OR REPLACE VIEW country_revenue AS
SELECT 
    country,
    COUNT(DISTINCT model_name) AS num_models,
    SUM(units_sold) AS total_units_sold,
    AVG(price) AS avg_price,
    SUM(price * units_sold) AS total_revenue
FROM car_sales_consolidated
GROUP BY country
ORDER BY total_revenue DESC;