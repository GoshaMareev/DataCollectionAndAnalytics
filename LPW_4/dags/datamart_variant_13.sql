-- Витрина данных для анализа зарплат в Data Science 2025
-- Вариант задания №13
-- Создает VIEW на основе обогащенных данных из stg_ds_salaries

DROP VIEW IF EXISTS ds_salaries_dashboard;

CREATE VIEW ds_salaries_dashboard AS
SELECT 
    experience_level,
    employment_type,
    job_title,
    company_size,
    salary_in_usd,
    work_year
FROM 
    stg_ds_salaries
WHERE
    -- Фильтрация корректных данных
    salary_in_usd > 0
    AND experience_level IN ('EN', 'MI', 'SE', 'EX')  -- Entry, Mid, Senior, Executive
    AND employment_type = 'FT'  -- Только полная занятость (по условию датасета)
    AND company_size IN ('S', 'M', 'L');  -- Small, Medium, Large

-- Комментарий к витрине
COMMENT ON VIEW ds_salaries_dashboard IS 
'Аналитическая витрина по зарплатам в Data Science за 2025 год Готова к визуализации.';