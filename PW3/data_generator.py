"""
Генератор синтетических данных для задания по консолидации и анализу данных автомобилей
Создает три файла:
1. manufacturers.csv  - производители: brand_id, brand_name, country
2. models.xlsx         - модели: model_name, brand_id, price
3. sales.json          - продажи: model_name, units_sold
"""

import pandas as pd
import numpy as np
import json
import os
import random

# Настройка генератора случайных чисел для воспроизводимости
np.random.seed(42)
random.seed(42)

def generate_manufacturers_data():
    """Генерация данных о производителях автомобилей (CSV)"""
    countries = ["Германия", "Япония", "США", "Южная Корея", "Франция", "Италия", "Швеция", "Великобритания"]

    brands_by_country = {
        "Германия": ["BMW", "Mercedes-Benz", "Audi", "Porsche", "Volkswagen"],
        "Япония": ["Toyota", "Honda", "Nissan", "Lexus", "Mazda", "Subaru"],
        "США": ["Ford", "Chevrolet", "Tesla", "Dodge", "Jeep"],
        "Южная Корея": ["Hyundai", "Kia", "Genesis"],
        "Франция": ["Renault", "Peugeot", "Citroën"],
        "Италия": ["Fiat", "Alfa Romeo", "Maserati"],
        "Швеция": ["Volvo", "Polestar"],
        "Великобритания": ["Jaguar", "Land Rover", "Mini"]
    }

    manufacturers_data = []
    brand_counter = 1

    for country in countries:
        for brand_name in brands_by_country[country]:
            brand_id = f"BR{brand_counter:03d}"
            manufacturers_data.append({
                "brand_id": brand_id,
                "brand_name": brand_name,
                "country": country
            })
            brand_counter += 1

    df_manufacturers = pd.DataFrame(manufacturers_data)
    df_manufacturers.to_csv('data/manufacturers.csv', index=False, encoding='utf-8')
    print("✓ Файл manufacturers.csv создан")
    return manufacturers_data


def generate_models_data(manufacturers_data):
    """Генерация данных о моделях автомобилей (Excel)"""
    base_models = {
        "sedan": ["Sedan", "Saloon", "Comfort", "Executive"],
        "hatchback": ["Hatch", "Urban", "City", "Active"],
        "suv": ["SUV", "Explorer", "Trail", "Adventure", "Cross"],
        "coupe": ["Coupe", "GT", "Sport", "Racer"],
        "truck": ["Pickup", "Ranger", "Warrior", "Beast"]
    }

    # Диапазоны цен по стране (в рублях)
    price_ranges = {
        "Германия": (2500000, 12000000),
        "Япония": (1200000, 6000000),
        "США": (1500000, 8000000),
        "Южная Корея": (900000, 4500000),
        "Франция": (1000000, 5000000),
        "Италия": (2000000, 10000000),
        "Швеция": (1800000, 7000000),
        "Великобритания": (2200000, 9000000)
    }

    models_data = []
    model_counter = 1

    for manufacturer in manufacturers_data:
        brand_id = manufacturer["brand_id"]
        brand_name = manufacturer["brand_name"]
        country = manufacturer["country"]
        
        # Количество моделей у бренда
        num_models = random.randint(2, 5)
        
        for _ in range(num_models):
            body_type = random.choice(list(base_models.keys()))
            model_suffix = random.choice(base_models[body_type])
            model_name = f"{brand_name} {model_suffix}-{random.randint(100, 999)}"

            min_price, max_price = price_ranges[country]
            price = random.randint(min_price // 10000, max_price // 10000) * 10000  # округляем до 10k

            models_data.append({
                "model_name": model_name,
                "brand_id": brand_id,
                "price": price
            })

            model_counter += 1

    df_models = pd.DataFrame(models_data)
    df_models.to_excel('data/models.xlsx', index=False)
    print("✓ Файл models.xlsx создан")
    return models_data


def generate_sales_data(models_data):
    """Генерация данных о продажах (JSON)"""
    sales_data = []

    for model in models_data:
        model_name = model["model_name"]

        # Продажи зависят от типа бренда и модели
        if "Tesla" in model_name or "BMW" in model_name or "Mercedes" in model_name:
            units_sold = random.randint(500, 3000)      # Популярные люксовые/электро
        elif "Toyota" in model_name or "Honda" in model_name:
            units_sold = random.randint(2000, 8000)     # Массовые японцы
        elif "Hyundai" in model_name or "Kia" in model_name:
            units_sold = random.randint(1500, 6000)
        else:
            units_sold = random.randint(300, 2500)      # Остальные

        sales_data.append({
            "model_name": model_name,
            "units_sold": units_sold
        })

    # Сохраняем в JSON
    with open('data/sales.json', 'w', encoding='utf-8') as f:
        json.dump(sales_data, f, ensure_ascii=False, indent=2)

    print("✓ Файл sales.json создан")
    return sales_data


def main():
    """Основная функция генерации данных"""
    print("Генерация синтетических данных для анализа автомобилей...")
    print("=" * 60)

    # Создаем папку data если её нет
    if not os.path.exists('data'):
        os.makedirs('data')
        print("✓ Создана папка 'data'")

    # Генерируем данные
    manufacturers_data = generate_manufacturers_data()
    models_data = generate_models_data(manufacturers_data)
    sales_data = generate_sales_data(models_data)

    print("=" * 60)
    print(f"Сгенерировано:")
    print(f"- Производителей: {len(manufacturers_data)}")
    print(f"- Моделей: {len(models_data)}")
    print(f"- Записей о продажах: {len(sales_data)}")
    print("\nФайлы готовы к анализу:")
    print("  data/manufacturers.csv")
    print("  data/models.xlsx")
    print("  data/sales.json")


if __name__ == "__main__":
    main()