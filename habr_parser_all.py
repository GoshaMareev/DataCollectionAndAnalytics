import pandas as pd
from playwright.async_api import async_playwright
import asyncio
import re

df = pd.DataFrame(columns=["Company","Salary_From","Salary_To","Salary_Raw"])

EXCHANGE_RATES = {
    'USD': 81.0,
    'EUR': 94.0,
}

# создадим функцию для извлечения зарплаты из карточки
async def extract_salary(salary_text):
    """
    Парсит зарплату с учётом пробелов как разделителей тысяч.
    """
    if not salary_text or "не указана" in salary_text.lower():
        return {"from": None, "to": None, "raw": salary_text.strip()}

    text = salary_text.strip()

    # Определяем валюту
    currency = 'RUB'
    if '$' in text or 'usd' in text.lower():
        currency = 'USD'
    elif '€' in text or 'eur' in text.lower():
        currency = 'EUR'

    # Извлекаем числа: любые последовательности цифр, возможно с пробелами внутри
    # Например: "100 000" → "100000"
    raw_numbers = re.findall(r'\d[\d\s]*\d|\d+', text)
    numbers = []
    for num_str in raw_numbers:
        cleaned = num_str.replace(' ', '').replace('\u2009', '')  # Убираем тонкие пробелы
        try:
            numbers.append(int(cleaned))
        except ValueError:
            continue  # На всякий случай

    has_from = bool(re.search(r'от', text, re.IGNORECASE))
    has_to = bool(re.search(r'до', text, re.IGNORECASE))

    salary_from = None
    salary_to = None

    if has_from and has_to and len(numbers) >= 2:
        salary_from = numbers[0]
        salary_to = numbers[1]
    elif has_from and not has_to and len(numbers) == 1:
        salary_from = numbers[0]
    elif not has_from and has_to and len(numbers) == 1:
        salary_to = numbers[0]
    elif len(numbers) == 1:
        # Если число одно и нет указаний — можно присвоить как from
        salary_from = numbers[0]

    # Конвертируем в рубли
    def convert(amount, curr):
        if amount is None:
            return None
        if curr == 'USD':
            return int(amount * EXCHANGE_RATES['USD'])
        elif curr == 'EUR':
            return int(amount * EXCHANGE_RATES['EUR'])
        else:
            return int(amount)

    salary_from_rub = convert(salary_from, currency)
    salary_to_rub = convert(salary_to, currency)

    return {
        "from": salary_from_rub,
        "to": salary_to_rub,
        "raw": salary_text.strip()
    }
async def main():
    global df
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_navigation_timeout(60000)
        page.set_default_timeout(15000)

        # ========================
        # 1. СБОР С ФИЛЬТРАМИ: Python + Django
        # ========================
        print("\n🔍 Сбор вакансий с фильтрами: Python, Django")
        await page.goto("https://career.habr.com/vacancies?type=all", wait_until="domcontentloaded")
        await page.wait_for_selector('input[placeholder="Поиск"]')

        search = page.get_by_placeholder("Поиск").first
        await search.fill("Python Developer")
        await search.press("Enter")

        skills = ['Python', 'Django']
        skill_filter = page.get_by_placeholder("Выберите навык").first
        await skill_filter.wait_for(state="visible", timeout=10000)
        for skill in skills:
            print(f"Добавляем навык: {skill}")
            await skill_filter.focus()
            await skill_filter.fill("")
            await skill_filter.fill(skill)
            await page.wait_for_timeout(1000)
            await skill_filter.press("ArrowDown")
            await page.wait_for_timeout(300)
            await skill_filter.press("Enter")
            await page.wait_for_timeout(300)

            try:
                tag_locator = page.locator(f'span.filter-item__title:has-text("{skill}")').first
                await tag_locator.wait_for(state="visible", timeout=5000)
            except:
                all_titles = await page.locator('span.filter-item__title').all_text_contents()
                if skill not in all_titles:
                    print(f"⚠️ Не удалось добавить фильтр: {skill}")

        await page.wait_for_selector('div.vacancy-card__info', timeout=10000)
        cards = await page.locator('div.vacancy-card__info').all()
        print(f"Найдено карточек (с фильтрами): {len(cards)}")

        data_filtered = []
        for card in cards:
            try:
                company_el = card.locator('a[href^="/companies/"].link-comp--appearance-dark').first
                company_name = (await company_el.inner_text()).strip() if await company_el.count() > 0 else "Не указано"

                salary_el = card.locator('.vacancy-card__salary .basic-salary')
                salary_raw = await salary_el.inner_text() if await salary_el.count() > 0 else "Не указана"
                salary_parsed = await extract_salary(salary_raw)

                data_filtered.append({
                    "Company": company_name,
                    "Salary_From": salary_parsed["from"],
                    "Salary_To": salary_parsed["to"],
                    "Salary_Raw": salary_parsed["raw"]
                })
            except Exception as e:
                print(f"Ошибка при парсинге карточки: {e}")

        df_filtered = pd.DataFrame(data_filtered)
        print(f"✅ Сбор с фильтрами завершён. Вакансий: {len(df_filtered)}")

        # ========================
        # 2. СБОР БЕЗ ФИЛЬТРОВ: только "Python Developer"
        # ========================
        print("\n🌍 Сбор вакансий без фильтров: только Python Developer")
        await page.goto("https://career.habr.com/vacancies?type=all", wait_until="domcontentloaded")  # ✅ Без пробелов!
        await page.wait_for_selector('input[placeholder="Поиск"]')

        search = page.get_by_placeholder("Поиск").first
        await search.fill("Python Developer")
        await search.press("Enter")

        # Ждём загрузки результатов
        await page.wait_for_selector('div.vacancy-card__info', timeout=10000)
        cards = await page.locator('div.vacancy-card__info').all()
        print(f"Найдено карточек (без фильтров): {len(cards)}")

        data_full = []
        for card in cards:
            try:
                company_el = card.locator('a[href^="/companies/"].link-comp--appearance-dark').first
                company_name = (await company_el.inner_text()).strip() if await company_el.count() > 0 else "Не указано"

                salary_el = card.locator('.vacancy-card__salary .basic-salary')
                salary_raw = await salary_el.inner_text() if await salary_el.count() > 0 else "Не указана"
                salary_parsed = await extract_salary(salary_raw)

                data_full.append({
                    "Company": company_name,
                    "Salary_From": salary_parsed["from"],
                    "Salary_To": salary_parsed["to"],
                    "Salary_Raw": salary_parsed["raw"]
                })
            except Exception as e:
                print(f"Ошибка при парсинге карточки: {e}")

        df_full = pd.DataFrame(data_full)
        print(f"✅ Сбор без фильтров завершён. Вакансий: {len(df_full)}")


        # ========================
        # 3. АНАЛИЗ И СРАВНЕНИЕ МЕДИАН
        # ========================
        def calculate_median(df):
            df_temp = df.dropna(subset=['Salary_From', 'Salary_To'], how='all').copy()  # ← .copy()
            df_temp['Mid_Salary'] = df_temp.apply(
                lambda row: (row['Salary_From'] + row['Salary_To']) / 2
                if pd.notna(row['Salary_From']) and pd.notna(row['Salary_To'])
                else row['Salary_From'] if pd.notna(row['Salary_From'])
                else row['Salary_To'] if pd.notna(row['Salary_To'])
                else None,
                axis=1
            )
            return df_temp['Mid_Salary'].dropna().median()

        median_filtered = calculate_median(df_filtered)  # с фильтрами Python, Django
        median_full = calculate_median(df_full)          # все Python Developer

        print("\n" + "="*60)
        print("📊 СРАВНЕНИЕ МЕДИАННЫХ ЗАРПЛАТ НА HABR CAREER")
        print("="*60)
        print(f"🔹 С фильтрами (Python + Django):     {median_filtered:,.0f} ₽")
        print(f"🔹 Без фильтров (все Python dev):     {median_full:,.0f} ₽")

        if median_filtered > median_full:
            diff = ((median_filtered - median_full) / median_full) * 100
            print(f"📈 Вывод: Зарплаты с фильтрами выше на {diff:.1f}%")
            print("💡 Это может означать, что вакансии с указанием Django — более профильные и лучше оплачиваемые.")
        elif median_filtered < median_full:
            diff = ((median_full - median_filtered) / median_full) * 100
            print(f"📉 Вывод: Зарплаты с фильтрами ниже на {diff:.1f}%")
            print("💡 Возможно, фильтр добавляет больше junior-вакансий или требует меньшей экспертизы.")
        else:
            print("🔸 Вывод: Уровень зарплат практически одинаков.")

        # ========================
        # 4. СОХРАНЕНИЕ В ФАЙЛЫ
        # ========================
        df_filtered.to_csv("habr_python_django.csv", index=False, encoding='utf-8-sig', quoting=1)
        df_full.to_csv("habr_python_all.csv", index=False, encoding='utf-8-sig', quoting=1)
        print(f"\n✅ Данные сохранены:")
        print("   - habr_python_django.csv (с фильтрами)")
        print("   - habr_python_all.csv (все вакансии)")

        await page.close()

if __name__ == '__main__':
    asyncio.run(main())