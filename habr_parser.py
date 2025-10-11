import pandas as pd
from playwright.async_api import async_playwright
import asyncio
import re

df = pd.DataFrame(columns=["Company","Salary_From","Salary_To","Salary_Raw"])

# создадим функцию для извлечения зарплаты из карточки
async def extract_salary(salary_text):
    """
    Парсим строку с зарплатой и возвращаем словарь с from/to.
    - 'от 160000 ₽'
    - 'до 250000 ₽'
    - 'от 180000 ₽ до 220000 ₽'
    - Пусто / не указано
    """
    if not salary_text or "не указана" in salary_text:
        return {"from": None, "to": None, "raw": salary_text}

        # Убираем лишние символы
    clean = re.sub(r"[^\d\sотдо]", "", salary_text.replace("₽", "").strip())
    numbers = list(map(int, re.findall(r"\d+", salary_text)))

    has_from = "от" in salary_text
    has_to = "до" in salary_text

    salary_from = numbers[0] if has_from and len(numbers) > 0 else None
    salary_to = numbers[1] if has_to and len(numbers) > 1 else (
        numbers[0] if has_to and not has_from and len(numbers) == 1 else None
    )

    return {
        "from": salary_from,
        "to": salary_to,
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

        await page.goto("https://career.habr.com/vacancies?type=all", wait_until="domcontentloaded")

        # ждём конкретный элемент поиска (надёжнее чем просто load_state)
        await page.wait_for_selector('input[placeholder="Поиск"]')

        # 3) поиск Pytho Developer
        search = page.get_by_placeholder("Поиск").first
        await search.fill("Python Developer")
        await search.press("Enter")

        # 4) установка фильтров по навыкам "Python" и "Django"
        skills = ['Python', 'Django']
        skill_filter = page.get_by_placeholder("Выберите навык").first
        await skill_filter.wait_for(state="visible", timeout=10000)
        for skill in skills:
            print(f"Добавляем навык: {skill}")

            await skill_filter.focus()
            await skill_filter.fill("")
            await skill_filter.fill(skill)

            # время на появление выпадающего списка
            await page.wait_for_timeout(1000)

            # выбираем первый вариант: стрелка вниз + Enter
            await skill_filter.press("ArrowDown")
            await page.wait_for_timeout(300)
            await skill_filter.press("Enter")
            await page.wait_for_timeout(300)

            # ПРОВЕРКА: ищем именно span.filter-item__title
            try:
                tag_locator = page.locator(f'span.filter-item__title:has-text("{skill}")').first
                await tag_locator.wait_for(state="visible", timeout=5000)
                print(f"Навык '{skill}' успешно добавлен")
            except Exception as e:
                print(f" Тег для '{skill}' не найден сразу")
                # Иногда тег появляется с задержкой — можно запросить список
                all_titles = await page.locator('span.filter-item__title').all_text_contents()
                if skill in all_titles:
                    print(f"Найдено вручную: '{skill}' среди фильтров")
                else:
                    print(f"Так и не удалось добавить '{skill}'")

        # 5) ждем пока карточки загрузятся
        await page.wait_for_selector('div.vacancy-card__info', timeout=10000)
        # 6) Сбор данных
        cards = await page.locator('div.vacancy-card__info').all()
        print(f"Найдено карточек: {len(cards)}")

        data = []

        for card in cards:
            try:
                # Компания — точный селектор по href
                company_el = card.locator('a[href^="/companies/"].link-comp--appearance-dark').first
                if await company_el.count() > 0:
                    company_name = (await company_el.inner_text()).strip()
                else:
                    company_name = "Не указано"
                company_name = company_name.strip()

                # Зарплата (сырой текст)
                salary_el = card.locator('.vacancy-card__salary .basic-salary')
                salary_raw = await salary_el.inner_text() if await salary_el.count() > 0 else "Не указана"

                # Парсим
                salary_parsed = await extract_salary(salary_raw)

                data.append({
                    "Company": company_name,
                    "Salary_From": salary_parsed["from"],
                    "Salary_To": salary_parsed["to"],
                    "Salary_Raw": salary_parsed["raw"]
                })

                print(f"{company_name} | {salary_parsed['raw']}")

            except Exception as e:
                print(f"Ошибка при парсинге карточки: {e}")

        global df
        df = pd.DataFrame(data)
        print("Данные успешно собраны:")
        print(df[["Company", "Salary_From", "Salary_To", "Salary_Raw"]].head())

        applied_filters = await page.locator('span.filter-item__title').all_text_contents()
        print(f"Применённые фильтры: {applied_filters}")
        await asyncio.sleep(10)
        await page.close()
        # await page.pause()  # откроет Playwright Inspector


if __name__ == '__main__':
    asyncio.run(main())