import pandas as pd
from playwright.async_api import async_playwright
import asyncio
import re
df = pd.DataFrame()
async def main():
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

        #4) установка фильтров по навыкам "Python" и "Django"
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

        #1 version
        # await search_filter.fill("Python")
        # await asyncio.sleep(3)
        # await search_filter.press("ArrowDown")
        # await search_filter.press("Enter")
        # await asyncio.sleep(1)
        # await search_filter.fill("Django")
        # await asyncio.sleep(3)
        # await search_filter.press("ArrowDown")
        # await search_filter.press("Enter")
        #
        # await asyncio.sleep(10)

        applied_filters = await page.locator('span.filter-item__title').all_text_contents()
        print(f"Применённые фильтры: {applied_filters}")
        await asyncio.sleep(10)
        await page.close()
        # await page.pause()  # откроет Playwright Inspector

if __name__ == '__main__':
    asyncio.run(main())