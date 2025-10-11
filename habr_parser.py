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

        #ждём конкретный элемент поиска (надёжнее чем просто load_state)
        await page.wait_for_selector('input[placeholder="Поиск"]')

        # 3) поиск Pytho Developer
        search = page.get_by_placeholder("Поиск").first
        await search.fill("Python Developer")
        await search.press("Enter")

        #4) Установка фильтров по навыкам "Python" и "Django"
        search_filter = page.get_by_placeholder("Выберите навык").first
        await search_filter.fill("Python")
        await asyncio.sleep(3)
        await search_filter.press("ArrowDown")
        await search_filter.press("Enter")
        await asyncio.sleep(1)
        await search_filter.fill("Django")
        await asyncio.sleep(3)
        await search_filter.press("ArrowDown")
        await search_filter.press("Enter")

        await asyncio.sleep(10)  # ← страница останется открытой 10 секунд

        await page.close()
        # await page.pause()  # откроет Playwright Inspector

if __name__ == '__main__':
    asyncio.run(main())