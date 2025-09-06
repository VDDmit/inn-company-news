import asyncio
import json
import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional

from pydoll.browser import Chrome, tab
from pydoll.browser.options import ChromiumOptions

from config.logger_config import get_logger

logger = get_logger(__name__)

OUTPUT_DIR = Path("../output/scraped_data")
OUTPUT_FILENAME = "scraped_with_pydoll_results.json"
MAX_CONCURRENT_TASKS = 5
PAGE_LOAD_TIMEOUT = 60
SEARCH_TIMEOUT = 30


class DuckDuckGoScraper:
    SEARCH_URL_TEMPLATE = 'https://html.duckduckgo.com/html/?q={query}'
    RESULT_LINK_SELECTOR = 'a.result__a'

    def __init__(self, headless: bool = True, max_concurrent_tasks: int = 5):
        self.headless = headless
        self.semaphore = asyncio.Semaphore(max_concurrent_tasks)
        self.browser_options = self._create_browser_options()
        logger.info(
            f"Скрапер инициализирован. Режим Headless: {headless}. "
            f"Максимум одновременных задач: {max_concurrent_tasks}."
        )

    def _create_browser_options(self) -> ChromiumOptions:
        options = ChromiumOptions()
        if self.headless:
            options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--blink-settings=imagesEnabled=false')
        return options

    async def _extract_urls_from_search_page(self, main_tab: tab, search_query: str) -> List[str]:
        encoded_query = urllib.parse.quote_plus(search_query)
        search_url = self.SEARCH_URL_TEMPLATE.format(query=encoded_query)

        logger.info(f"Навигация к поисковой выдаче: {search_url}")
        await main_tab.go_to(search_url, timeout=SEARCH_TIMEOUT)

        logger.info("Извлечение URL-адресов из результатов поиска...")
        link_elements = await main_tab.query(
            self.RESULT_LINK_SELECTOR,
            find_all=True,
            timeout=20
        )

        if not link_elements:
            logger.warning("Не найдено ни одного элемента с результатами поиска.")
            return []

        search_result_urls = set()
        for element in link_elements:
            href = element.get_attribute('href')
            if not href:
                continue
            try:
                parsed_href = urllib.parse.urlparse(href)
                query_params = urllib.parse.parse_qs(parsed_href.query)
                if 'uddg' in query_params and query_params['uddg'][0]:
                    actual_url = query_params['uddg'][0]
                    search_result_urls.add(actual_url)
            except (KeyError, IndexError, Exception) as e:
                logger.warning(f"Не удалось извлечь URL из href '{href}': {e}")

        unique_urls = list(search_result_urls)
        logger.info(f"Найдено {len(unique_urls)} уникальных URL-адресов.")
        return unique_urls

    async def _scrape_single_page(self, browser: Chrome, url: str) -> Optional[Dict[str, str]]:
        async with self.semaphore:
            logger.info(f"Начинаю обработку: {url}")
            current_tab = None
            try:
                current_tab = await browser.new_tab()
                await current_tab.go_to(url, timeout=PAGE_LOAD_TIMEOUT)

                content_raw = await current_tab.execute_script('return document.body.innerText;')

                actual_content = ""
                if isinstance(content_raw, str):
                    actual_content = content_raw
                elif isinstance(content_raw, dict) and 'value' in content_raw:
                    actual_content = str(content_raw.get('value', ''))
                elif content_raw is None:
                    logger.warning(f"Контент для {url} не был получен (None).")
                else:
                    logger.warning(f"Неожиданный тип контента {type(content_raw)} для {url}. Конвертирую в строку.")
                    actual_content = str(content_raw)

                return {'url': url, 'content': actual_content.strip()}
            except Exception as e:
                logger.error(f"Ошибка при скрапинге {url}: {e}", exc_info=False)
                return None
            finally:
                if current_tab:
                    try:
                        await current_tab.close()
                    except Exception as e:
                        logger.error(f"Не удалось закрыть вкладку для {url}: {e}")

    async def run(self, search_query: str) -> List[Dict[str, str]]:
        logger.info(f"--- Начало нового сеанса скрапинга для запроса: '{search_query}' ---")
        scraped_data = []

        async with Chrome(options=self.browser_options) as browser:
            main_tab = await browser.start()

            try:
                urls_to_scrape = await self._extract_urls_from_search_page(main_tab, search_query)
            finally:
                await main_tab.close()

            if not urls_to_scrape:
                logger.info("URL для скрапинга не найдены. Завершение работы.")
                return []

            logger.info(f"Запуск {len(urls_to_scrape)} задач на скрапинг страниц...")
            tasks = [self._scrape_single_page(browser, url) for url in urls_to_scrape]

            results = await asyncio.gather(*tasks)

            scraped_data = [result for result in results if result is not None]

        logger.info(f"--- Сеанс скрапинга завершен. Собрано данных с {len(scraped_data)} страниц. ---")
        return scraped_data


def save_results_to_json(data: List[Dict], output_dir: Path, filename: str):
    if not data:
        logger.info("Нет данных для сохранения.")
        return

    output_dir.mkdir(exist_ok=True)
    output_path = output_dir / filename

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        logger.info(f"Данные ({len(data)} записей) успешно сохранены в файл: {output_path}")
    except IOError as e:
        logger.error(f"Ошибка при сохранении файла {output_path}: {e}")


async def run_duckduckgo_scraper_task(
        search_query: str,
        headless: bool = True,
        max_concurrent_tasks: int = MAX_CONCURRENT_TASKS,
        output_dir: Path = OUTPUT_DIR,
        output_filename: str = OUTPUT_FILENAME
) -> List[Dict[str, str]]:
    logger.info(f"--- Запуск задачи скрапинга DuckDuckGo для запроса: '{search_query}' ---")

    scraper = DuckDuckGoScraper(
        headless=headless,
        max_concurrent_tasks=max_concurrent_tasks
    )
    scraped_data = await scraper.run(search_query)

    save_results_to_json(scraped_data, output_dir, output_filename)

    if scraped_data:
        logger.info(f"Скрапинг для запроса '{search_query}' успешно завершен. Данные сохранены в JSON.")
        logger.info("\n--- Пример извлеченных данных (первая запись) ---")
        if scraped_data:
            logger.info(json.dumps(scraped_data[0], indent=2, ensure_ascii=False))
    else:
        logger.warning(f"Не удалось получить данные для запроса '{search_query}'.")

    logger.info(f"--- Задача скрапинга DuckDuckGo для запроса: '{search_query}' завершена ---")
    return scraped_data
