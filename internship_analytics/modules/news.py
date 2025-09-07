import base64
import json
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from internship_analytics.conf import DOMAIN_WEIGHTS
from .config.logger_config import get_logger

load_dotenv()

logger = get_logger("news")

IAM_TOKEN = os.environ["YC_IAM_TOKEN"]
FOLDER_ID = os.environ["YC_FOLDER_ID"]


def create_yandex_search_query(user_query: str, domains: List[str]) -> str:
    domain_filters = " | ".join([f"site:{domain}" for domain in domains])
    return f'"{user_query}" ({domain_filters})'


def start_search_task(search_query: str, folder_id: str, iam_token: str, page: int = 0) -> str:
    url = "https://searchapi.api.cloud.yandex.net/v2/web/searchAsync"
    headers = {
        "Authorization": f"Bearer {iam_token}"
    }
    body = {
        "query": {
            "queryText": search_query,
            "searchType": "SEARCH_TYPE_RU",
            "page": str(page)
        },
        "folderId": folder_id,
        "responseFormat": "FORMAT_XML"
    }

    logger.info(f"1. Отправка запроса на запуск поиска (страница {page})...")
    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()

    operation_id = response.json().get("id")
    logger.info(f"   ...Успешно. ID операции: {operation_id}")
    return operation_id


def wait_for_result(operation_id: str, iam_token: str) -> Dict[str, Any]:
    url = f"https://operation.api.cloud.yandex.net/operations/{operation_id}"
    headers = {
        "Authorization": f"Bearer {iam_token}"
    }
    logger.info("2. Ожидание завершения операции...")
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        if data.get("done"):
            logger.info("   ...Операция завершена.")
            return data
        logger.info("   ...Поиск еще выполняется, ожидание 15 секунд.")
        time.sleep(15)


def get_result_xml(operation_data: Dict[str, Any]) -> Optional[str]:
    logger.info("3. Извлечение XML из ответа...")
    try:
        raw_data_base64 = operation_data["response"]["rawData"]
        decoded_bytes = base64.b64decode(raw_data_base64)
        xml_result = decoded_bytes.decode('utf-8')
        logger.info(f"   ...XML успешно извлечен.")
        return xml_result
    except KeyError:
        logger.error("Ошибка: в ответе не найдены данные. Ответ сервера:")
        logger.error(json.dumps(operation_data, indent=2, ensure_ascii=False))
        return None


def parse_search_results(xml_content: Optional[str]) -> List[Dict[str, Any]]:
    if not xml_content:
        return []

    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.error(f"Ошибка при парсинге XML: {e}")
        return []

    articles = []
    grouping = root.find('.//grouping')
    if grouping is None or not grouping.findall('.//doc'):
        logger.info("   ...На этой странице релевантных документов не найдено.")
        return []

    for doc in root.findall('.//doc'):
        title_element = doc.find('title')
        passage_element = doc.find('.//passage')

        if title_element is None or passage_element is None:
            continue

        title = "".join(title_element.itertext()).strip()
        passage = "".join(passage_element.itertext()).strip()

        domain = doc.find('domain').text if doc.find('domain') is not None else "N/A"
        url = doc.find('url').text if doc.find('url') is not None else "N/A"

        modtime_str = doc.find('modtime').text if doc.find('modtime') is not None else ""
        try:
            date_obj = datetime.strptime(modtime_str, '%Y%m%dT%H%M%S')
            date = date_obj.strftime('%Y-%m-%d')
        except ValueError:
            date = "N/A"

        weight = DOMAIN_WEIGHTS.get(domain, "—")

        articles.append({
            "date": date,
            "title": title,
            "summary": passage,
            "source": domain,
            "url": url,
            "weight": weight,
            "full_text": None
        })

    return articles


def extract_full_article_text(url: str, domain: str) -> Optional[str]:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        article_body = None
        selectors = []

        if "rbc.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'article__text'},
                {'tag': 'div', 'class_': 'article__body'}
            ]
        elif "kommersant.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'article_text'},
                {'tag': 'div', 'class_': 'js-article-text'}
            ]
        elif "vedomosti.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'article-body'}
            ]
        elif "tass.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'text-block'}
            ]
        elif "ria.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'article__body'}
            ]
        elif "interfax.ru" in domain:
            selectors = [
                {'tag': 'article', 'itemprop': 'articleBody'}
            ]
        elif "forbes.ru" in domain:
            selectors = [
                {'tag': 'div', 'class_': 'article-body'}
            ]

        for selector in selectors:
            tag = selector.pop('tag', 'div')
            if 'class_' in selector:
                selector['class'] = selector.pop('class_')
            article_body = soup.find(tag, **selector)
            if article_body:
                break

        if not article_body:
            article_body = soup.find('article') or soup.find('main') or soup.body

        if article_body:
            for ad_element in article_body.select('.adv, .subscription-block, .banner'):
                ad_element.decompose()

            paragraphs = article_body.find_all('p', recursive=True)
            full_text = "\n".join([p.get_text(separator=' ', strip=True) for p in paragraphs if p.get_text(strip=True)])
            return full_text if full_text else None

        return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при запросе к {url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при парсинге {url}: {e}")
        return None


def run_full_search_and_parse(user_search_query: str, domains_to_search: List[str], num_pages: int,
                              path_to_output: str) -> str:
    full_query = create_yandex_search_query(user_search_query, domains_to_search)
    logger.info(f"Сформирован поисковый запрос: {full_query}\n")

    all_articles = []

    for page_num in range(num_pages):
        try:
            op_id = start_search_task(full_query, FOLDER_ID, IAM_TOKEN, page=page_num)
            if op_id:
                final_data = wait_for_result(op_id, IAM_TOKEN)
                xml_data = get_result_xml(final_data)
                parsed_page_data = parse_search_results(xml_data)

                if not parsed_page_data:
                    logger.info(f"На странице {page_num} больше нет результатов. Завершаю поиск.")
                    break

                all_articles.extend(parsed_page_data)
                time.sleep(2)

        except requests.exceptions.HTTPError as e:
            logger.error(f"Произошла ошибка HTTP: {e.response.status_code}")
            logger.error(f"Ответ сервера: {e.response.text}")
            break
        except Exception as e:
            logger.error(f"Произошла непредвиденная ошибка на странице {page_num}: {e}")
            break

    if not all_articles:
        logger.info("Поиск не дал результатов.")
        return None

    logger.info(
        f"Всего найдено {len(all_articles)} статей на {num_pages} страницах. Начинаю извлечение полного текста...")

    unique_articles = list({article['url']: article for article in all_articles}.values())
    logger.info(f"После удаления дубликатов осталось {len(unique_articles)} уникальных статей.")

    processed_count = 0
    for article in unique_articles:
        logger.info(f"Обработка статьи: {article['url']}")
        time.sleep(1)
        article['full_text'] = extract_full_article_text(article['url'], article['source'])
        if article['full_text']:
            logger.info(f"   ...Полный текст извлечен для {article['url']}")
            processed_count += 1
        else:
            logger.warning(f"   ...Не удалось извлечь полный текст для {article['url']}")

    output_filename = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_parsed.json"
    output_filepath = os.path.join(path_to_output, output_filename)

    os.makedirs(path_to_output, exist_ok=True)

    with open(output_filepath, 'w', encoding='utf-8') as f:
        json.dump(unique_articles, f, ensure_ascii=False, indent=2)

    logger.info(f"Результат сохранен в файл: {output_filepath}")

    return output_filepath
