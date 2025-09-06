import json
import os
import time
from decimal import Decimal

import ijson
from dotenv import load_dotenv
from google import genai

from .config.logger_config import get_logger

load_dotenv()
logger = get_logger("gemini_data_processor")

gemini_client = None
try:
    api_key = os.environ["GENAI_API_KEY"]
    gemini_client = genai.Client(api_key=api_key)
except KeyError:
    logger.critical("GENAI_API_KEY не найден в переменных окружения. Завершение работы.")
    exit(1)

PROMPT_1 = """
Твоя задача — очистить предоставленный сырой текст, извлекая из него только связный и осмысленный контент, относящийся к основной теме документа.
Инструкции:
1. Внимательно проанализируй весь текст.
2. Удали весь "мусор":
   - Навигационные ссылки (например, 'Описание', 'Похожие компании', 'Контакты', 'Арбитражные дела').
   - Списки других компаний или организаций, если они не являются частью основного повествования.
   - Повторяющиеся блоки с реквизитами (ИНН, ОГРН, Уставной капитал и т.д.) из длинных списков.
   - Стандартные фразы и оговорки (например, 'Бухгалтерская (финансовая) отчетность публикуется...', 'Все данные о наименовании торговой марки... актуальны...', 'Проверить информацию и посмотреть отзывы можно онлайн...').
3. Сохрани только те абзацы и предложения, которые напрямую описывают объект, событие или компанию, являющуюся главной темой текста.
4. Объедини оставшийся текст в единый, гладкий и читаемый фрагмент. Не добавляй от себя никакой информации, просто отфильтруй исходный материал.
Сырой текст для очистки:
---
"""

PROMPT_2_TEMPLATE = """
Определи, является ли предоставленный текст релевантным запросу '{context_query}'.

Текст считается релевантным, если '{context_query}' является его ОСНОВНОЙ темой.
Текст НЕ считается релевантным, если '{context_query}' просто упоминается вскользь, в списке других компаний без подробного описания, или в качестве второстепенного примера.

Проанализируй текст и ответь ОДНИМ СЛОВОМ: 'да' или 'нет'.

Текст для анализа:
---
{text_content}
---
"""

PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE = """
Твоя задача — сделать краткую, но ёмкую выжимку из предоставленных новостных текстов по теме '{context_query}'.
Выдели только ключевые факты, события и цифры. Игнорируй "воду" и второстепенные детали.
Объедини информацию, если она дублируется.
Результат представь в виде списка тезисов.

Тексты для анализа:
---
{chunk_texts}
---
"""

PROMPT_3_FINAL_SUMMARY_TEMPLATE = """
Ты — экспертный аналитик. Твоя задача — создать единую, целостную и структурированную сводку на основе нескольких предварительных отчетов.
Сводку делай с таблицей источников в конце и коэффициентами weight(например "weight": 0.95,) на эти источники+релевантный текст взятый оттуда(3 столбца)

Основная тема сводки: '{context_query}'.

Инструкции:
1. Внимательно изучи все предоставленные тезисные отчеты.
2. Синтезируй информацию из всех источников в один связный итоговый отчет.
3. Устрани дублирующуюся информацию.
4. Выдели и представь только самые важные факты, события, цифры и выводы.
5. Структурируй итоговый текст с помощью заголовков, подзаголовков и списков для лучшей читаемости.
6. Язык отчета — русский.

Тезисные отчеты для анализа:
---
{combined_summaries}
---
"""
GEMINI_MODEL_1 = 'models/gemini-1.5-flash-latest'
GEMINI_MODEL_2 = 'models/gemini-1.5-flash-latest'
GEMINI_MODEL_3 = 'models/gemini-2.5-pro'


def json_serializer(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def stream_json_objects(file_path: str):
    try:
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, 'item')
            for record in parser:
                yield record
    except FileNotFoundError:
        logger.error(f"Файл не найден: {file_path}")
    except Exception as e:
        logger.error(f"Ошибка при потоковом чтении файла {file_path}: {e}")


def _call_gemini_api(prompt: str, model: str) -> str:
    global gemini_client
    try:
        response = gemini_client.models.generate_content(
            model=model,
            contents=prompt
        )
        return response.text.strip()
    except ValueError:
        logger.warning(f"Получен пустой или заблокированный ответ от модели {model}.")
        return ""
    except Exception as e:
        logger.error(f"Ошибка при вызове API Gemini для модели {model}: {e}")
        return ""


def clean_raw_data(input_file_path: str, output_file_path: str):
    logger.info("--- НАЧАЛО УРОВНЯ 1: Потоковая очистка сырых данных ---")

    processed_count = 0
    try:
        with open(output_file_path, 'w', encoding='utf-8') as f_out:
            f_out.write('[')
            is_first_item = True

            for item in stream_json_objects(input_file_path):
                content_to_clean = "\n".join(filter(None, [
                    item.get('title', ''),
                    item.get('summary', ''),
                    item.get('full_text', '')
                ]))

                if not content_to_clean.strip():
                    logger.warning(f"Пропуск записи с URL {item.get('url')} из-за отсутствия текстового контента.")
                    continue

                logger.info(f"Очистка записи: {item.get('url', 'N/A')}")
                prompt = PROMPT_1 + "\n" + content_to_clean
                cleaned_text = _call_gemini_api(prompt, GEMINI_MODEL_1)

                item['cleaned_text'] = cleaned_text

                if not is_first_item:
                    f_out.write(',')
                json.dump(item, f_out, ensure_ascii=False, indent=2, default=json_serializer)
                is_first_item = False
                processed_count += 1
                time.sleep(1)

            f_out.write(']')

    except Exception as e:
        logger.error(f"Критическая ошибка на Уровне 1: {e}")
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        return

    logger.info(f"Уровень 1 завершен. Очищено и сохранено {processed_count} записей в: {output_file_path}")
    logger.info("--- КОНЕЦ УРОВНЯ 1 ---")


def filter_and_deduplicate_data(input_file_path: str, output_file_path: str, context_query: str):
    logger.info("--- НАЧАЛО УРОВНЯ 2: Потоковая фильтрация и дедупликация ---")
    logger.info(f"Контекст для фильтрации: '{context_query}'")

    seen_contents = set()
    relevant_count = 0

    try:
        with open(output_file_path, 'w', encoding='utf-8') as f_out:
            f_out.write('[')
            is_first_item = True

            for item in stream_json_objects(input_file_path):
                content = item.get('cleaned_text', '')
                if not content:
                    continue

                if content in seen_contents:
                    logger.info(f"ДУБЛИКАТ: {item.get('url')} пропущен.")
                    continue

                prompt = PROMPT_2_TEMPLATE.format(context_query=context_query, text_content=content)
                relevance_response = _call_gemini_api(prompt, GEMINI_MODEL_2)

                if relevance_response and 'да' in relevance_response.lower():
                    logger.info(f"СООТВЕТСТВИЕ: {item.get('url')} добавлен.")
                    seen_contents.add(content)

                    if not is_first_item:
                        f_out.write(',')
                    json.dump(item, f_out, ensure_ascii=False, indent=2, default=json_serializer)
                    is_first_item = False
                    relevant_count += 1
                else:
                    logger.info(f"НЕСООТВЕТСТВИЕ: {item.get('url')} отфильтрован (Ответ: '{relevance_response}').")

                time.sleep(1)

            f_out.write(']')

    except Exception as e:
        logger.error(f"Критическая ошибка на Уровне 2: {e}")
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        return

    logger.info(f"Уровень 2 завершен. Отфильтровано {relevant_count} уникальных релевантных статей.")
    logger.info(f"Итоговые данные сохранены в: {output_file_path}")
    logger.info("--- КОНЕЦ УРОВНЯ 2 ---")


def summarize_final_data(input_file_path: str, output_file_path: str, context_query: str, chunk_size: int = 10):
    logger.info("--- НАЧАЛО УРОВНЯ 3: Создание итоговой сводки (Map-Reduce) ---")
    logger.info(f"Фаза MAP: создание промежуточных сводок по чанкам размером {chunk_size}...")
    intermediate_summaries = []
    chunk = []

    try:
        for item in stream_json_objects(input_file_path):
            chunk.append(item)
            if len(chunk) >= chunk_size:
                chunk_texts = "\n\n---\n\n".join([c.get('cleaned_text', '') for c in chunk])
                prompt = PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE.format(context_query=context_query, chunk_texts=chunk_texts)

                logger.info(f"Обработка чанка из {len(chunk)} статей...")
                summary = _call_gemini_api(prompt, GEMINI_MODEL_1)
                if summary:
                    intermediate_summaries.append(summary)

                chunk = []
                time.sleep(2)

        if chunk:
            chunk_texts = "\n\n---\n\n".join([c.get('cleaned_text', '') for c in chunk])
            prompt = PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE.format(context_query=context_query, chunk_texts=chunk_texts)
            logger.info(f"Обработка финального чанка из {len(chunk)} статей...")
            summary = _call_gemini_api(prompt, GEMINI_MODEL_1)
            if summary:
                intermediate_summaries.append(summary)

    except Exception as e:
        logger.error(f"Ошибка на фазе MAP: {e}")
        return

    if not intermediate_summaries:
        logger.warning("Не удалось создать ни одной промежуточной сводки. Пропускаю Уровень 3.")
        return

    logger.info(f"Фаза MAP завершена. Создано {len(intermediate_summaries)} промежуточных сводок.")
    logger.info("Фаза REDUCE: создание финальной сводки из промежуточных...")
    combined_summaries = "\n\n===\n\n".join(intermediate_summaries)
    final_prompt = PROMPT_3_FINAL_SUMMARY_TEMPLATE.format(context_query=context_query,
                                                          combined_summaries=combined_summaries)

    final_summary = _call_gemini_api(final_prompt, GEMINI_MODEL_3)

    if not final_summary:
        logger.error("Не удалось сгенерировать финальную сводку.")
        return

    try:
        with open(output_file_path, 'w', encoding='utf-8') as f:
            f.write(final_summary)
        logger.info(f"Уровень 3 завершен. Финальная сводка сохранена в: {output_file_path}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении файла {output_file_path}: {e}")
    logger.info("--- КОНЕЦ УРОВНЯ 3 ---")


def run_gemini_processing_pipeline(raw_json_file_path: str, context_query: str, processed_data_dir: str):
    logger.info(f"--- Запуск пайплайна обработки Gemini с контекстом: '{context_query}' ---")

    if not os.path.exists(processed_data_dir):
        os.makedirs(processed_data_dir)
        logger.info(f"Создана директория для сохранения данных: {processed_data_dir}")

    if not os.path.exists(raw_json_file_path):
        logger.error(f"Входной файл не найден: {raw_json_file_path}. Пайплайн остановлен.")
        return None

    base_name = os.path.splitext(os.path.basename(raw_json_file_path))[0]

    level_1_output_file = os.path.join(processed_data_dir, f"{base_name}_level_1_cleaned.json")
    level_2_output_file = os.path.join(processed_data_dir, f"{base_name}_level_2_filtered.json")
    level_3_output_file = os.path.join(processed_data_dir, f"{base_name}_level_3_summary.txt")

    clean_raw_data(input_file_path=raw_json_file_path, output_file_path=level_1_output_file)
    if not os.path.exists(level_1_output_file):
        logger.error("Уровень 1 не создал выходной файл. Пайплайн прерван.")
        return None

    filter_and_deduplicate_data(
        input_file_path=level_1_output_file,
        output_file_path=level_2_output_file,
        context_query=context_query
    )
    if not os.path.exists(level_2_output_file):
        logger.error("Уровень 2 не создал выходной файл. Пайплайн прерван.")
        return None

    summarize_final_data(
        input_file_path=level_2_output_file,
        output_file_path=level_3_output_file,
        context_query=context_query
    )

    logger.info("Все этапы обработки завершены.")
    logger.info(f"--- Пайплайн обработки Gemini завершен для контекста: '{context_query}' ---")
    return level_3_output_file
