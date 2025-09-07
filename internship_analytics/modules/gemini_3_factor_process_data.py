import json
import os
import time
from decimal import Decimal

import ijson
from dotenv import load_dotenv

from .config.logger_config import get_logger
from .request_to_gemini_api import call_to_gemini_api

load_dotenv()
logger = get_logger("gemini_data_processor")

PROMPT_1 = """
Ты — редактор-экстрактор. Твоя задача — очистить предоставленный сырой текст, извлекая из него только связный и осмысленный контент, относящийся к основной теме документа.

Метаданные источника:
- Домен: {source_domain}
- Вес источника (0–1): {source_weight}
- URL: {url}

Инструкции по очистке:
1) Внимательно проанализируй весь текст.
2) Удали весь "мусор":
   - Навигационные элементы ('Описание', 'Похожие компании', 'Контакты', 'Арбитражные дела' и т.п.).
   - Списки других компаний/организаций, если они не являются частью основного повествования.
   - Повторяющиеся блоки реквизитов (ИНН, ОГРН, уставной капитал и т.д.) из длинных однотипных списков.
   - Шаблонные оговорки общего характера (напр. про задержку публикации отчетности ФНС и т.п.).
3) Сохрани только те абзацы и предложения, которые напрямую описывают объект/событие/компанию — главную тему текста.
4) Не удаляй единичные идентификаторы (ИНН/ОГРН, даты регистрации, адрес), если они помогают однозначной идентификации объекта.
5) При сомнении включать ли пограничный фрагмент — отдай приоритет сохранению, если вес источника ≥ 0.90 и фрагмент может быть полезен для последующего анализа.
6) Объедини оставшийся контент в единый, гладкий и читаемый фрагмент без добавления новой информации.

Важно: верни ТОЛЬКО очищенный связный текст без заголовков, без метаданных и без упоминания веса.
Сырой текст для очистки:
---
"""

PROMPT_2_TEMPLATE = """
Определи, содержит ли предоставленный текст информацию, связанную с запросом '{context_query}'.
Если запрос связан с состоянием рынка или какой-то аналитикой - оставь, так же если в запросе просится предоставить что то на тему (рынок, конкуренты, тренды, регуляции) оставляй это и пиши одним словом 'да'

Метаданные источника:
- Домен: {source_domain}
- Вес источника (0–1): {source_weight}
- URL: {url}
- Дата: {date}

Критерии релевантности (с учётом веса):
A) Прямая релевантность → 'да':
   - Прямое упоминание '{context_query}' или его официальных/юридических/брендовых наименований (включая транслитерации и распространённые сокращения),
   - Совпадение уникальных идентификаторов (ИНН/ОГРН/адрес/учредители/бенефициары), явные упоминания проектов/подразделений/брендов, принадлежащих '{context_query}'.

B) Косвенная релевантность (контекст/связи) → 
   - Если связь подтверждается фактами (партнёрства, судебные дела, один адрес/учредитель, принадлежность к группе, участие в одном проекте) и вес источника ≥ 0.90, ответ 'да'.
   - Если присутствуют ТОЛЬКО слабые/намёчные совпадения (без явной связи) и вес < 0.90, ответ 'нет'.

C) Полное отсутствие связи → 'нет'.

Ответь ОДНИМ СЛОВОМ на русском: 'да' или 'нет'.

Текст для анализа:
---
{text_content}
---
"""

PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE = """
Ты — аналитик. Сформируй краткую, ёмкую выжимку по теме '{context_query}' из набора источников.
Каждый источник передан в формате:
[SRC:{{source_domain}} | W:{{source_weight}} | URL:{{url}} | DATE:{{date}}]
<Текст>

Задача:
1) Выдели ключевые факты/события/цифры, убери воду.
2) Сгруппируй дубли, объединяя формулировки.
3) Для КАЖДОГО тезиса укажи метаданные поддержки:
   - список доменов-источников и их веса в формате: [evidence: domain1(w=0.95); domain2(w=1.00)]
   - вычисли поддерживающий вес тезиса: support = min(1.00, сумма весов уникальных источников, округли до 2 знаков).
4) Если по одному факту есть конфликтующие версии — кратко отметь конфликт и отдай приоритет версии с бо́льшим суммарным support.

Выведи результат в виде маркированного списка тезисов. Каждый тезис оканчивай блоком:
[evidence: ...] [support: 0.xx]

Источники (блоки) для анализа:
---
{chunk_texts}
---
"""

PROMPT_3_FINAL_SUMMARY_TEMPLATE = """
Ты — экспертный аналитик. На основе всех промежуточных отчётов подготовь развернутую итоговую сводку по теме '{context_query}' с учётом весов источников.

Инструкции:
1) Синтезируй тезисы, объедини дубли, нормализуй формулировки.
2) Для каждого итогового факта рассчитай агрегированный поддерживающий вес:
   aggregated_support = min(1.00, сумма уникальных весов источников, подтвердивших факт). Округляй до 2 знаков.
3) Конфликты:
   - Явно отмечай расхождения.
   - Выбирай основную версию по бо́льшему aggregated_support; при близких значениях используй дополнительный критерий — более свежая дата.
4) Представь структуру отчёта:
   - Ключевые выводы (5–10 пунктов) — укажи для каждого [support: 0.xx].
   - Детализация по блокам (события, финансовое/правовое, партнёры/контрагенты, география/активы и т.п.) — с краткими фактами и их support.
   - Риски и возможности — с кратким обоснованием и support.
5) В конце добавь таблицу источников:

| Источник (домен) | URL | Вес (w) | Роль (подтверждение/уточнение/конфликт) | Кратко какие данные использованы |
|---|---|---|---|---|

6) Пиши чётко, по делу, аналитическим стилем. Язык — русский.

Промежуточные отчёты (с тезисами, evidence и support):
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
                prompt = PROMPT_1.format(
                    source_domain=item.get('source', ''),
                    source_weight=item.get('weight', 0),
                    url=item.get('url', '')
                ) + "\n" + content_to_clean
                cleaned_text = call_to_gemini_api(prompt, GEMINI_MODEL_1)

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

                prompt = PROMPT_2_TEMPLATE.format(
                    context_query=context_query,
                    text_content=content,
                    source_domain=item.get('source', ''),
                    source_weight=item.get('weight', 0),
                    url=item.get('url', ''),
                    date=item.get('date', '')
                )
                relevance_response = call_to_gemini_api(prompt, GEMINI_MODEL_2)

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
                chunk_texts = "\n\n---\n\n".join([
                    "[SRC:{src} | W:{w} | URL:{u} | DATE:{d}]\n{txt}".format(
                        src=c.get('source', ''),
                        w=c.get('weight', 0),
                        u=c.get('url', ''),
                        d=c.get('date', ''),
                        txt=c.get('cleaned_text', '')
                    ) for c in chunk
                ])
                prompt = PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE.format(context_query=context_query, chunk_texts=chunk_texts)

                logger.info(f"Обработка чанка из {len(chunk)} статей...")
                summary = call_to_gemini_api(prompt, GEMINI_MODEL_1)
                if summary:
                    intermediate_summaries.append(summary)

                chunk = []
                time.sleep(2)

        if chunk:
            chunk_texts = "\n\n---\n\n".join([
                "[SRC:{src} | W:{w} | URL:{u} | DATE:{d}]\n{txt}".format(
                    src=c.get('source', ''),
                    w=c.get('weight', 0),
                    u=c.get('url', ''),
                    d=c.get('date', ''),
                    txt=c.get('cleaned_text', '')
                ) for c in chunk
            ])
            prompt = PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE.format(context_query=context_query, chunk_texts=chunk_texts)
            logger.info(f"Обработка финального чанка из {len(chunk)} статей...")
            summary = call_to_gemini_api(prompt, GEMINI_MODEL_1)
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

    final_summary = call_to_gemini_api(final_prompt, GEMINI_MODEL_3)

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
