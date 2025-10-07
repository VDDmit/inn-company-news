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

PROMPT_1 = r"""
You are a senior risk analyst acting as a STRICT extractor/cleaner. Use ONLY the provided raw text.
Do NOT invent, infer, or add facts. If nothing relevant remains, output exactly: "нет данных".

=== OUTPUT LANGUAGE ===
- Russian only.

=== INPUT METADATA ===
- Source domain: {source_domain}
- Source weight (0–1): {source_weight}
- URL: {url}

=== GOAL ===
Return a single coherent Russian fragment that preserves only meaningful content directly related to the main subject
of the document (company/object/event).

=== CLEANING RULES (apply in order) ===
1) Read the entire text carefully.
2) Remove all "noise", including:
   - Navigation/site chrome (e.g., "Описание", "Похожие компании", "Контакты", "Арбитражные дела", menus, breadcrumbs, headers/footers).
   - Lists of other companies/organizations unless integral to the narrative about the main subject.
   - Repetitive registry/boilerplate blocks (e.g., long repeating lines with ИНН/ОГРН/уставный капитал), generic disclaimers (e.g., FNS reporting delays).
   - Ads, CTAs, widgets, pagination, cookie notices, filters, tag clouds, unrelated links.
3) Keep only sentences/paragraphs that directly describe the main subject (company/object/event).
4) Do NOT delete unique identifiers that help unambiguous identification (e.g., ИНН/ОГРН, registration dates, address) when they appear once or are essential.
5) Borderline fragments: if usefulness is uncertain, PREFER KEEP when {source_weight} ≥ 0.90 **and** the fragment can support later analysis; otherwise DROP.
6) Stitch the remaining content into ONE smooth, readable fragment:
   - Preserve original meaning and factual wording; no new facts.
   - Remove duplicates; collapse near-duplicates.
   - Normalize whitespace and punctuation; keep original numbers and dates as-is (do not reformat).
   - Do not add headings or commentary.

=== VALIDATION BEFORE RETURN ===
- Return EXACTLY one block of cleaned text in Russian.
- No headings, no metadata, no mentions of domain/URL/weight.
- No quotes or explanations; avoid list markers unless essential to readability of retained content.
- If no relevant content remains → output exactly: "нет данных".

=== RAW TEXT TO CLEAN ===
{raw_text}
"""

PROMPT_2_TEMPLATE = r"""
You are a senior risk analyst. Operate in STRICT extractive mode: use ONLY the provided text and metadata.
Do NOT invent or infer facts.

=== OUTPUT LANGUAGE ===
- Russian only.

=== TASK ===
Answer with ONE word — either "да" or "нет" — indicating whether the text contains information relevant to the query "{context_query}".

=== SPECIAL RULE (market/analytics) ===
If the query asks for market state or analytics (e.g., рынок, конкуренты, тренды, регуляции), and the text contains such information relevant to "{context_query}", respond "да".

=== RELEVANCE CRITERIA ===
A) Direct relevance → "да":
   - Direct mention of "{context_query}" or its official/legal/brand names (including transliterations and common abbreviations).
   - Match on unique identifiers (ИНН/ОГРН/address/founders/beneficial owners), or explicit mentions of projects/divisions/brands owned by "{context_query}".
B) Indirect relevance →
   - If the connection is FACTUAL (partnerships, lawsuits, shared address/founder, group membership, participation in the same project) AND {source_weight} ≥ 0.90 → "да".
   - If ONLY weak/tenuous overlaps (no explicit link) AND {source_weight} < 0.90 → "нет".
C) No connection at all → "нет".

=== TIME & SOURCE HANDLING ===
- Use {date} only for context; classify relevance by content (do not reject solely due to recency).
- Do not apply any weighting beyond rule B. No speculation.

=== VALIDATION BEFORE RETURN ===
- Output EXACTLY one lowercase word: "да" or "нет" (no quotes, no punctuation, no extra spaces).
- No explanations or comments.

=== METADATA ===
- Source domain: {source_domain}
- Source weight (0–1): {source_weight}
- URL: {url}
- Date: {date}

=== TEXT TO ANALYZE ===
{text_content}
"""

PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE = r"""
You are a non-creative data processor. Your function is to perform 100% extractive processing.
You are strictly forbidden from interpreting, inferring, or adding any information not explicitly present in the sources. ZERO creativity is allowed.

=== CORE DIRECTIVE ===
- Operate in an EXTREMELY STRICT extractive mode. Every single word in your output MUST be directly traceable to the provided sources.

=== OUTPUT LANGUAGE ===
- Russian only.

=== CONTEXT QUERY ===
- Process facts related to: "{context_query}"

=== INPUT FORMAT ===
Each source is provided as:
[SRC:{{source_domain}} | W:{{source_weight}} | URL:{{url}} | DATE:{{date}}]
<Text>

=== MANDATORY TASK SEQUENCE ===
1) SCAN sources for facts, figures, and events that directly match the context query "{context_query}".
2) EXTRACT VERBATIM or with minimal, structurally necessary paraphrasing. Discard all filler, opinions, and promotional language.
3) CONSOLIDATE identical or near-identical facts from different sources into a single thesis. When merging, use the most direct and data-rich phrasing available in the sources.
4) ATTACH METADATA to EACH thesis without exception:
   - Evidence list: A list of supporting domains with their weights. Format: [evidence: domain1(w=0.95); domain2(w=1.00)]
   - Support score: Calculate as min(1.00, sum of unique source weights). Format: [support: 0.xx]
5) HANDLE CONFLICTS: If sources provide conflicting versions of a fact:
   - State the conflicting versions as separate theses.
   - The thesis with higher aggregated support is considered the primary version.

=== OUTPUT FORMAT ===
- The output MUST ONLY be a bullet-point list of theses.
- Each thesis MUST end with two tags: [evidence: ...] [support: 0.xx]
- Example:
  • Компания X получила кредит в размере 500 млн рублей. [evidence: rbc.ru(w=0.95); vedomosti.ru(w=1.00)] [support: 1.00]

=== CRITICAL VALIDATION BEFORE RESPONSE ===
- Is the output free of ANY introductory or concluding text? YES.
- Is every fact directly quoted or minimally paraphrased from a source? YES.
- Does every bullet point have BOTH the [evidence: ...] and [support: ...] tags? YES.
- Is every URL from the input sources correctly associated with its data? YES.

=== SOURCES TO PROCESS ===
{chunk_texts}
"""

PROMPT_3_FINAL_SUMMARY_TEMPLATE = r"""
You are an automated report assembly system. Your only function is to assemble a final report from pre-processed data chunks.
You MUST NOT perform any creative writing, interpretation, or analysis. Your output is based SOLELY on the provided intermediate summaries.

=== CORE DIRECTIVE ===
- Operate in a STRICTLY mechanical, assembly-line mode.
- It is forbidden to invent, infer, or embellish any information. Every statement must be justified by the input.

=== OUTPUT LANGUAGE ===
- Russian only.

=== CONTEXT QUERY ===
- Final report for topic: "{context_query}"

=== MANDATORY ASSEMBLY RULES ===
1) THESIS CONSOLIDATION:
   - Systematically scan all theses from the intermediate reports.
   - Merge 100% identical theses. For closely related theses, combine them into a single, data-driven statement, strictly using the phrasing from the inputs.
2. AGGREGATED SUPPORT CALCULATION:
   - For each consolidated fact, calculate the final aggregated_support = min(1.00, sum of all unique source weights confirming the fact), rounded to 2 decimals.
3. CONFLICT HANDLING:
   - If conflicting theses exist, explicitly mark them. Present the version with the highest aggregated_support as the main fact.
4. REPORT STRUCTURE (Apply mechanically):
   - **Ключевые выводы (5–10 bullet points):** Select the most significant, high-support theses. Each must end with [support: 0.xx].
   - **Детализация по блокам:** Categorize all remaining theses.
     - **CRITICAL RULE FOR CATEGORIZATION:** A thesis can only be placed in a specific section (e.g., "Риски и возможности") if the source text for that thesis explicitly uses keywords related to that category (e.g., 'риск', 'угроза', 'проблема', 'возможность', 'перспектива'). If no such keyword is present, place the fact in a general block like 'Операционная деятельность' or 'Финансовые показатели'. DO NOT INFER THE CATEGORY.
   - **Таблица источников:** Generate a Markdown table at the end of the report.

=== SOURCE TABLE GENERATION (Strict procedure) ===
For the final source table, you MUST:
a) List every unique source URL present in the evidence tags of all processed theses.
b) For the "Роль" column, write 'подтверждение' if the source supported a fact, 'уточнение' if it added a detail to an existing fact, or 'конфликт' if it supported a fact involved in a noted conflict.
c) For the "Кратко какие данные использованы" column, you MUST copy-paste the exact final thesis (or theses) that this source's evidence supports. DO NOT SUMMARIZE.

| Дата| Источник (домен) | URL | Вес (w) | Роль (подтверждение/уточнение/конфликт) | Кратко какие данные использованы |
|---|---|---|---|---|---|

=== FORBIDDEN ACTIONS ===
- Writing introductions, conclusions, or transitional sentences.
- Using synonyms not present in the source text.
- Inferring cause and effect unless explicitly stated.
- Adding any commentary, opinion, or "analyst insight".
- Generating a report if the input data is empty.

=== INPUT: INTERMEDIATE SUMMARIES ===
---
{combined_summaries}
---
"""
# GEMINI_MODEL_1 = 'models/gemini-2.5-flash-lite'
# GEMINI_MODEL_2 = 'models/gemini-2.5-flash-lite'
GEMINI_MODEL_1 = 'models/gemini-2.0-flash-lite'
GEMINI_MODEL_2 = 'models/gemini-2.0-flash-lite'
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
                    url=item.get('url', ''),
                    raw_text=content_to_clean,
                )
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
            prompt = PROMPT_3_SUMMARIZE_CHUNK_TEMPLATE.format(
                context_query=context_query,
                chunk_texts=chunk_texts
            )
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
