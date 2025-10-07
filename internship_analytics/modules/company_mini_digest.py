import re
from datetime import datetime
from typing import Optional, Sequence

from internship_analytics.conf import COMPANY_NEWS_OUTPUT_DIR # Предполагаем, что есть такая константа
from .config.logger_config import get_logger
from .gemini_3_factor_process_data import run_gemini_processing_pipeline
from .news import run_full_search_and_parse
from .request_to_gemini_api import call_to_gemini_api

logger = get_logger("company_digest")

PROMPT_COMPANY_DIGEST_NEWS = r"""
You are a senior analyst. Operate in STRICT extractive mode: use ONLY the provided company summary.
Do NOT invent facts.

=== OUTPUT LANGUAGE ===
- Russian only.

=== GOAL ===
Generate ONE short search query (3–7 words, Russian) to find the latest news about the COMPANY itself:
- Sanctions, lawsuits, government checks.
- Financial results (revenue, profit, loss).
- Management changes.
- Scandals or negative press.
- Major projects or contracts.

=== RULES ===
- Return EXACTLY one line, without quotes or extra text.
- Use the company's official name (brand or legal name like OOO "Romashka", PAO "Sberbank").
- Prefer the year “2025”; if not applicable then “2024”.
- If the city/region is present in the summary → include it.
- No punctuation art, no pipes, no “Дата”, no markdown.

=== INPUT ===
Company summary: {company_summary}
"""


def _sanitize_query_line(q: str) -> str:
    """Чистим всё, что ломает поиск или вносит мусор."""
    q = (q or "").replace("\n", " ").replace("\r", " ")
    q = q.replace("«", '"').replace("»", '"').replace("“", '"').replace("”", '"')
    q = q.strip(' "\'')
    q = re.sub(r'\bДата\b', ' ', q, flags=re.IGNORECASE)
    q = q.replace("|", " ")
    q = q.replace("\u00A0", " ")
    q = re.sub(r'[,\.;:]+', ' ', q)
    q = re.sub(r'\s+', ' ', q).strip()
    return q


def _ensure_year(q: str) -> str:
    """Гарантируем наличие 2025 или 2024, предпочтительно 2025."""
    if "2025" in q or "2024" in q:
        return q
    year = "2025" if datetime.now().year >= 2025 else "2024"
    return f"{q} {year}".strip()


def _bound_words(q: str, lo: int = 3, hi: int = 7) -> str:
    """Ограничиваем количество слов под поисковый запрос."""
    words = q.split()
    if len(words) < lo:
        return q
    if len(words) > hi:
        return " ".join(words[:hi])
    return q


def generate_company_query_one(
        company_summary_text: str,
        *,
        model: str = "models/gemini-2.0-flash-lite",
        max_output_tokens: int = 60,
) -> str:
    """Генерирует поисковый запрос для новостей о КОНКРЕТНОЙ компании."""
    if not company_summary_text or not company_summary_text.strip():
        return ""

    prompt = PROMPT_COMPANY_DIGEST_NEWS.format(company_summary=company_summary_text)
    raw = call_to_gemini_api(
        prompt,
        model=model,
        max_output_tokens=max_output_tokens,
        temperature=0.2, # еще более детерминированно
        top_p=0.9,
    )
    query = _sanitize_query_line(raw)
    query = _ensure_year(query)
    query = _bound_words(query, 3, 7)

    query = query.strip(' "\'')

    return query


def get_company_digest(company_summary_text: str) -> str:
    """
    Полный цикл: генерирует запрос о компании -> ищет новости в вебе -> делает итоговую сводку.
    Возвращает путь к финальной сводке (txt) или пустую строку при неудаче.
    """
    query = generate_company_query_one(company_summary_text)
    if not query:
        logger.error("Не удалось сгенерировать поисковый запрос для компании.")
        return ""

    logger.info(f"Сгенерирован поисковый запрос для компании: {query}")

    # Ищем на 1 странице, по всему интернету (список доменов пустой)
    num_pages = 1
    domains = []

    raw_json_path = run_full_search_and_parse(
        user_search_query=query,
        domains_to_search=domains,
        num_pages=num_pages,
        path_to_output=COMPANY_NEWS_OUTPUT_DIR,
    )

    if not raw_json_path:
        logger.warning("Не удалось собрать новости для дайджеста по компании.")
        return ""

    # Используем тот же пайплайн обработки, что и для рыночных новостей
    final_summary_path = run_gemini_processing_pipeline(
        raw_json_file_path=raw_json_path,
        context_query=query,
        processed_data_dir=COMPANY_NEWS_OUTPUT_DIR,
    )

    if final_summary_path:
        logger.info(f"Финальное саммари по компании сохранено: {final_summary_path}")
        return final_summary_path

    return ""
