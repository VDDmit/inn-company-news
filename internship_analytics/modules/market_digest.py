from typing import Optional, Sequence

from internship_analytics.conf import DOMAIN_WEIGHTS, MARKET_NEWS_OUTPUT_DIR, PAGES_TO_SEARCH_MARKET
from .config.logger_config import get_logger
from .gemini_3_factor_process_data import run_gemini_processing_pipeline
from .news import run_full_search_and_parse
from .request_to_gemini_api import call_to_gemini_api

logger = get_logger("market_digest")

PROMPT_MARKET_DIGEST_NEWS = r"""
You are a senior risk analyst. Operate in STRICT extractive mode: use ONLY the provided company summary.  
Do NOT invent facts.

=== OUTPUT LANGUAGE ===
- Russian only.

=== GOAL ===
Generate ONE short search query (6–12 words) in Russian to find the latest news about:  
- the company’s market situation,  
- competitors,  
- trends,  
- regulatory changes.  

=== RULES ===
- Return EXACTLY one line, without quotes or explanations.  
- Must include year: “2024” or “2025”.  
- If the city is mentioned in the summary → include it. If not → skip.  
- Do NOT add facts not present in the summary.  

At the end include a Markdown table of sources:
Дата| Источник (домен) | URL | Вес (w) | Роль (подтверждение/уточнение/конфликт) | Кратко какие данные использованы |
|---|---|---|---|---|---|

=== INPUT ===
Company summary: {company_summary}
"""


def _sanitize_query_line(q: str) -> str:
    q = (q or "").replace("\n", " ").replace("\r", " ").strip()
    q = q.strip('"\''"“”‘’").strip()
    q = " ".join(q.split())
    return q


def generate_market_query_one(
        company_summary_text: str,
        *,
        model: str = "models/gemini-2.5-flash-lite",
        max_output_tokens: int = 60,
) -> str:
    if not company_summary_text or not company_summary_text.strip():
        return ""

    prompt = PROMPT_MARKET_DIGEST_NEWS.format(company_summary=company_summary_text)
    raw = call_to_gemini_api(
        prompt,
        model=model,
        max_output_tokens=max_output_tokens,
        temperature=0.4,
        top_p=0.9,
    )
    query = _sanitize_query_line(raw)

    word_count = len(query.split())
    if not query or word_count < 5 or word_count > 14:
        query = " ".join(query.split()[:12]).strip()
    return query


def get_market_digest(
        company_summary_text: str,
        *,
        domains: Optional[Sequence[str]] = None,
        num_pages: Optional[int] = None,
) -> str:
    """
    Полный цикл: генерирует короткий запрос -> собирает новости -> делает итоговую сводку по рынку.
    Возвращает путь к финальной сводке (txt) или пустую строку при неудаче.
    """
    query = generate_market_query_one(company_summary_text)
    if not query:
        logger.error("Не удалось сгенерировать поисковый запрос.")
        return ""

    logger.info(f"Сгенерирован поисковый запрос для рынка: {query}")

    # домены поиска
    if domains is None:
        domains = list(DOMAIN_WEIGHTS.keys())

    # число страниц поиска
    if num_pages is None:
        try:
            num_pages = PAGES_TO_SEARCH_MARKET
        except NameError:
            num_pages = 3

    raw_json_path = run_full_search_and_parse(
        user_search_query=query,
        domains_to_search=list(domains),
        num_pages=num_pages,
        path_to_output=MARKET_NEWS_OUTPUT_DIR,
    )

    if not raw_json_path:
        logger.warning("Не удалось собрать новости для рыночного дайджеста.")
        return ""

    final_summary_path = run_gemini_processing_pipeline(
        raw_json_file_path=raw_json_path,
        context_query=query,
        processed_data_dir=MARKET_NEWS_OUTPUT_DIR,
    )

    if final_summary_path:
        logger.info(f"Финальное саммари по рынку сохранено: {final_summary_path}")
        return final_summary_path

    return ""
