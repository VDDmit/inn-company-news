import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BASE_OUTPUT_DIR = os.path.join(BASE_DIR, "output")
BASE_INPUT_DIR = os.path.join(BASE_DIR, "input")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DIR = os.path.join(BASE_OUTPUT_DIR, f"{TIMESTAMP}")

EGRUL_PDF_DIR = os.path.join(RUN_DIR, "egrul_pdf")
EGRUL_JSON_DIR = os.path.join(RUN_DIR, "egrul_json")
PROCESSED_DATA_DIR = os.path.join(RUN_DIR, "processed_data")

COMPANY_INFO_CSV = os.path.join(BASE_INPUT_DIR, "csv", "Output_updated.csv")

KOMMERSANT_JSON_DIR = os.path.join(RUN_DIR, "kommersant_news")

PYDOLL_SCRAPED_DATA_DIR = os.path.join(RUN_DIR, "scraped_data")

FINAL_REPORT_FILE = os.path.join(RUN_DIR, "final_summary_report.md")

COMPANY_NEWS_OUTPUT_DIR = os.path.join(RUN_DIR, "search_api_news_company")
SEO_NEWS_OUTPUT_DIR = os.path.join(RUN_DIR, "search_api_news_seo")
MARKET_NEWS_OUTPUT_DIR = os.path.join(RUN_DIR, "search_api_news_market")

FINAL_REPORTS_OUTPUT_DIR = os.path.join(RUN_DIR, "summaries")

DOMAIN_WEIGHTS = {
    "interfax.ru": 1.00,
    "rbc.ru": 0.95,
    "companies.rbc.ru": 0.95,
    "marketing.rbc.ru": 0.95,
    "www.rbc.ru": 0.95,
    "kommersant.ru": 0.95,
    "vedomosti.ru": 0.90,
    "rg.ru": 0.80,
    "forbes.ru": 0.80,
    "tass.ru": 1.00,
    "ria.ru": 0.90
}

PAGES_TO_SEARCH_COMPANY = 1
PAGES_TO_SEARCH_SEO = 1
PAGES_TO_SEARCH_MARKET = 1

for d in [
    EGRUL_PDF_DIR,
    EGRUL_JSON_DIR,
    PROCESSED_DATA_DIR,
    KOMMERSANT_JSON_DIR,
    PYDOLL_SCRAPED_DATA_DIR,
    COMPANY_NEWS_OUTPUT_DIR,
    SEO_NEWS_OUTPUT_DIR,
    MARKET_NEWS_OUTPUT_DIR
]:
    os.makedirs(d, exist_ok=True)

FINAL_REPORT_PROMPT_TEMPLATE_V2 = r"""
You are a senior risk analyst. Operate in STRICT extractive mode:
use ONLY the structured inputs provided. Do NOT invent facts.
If any field is missing, output “нет данных”.

Goal: Produce a one-page Markdown report in Russian that EXACTLY follows
the structure and headings of the sample “отчет_v2”. Keep it concise and factual.

=== OUTPUT LANGUAGE ===
- Russian only.

=== TIME WINDOW ===
- 365 days ending on {generation_date} (inclusive).
- Use DD.MM.YYYY for dates in evidence.

=== SECTION ORDER AND EXACT HEADINGS (no extra sections) ===
1) Title block (two lines)
   Line 1: Одностраничный отчёт — {{legal_name}}
   Line 2: (бренд «{{brand_name}}»)

3) Корзина / окно анализа (two lines)
   Line 3: Корзина: {{verdict}}            # one of: Avoid / Monitor / Accept
   Line 4: Окно анализа: 365 дней · Сгенерировано: {generation_date}

5) Top-line
   Heading: Top-line
   Content: 2–3 short sentences. Start with the verdict word if helpful (e.g., “Avoid:”).
   Base ONLY on the supplied inputs ({{top_line_summary}}; cite key outlets inline in parentheses).

6) 2 причины (MECE)
   Heading: 2 причины (MECE)

   # Sub-block MUST appear exactly as below (three lines):
   Оценка индустрии (важность бренда)
   Индустрия: {{industry_line}}
   Важность бренда для потребителей: {{brand_importance_score}}/10 — {{brand_importance_rationale}}

   Then a numbered list of exactly two items:
     1. {{reason_1_title}}. {{reason_1_description}} (Источники: {{reason_1_sources}})
     2. {{reason_2_title}}. {{reason_2_description}} (Источники: {{reason_2_sources}})

   Then one line with a deterministic binary decision (NO “Зависит” allowed):
     Инвестировал бы? {{invest_decision_optional_stage}} — {{invest_rationale}}

7) Risk Score — коротко, «по-бизнесу»
   Heading: Risk Score — коротко, «по-бизнесу»
   Что это: единый индикатор новостного/регуляторного риска за год. Смотрим на тяжесть событий, свежесть и качество источников.
   Итог по компании: {{risk_score}} — {{risk_score_business_interp}} ({{event_age_phrase}}).
   Как читать решение: у нас действует правило «событие класса максимальной тяжести ⇒ Avoid» — даже если итоговый балл умеренный. Поэтому статус {{verdict}} сохраняется, пока {{resolution_condition}}.

   Ключевые метрики (12 мес)
   Heading: Ключевые метрики (12 мес), которые ты можешь сам вывести исходя только из данных которые я тебе дал (нет данных старайся не писать) 
   Формат таблицы (строго такой):
   
   | Метрика | Значение | Примечание |
   |---|---|---|
   | Упоминаний | {{mentions_trend}} | {{mentions_note}} |
   | Медианный тон | {{median_tone}} | {{tone_note}} |
   | Тяжёлые события | {{hard_events_count}} | {{hard_events_note}} |
   | Risk Score (итог) | {{risk_score}} | {{risk_score_note}} |

9) Доказательства (источники)
   Heading: Доказательства (источники)
   | Дата | Заголовок/суть | Источник |
   {{evidence_items}}

10) Three explanatory paragraphs:
   - Что это: ...
   - Итог по компании: {{risk_score}} ...
   - Как читать решение: ...
"""

PROMPT_MARKET_DIGEST_NEWS_V2 = r"""
You are a senior risk analyst. Operate in STRICT extractive mode: use ONLY the provided company summary.  
Do NOT invent facts.

=== OUTPUT LANGUAGE ===
- Russian only.

=== TASK ===
Generate ONE short natural-language browser search query in Russian to find the latest news about:
- the company’s market situation,
- its competitors,
- industry trends,
- regulatory changes.

=== REQUIREMENTS ===
- Output EXACTLY one query string (no explanations, no quotes).
- Query length: 6–12 words.
- Preferably include: industry/market, competitors/players, geography (city or region if present), key events (e.g., кризис, санкции, господдержка, тендеры).
- Must include year: "2024" or "2025" for freshness.
- Do NOT use operators (e.g., site:), quotes, or Boolean logic.
- If city is missing in the summary → skip it.
- Do NOT invent facts — base only on the summary.

=== INPUT ===
Company summary:
{company_summary}
"""
