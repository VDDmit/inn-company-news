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
COMPANY_SEARCH_MINI_DIGEST_OUTPUT_DIR = os.path.join(RUN_DIR, "search_api_mini_digest")
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
    "pravdaurfo.ru": 0.90,
    "forbes.ru": 0.80,
    "tass.ru": 1.00,
    "ria.ru": 0.90,
    "1prime.ru": 0.80,
    "cbr.ru": 1.00,
    "fas.gov.ru": 1.00,
    "minjust.gov.ru": 1.00,
    "e-disclosure.ru": 1.00,
    "pravo.gov.ru": 1.00,
    "nalog.gov.ru": 1.00,
    "sudrf.ru": 1.00,
    "kad.arbitr.ru": 1.00,
    "moex.com": 1.00,
    "spbexchange.ru": 1.00,
    "iz.ru": 0.65,
    "interfax-russia.ru": 0.85,
    "bfm.ru": 0.80,
    "banki.ru": 0.75,
    "cnews.ru": 0.75,
    "lenta.ru": 0.75,
    "gazeta.ru": 0.65,
    "thebell.io": 0.65,
    "finam.ru": 0.65,
    "rzd-partner.ru": 0.60,
    "portnews.ru": 0.60,
    "investfunds.ru": 0.60,
    "rb.ru": 0.70,
    "secretmag.ru": 0.70,
    "fontanka.ru": 0.70,
    "e1.ru": 0.60,
    "66.ru": 0.55,
    "74.ru": 0.55,
    "abireg.ru": 0.55,
    "profile.ru": 0.55,
    "theins.ru": 0.55,
    "ngs.ru": 0.45,
    "smart-lab.ru": 0.45,
    "vc.ru": 0.50,
    "mk.ru": 0.35,
    "rosbalt.ru": 0.35
}

PAGES_TO_SEARCH_COMPANY = 3
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

# FINAL_REPORT_PROMPT_TEMPLATE_V2 = r"""
# You are a senior risk analyst. Operate in STRICT extractive mode:
# use ONLY the structured inputs provided. Do NOT invent facts.
# First, carefully analyze ALL provided data. Then, select and include
# ONLY the most important and relevant information in the report.
#
# Goal: Produce a one-page Markdown report in Russian that EXACTLY follows
# the structure and headings of the sample “отчет_v2”. Keep it concise and factual.
#
# === OUTPUT LANGUAGE ===
# - Russian only.
#
# === TIME WINDOW ===
# - 365 days ending on {generation_date} (inclusive).
# - Use DD.MM.YYYY for dates in evidence.
#
# === SECTION ORDER AND EXACT HEADINGS (no extra sections) ===
# 1) Title block (two lines)
#    Line 1: Одностраничный отчёт — {{legal_name}}
#    Line 2: (бренд «{{brand_name}}»)
#
# 3) Корзина / окно анализа (two lines)
#    Line 3: Корзина: {{verdict}}            # one of: Avoid / Monitor / Accept
#    Line 4: Окно анализа: 365 дней · Сгенерировано: {generation_date}
#
# 5) Top-line
#    Heading: Top-line
#    Content: 2–3 short sentences. Start with the verdict word if helpful (e.g., “Avoid:”).
#    Base ONLY on the supplied inputs ({{top_line_summary}}; cite key outlets inline in parentheses).
#
# 6) 2 причины (MECE)
#    Heading: 2 причины (MECE)
#
#    # Sub-block MUST appear exactly as below (three lines):
#    Оценка индустрии (важность бренда)
#    Индустрия: {{industry_line}}
#    Важность бренда для потребителей: {{brand_importance_score}}/10 — {{brand_importance_rationale}}
#
#    Then a numbered list of exactly two items:
#      1. {{reason_1_title}}. {{reason_1_description}} (Источники: {{reason_1_sources}})
#      2. {{reason_2_title}}. {{reason_2_description}} (Источники: {{reason_2_sources}})
#
#    Then one line with a deterministic binary decision (NO “Зависит” allowed):
#      Инвестировал бы? {{invest_decision_optional_stage}} — {{invest_rationale}}
#
# 7) Risk Score — коротко, «по-бизнесу»
#    Heading: Risk Score — коротко, «по-бизнесу»
#    Что это: единый индикатор новостного/регуляторного риска за год. Смотрим на тяжесть событий, свежесть и качество источников.
#    Итог по компании: {{risk_score}} — {{risk_score_business_interp}} ({{event_age_phrase}}).
#    Как читать решение: у нас действует правило «событие класса максимальной тяжести ⇒ Avoid» — даже если итоговый балл умеренный. Поэтому статус {{verdict}} сохраняется, пока {{resolution_condition}}.
#
#    Ключевые метрики (12 мес), которые ты можешь сам вывести исходя только из данных которые я тебе дал (нет данных старайся не писать)
#    Heading: Ключевые метрики (12 мес)
#    Формат таблицы (строго такой) :
#
#    | Метрика | Значение | Примечание |
#    |---|---|---|
#    | Упоминаний | {{mentions_trend}} | {{mentions_note}} |
#    | Медианный тон | {{median_tone}} | {{tone_note}} |
#    | Тяжёлые события | {{hard_events_count}} | {{hard_events_note}} |
#    | Risk Score (итог) | {{risk_score}} | {{risk_score_note}} |
#
# 9) Доказательства (источники)(critical)
#    Heading: Доказательства (источники)
#    | Дата | Заголовок/суть | Источник |
#    {{evidence_items}}
#
# 10) Three explanatory paragraphs:
#    - Что это: ...
#    - Итог по компании: {{risk_score}} ...
#    - Как читать решение: ...
# """
# FINAL_REPORT_PROMPT_TEMPLATE_V2 = r"""
# You are an AI assistant functioning as a data-to-text engine. Your sole purpose is to populate a predefined Markdown template.
#
# **Core Instructions:**
# 1.  **Strictly Extractive Mode:** You MUST operate in a strictly extractive mode. Use ONLY the data provided in the `=== STRUCTURED DATA TO USE ===` (compose it from data from SOURCE 1 and SOURCE 2 above.) section.
# 2.  **No Invention:** Do NOT invent, infer, summarize, or alter the provided data. Your role is to insert the placeholder values exactly as they are given into the corresponding `{{placeholder}}` in the template.
# 3.  **Exact Structure:** The final output MUST be a single Markdown block in Russian that EXACTLY follows the structure, headings, and formatting of the provided `=== RUSSIAN MARKDOWN TEMPLATE ===`. Do not add or remove any sections, lines, or formatting.
# 4.  **Date Format:** All dates in the report must use the `DD.MM.YYYY` format. The report's time window is 365 days ending on the `generation_date`.
# 5.  "Итог по компании:" you can write the report yourself based on the data you have and both conclusions, but there should not be anything extraneous.
#
# ---
#
# ### === STRUCTURED DATA TO USE   take this data from the SOURCE 1 and SOURCE 2 above===
#
# *   **generation_date:** {generation_date}
# *   **legal_name:** `{{legal_name}}`
# *   **brand_name:** `{{brand_name}}`
# *   **verdict:** `{{verdict}}`
# *   **top_line_summary:** `{{top_line_summary}}`
# *   **industry_line:** `{{industry_line}}`
# *   **brand_importance_score:** `{{brand_importance_score}}`
# *   **brand_importance_rationale:** `{{brand_importance_rationale}}`
# *   **reason_1_title:** `{{reason_1_title}}`
# *   **reason_1_description:** `{{reason_1_description}}`
# *   **reason_1_sources:** `{{reason_1_sources}}`
# *   **reason_2_title:** `{{reason_2_title}}`
# *   **reason_2_description:** `{{reason_2_description}}`
# *   **reason_2_sources:** `{{reason_2_sources}}`
# *   **invest_decision_optional_stage:** `{{invest_decision_optional_stage}}`
# *   **invest_rationale:** `{{invest_rationale}}`
# *   **risk_score:** `{{risk_score}}`
# *   **risk_score_business_interp:** `{{risk_score_business_interp}}`
# *   **event_age_phrase:** `{{event_age_phrase}}`
# *   **resolution_condition:** `{{resolution_condition}}`
# *   **mentions_trend:** `{{mentions_trend}}`
# *   **mentions_note:** `{{mentions_note}}`
# *   **median_tone:** `{{median_tone}}`
# *   **tone_note:** `{{tone_note}}`
# *   **hard_events_count:** `{{hard_events_count}}`
# *   **hard_events_note:** `{{hard_events_note}}`
# *   **risk_score_note:** `{{risk_score_note}}`
# *   **evidence_items:** `{{evidence_items}}`
#
# ---
#
# ### === RUSSIAN MARKDOWN TEMPLATE (POPULATE THIS) ===
#
# Одностраничный отчёт — {{legal_name}}
# (бренд «{{brand_name}}»)
#
# **Корзина / окно анализа**
#
# Корзина: {{verdict}}
# Окно анализа: 365 дней · Сгенерировано: {generation_date}
#
# **Ключевые метрики (12 мес)**
#
# | Метрика | Значение | Примечание |
# |---|---|---|
# | Упоминаний | {{mentions_trend}} | {{mentions_note}} |
# | Медианный тон | {{median_tone}} | {{tone_note}} |
# | Тяжёлые события | {{hard_events_count}} | {{hard_events_note}} |
# | Risk Score (итог) | {{risk_score}} | {{risk_score_note}} |
#
# **Доказательства (источники)**
#
# | Дата | Заголовок/суть | Источник |
# |---|---|---|
# {{evidence_items}}
#
# **Top-line**
#
# {{top_line_summary}}
#
# **2 причины (MECE)**
#
# Оценка индустрии (важность бренда)
# Индустрия: {{industry_line}}
# Важность бренда для потребителей: {{brand_importance_score}}/10 — {{brand_importance_rationale}}
#
# 1.  {{reason_1_title}}. {{reason_1_description}} (Источники: {{reason_1_sources}})
# 2.  {{reason_2_title}}. {{reason_2_description}} (Источники: {{reason_2_sources}})
#
# Инвестировал бы? {{invest_decision_optional_stage}} — {{invest_rationale}}
#
# **Risk Score — коротко, «по-бизнесу»**
#
# Что это: единый индикатор новостного/регуляторного риска за год. Смотрим на тяжесть событий, свежесть и качество источников.
#
# Итог по компании: {{risk_score}} — {{risk_score_business_interp}} ({{event_age_phrase}}).
#
# Как читать решение: у нас действует правило «событие класса максимальной тяжести ⇒ Avoid» — даже если итоговый балл умеренный. Поэтому статус «{{verdict}}» сохраняется, пока {{resolution_condition}}.
# """

FINAL_REPORT_PROMPT_TEMPLATE_V2 = r"""FIELD POPULATION LOGIC (STRICT)
- For each placeholder below, fill from the “STRUCTURED DATA TO USE”. If a field is not present or is out-of-window or cannot be supported strictly by the provided sources, write “н/д”.
- For “Примечание” cells, if there is no in-window info, add a short reason like “нет данных в переданных источниках за 365 дней”.
- For {{evidence_items}}, construct table rows strictly from SOURCE 1/2 content that falls within the window. If none, leave the table body empty (no rows) OR if a row must exist per system, insert a single row with н/д | н/д | н/д.

QUALITY CHECKS BEFORE YOU OUTPUT
- Does the output contain ONLY the Markdown block? (Yes required.)
- Are all dates in DD.MM.YYYY? (Yes required.)
- Are all missing fields “н/д”? (Yes required.)
- No over-focus on CEO/SEO unless that’s the only provided data, in which case explicitly say the dataset is narrow. (Yes required.)
- No invented numbers, events, or interpretations. (Yes required.)
- Max 10–15 evidence rows, deduped. (Yes required.)
- Pipes escaped in table cells. (Yes required.)

RUSSIAN MARKDOWN TEMPLATE (YOU MUST POPULATE THIS EXACTLY)

Одностраничный отчёт — {{legal_name}}
(бренд «{{brand_name}}»)

Корзина / окно анализа

Корзина: {{verdict}}
Окно анализа: 365 дней · Сгенерировано: {generation_date}

Ключевые метрики (12 мес)

| Метрика | Значение | Примечание |
|---|---|---|
| Упоминаний | {{mentions_trend}} | {{mentions_note}} |
| Медианный тон | {{median_tone}} | {{tone_note}} |
| Тяжёлые события | {{hard_events_count}} | {{hard_events_note}} |
| Risk Score (итог) | {{risk_score}} | {{risk_score_note}} |

Доказательства (источники)

| Дата | Заголовок/суть | Источник |
|---|---|---|
{{evidence_items}}

Top-line

{{top_line_summary}}

2 причины (MECE)

Оценка индустрии (важность бренда)
Индустрия: {{industry_line}}
Важность бренда для потребителей: {{brand_importance_score}}/10 — {{brand_importance_rationale}}

1.  {{reason_1_title}}. {{reason_1_description}} (Источники: {{reason_1_sources}})
2.  {{reason_2_title}}. {{reason_2_description}} (Источники: {{reason_2_sources}})

Инвестировал бы? {{invest_decision_optional_stage}} — {{invest_rationale}}

Risk Score — коротко, «по-бизнесу»

Что это: единый индикатор новостного/регуляторного риска за год. Смотрим на тяжесть событий, свежесть и качество источников.

Итог по компании: {{risk_score}} — {{risk_score_business_interp}} ({{event_age_phrase}}).


Как читать решение: у нас действует правило «событие класса максимальной тяжести ⇒ Avoid» — даже если итоговый балл умеренный. Поэтому статус «{{verdict}}» сохраняется, пока {{resolution_condition}}."""
PROMPT_MARKET_DIGEST_NEWS_V2 = r"""
You are a meticulous and literal-minded AI assistant. Your purpose is to process text based on strict rules.

=== CORE DIRECTIVE: ZERO-INVENTION ===
Your single and absolute rule is to operate in a STRICTLY EXTRACTIVE mode.
- You are FORBIDDEN to invent, infer, or assume any information not explicitly present in the provided `{company_summary}`.
- If a detail (like a competitor's name, a specific market trend, a city, or a regulation) is NOT in the summary, you MUST NOT include it in the output. No exceptions.
- Your knowledge outside of the provided text is disabled. Any deviation is a failure.

=== OUTPUT LANGUAGE ===
- Russian only.

=== TASK ===
Based EXCLUSIVELY on the `{company_summary}`, perform two actions in order:
1.  **Generate a Search Query:** Create a single, concise Russian search query. This query must be assembled ONLY from keywords found within the summary.
2.  **List Sources:** Populate the provided Markdown table with the source information. All URLs from the input MUST be preserved exactly as they are.

=== QUERY REQUIREMENTS ===
- **Output Format:** The query must be a single line of text. No extra explanations, no quotation marks, no leading text like "Query:".
- **Content Construction:** The query must be constructed using keywords EXTRACTED directly from the `{company_summary}`.
- **Keyword Inclusion (Strict Conditional):**
    - Include industry/market terms ONLY IF present in the summary.
    - Include competitor names ONLY IF present in the summary.
    - Include geography (city or region) ONLY IF present in the summary.
    - Include key events (e.g., кризис, санкции) ONLY IF present in the summary.
- **Freshness:** ALWAYS add the current year (e.g., "2024") to the end of the query.
- **Length:** 6–12 words.
- **Prohibited Elements:** Do NOT use search operators (site:, AND, OR), Boolean logic, or quotation marks.

=== SOURCES TABLE ===
At the end of your response, include the following Markdown table. Fill it meticulously using the source data provided alongside the summary. Do not alter URLs.

Дата| Источник (домен) | URL | Вес (w) | Роль (подтверждение/уточнение/конфликт) | Кратко какие данные использованы |
|---|---|---|---|---|---|

=== INPUT ===
Company summary:
{company_summary}
"""
