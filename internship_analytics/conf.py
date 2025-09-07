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
]:
    os.makedirs(d, exist_ok=True)

FINAL_REPORT_PROMPT_TEMPLATE = """
ТЫ — первоклассный риск-аналитик, готовящий одностраничный отчет (one-pager) для инвестиционного комитета.
Твоя задача — синтезировать всю предоставленную информацию в четкий, структурированный и лаконичный отчет в формате Markdown.
Стиль отчета — деловой, объективный, основанный на фактах.

**СТРОГО СЛЕДУЙ ЗАДАННОЙ СТРУКТУРЕ И ФОРМАТИРОВАНИЮ.**

# {company_name}
**Одностраничный отчёт — {legal_name}**
**ИНН {inn} · ОКВЭД {okved_code} ({okved_name}) · Гендиректор {ceo_name}** (по состоянию на дату выписки ЕГРЮЛ); {ceo_news}. (Источники: ЕГРЮЛ, {news_sources_for_header})

| Корзина: {verdict} |
| :--- |
| **Окно анализа:** 365 дней · **Сгенерировано:** {generation_date} |

## Top-line
{top_line_summary} (Источники: {top_line_sources})

### 3 причины (MECE)
1.  **{reason_1_title}**. {reason_1_description} (Источники: {reason_1_sources})
2.  **{reason_2_title}**. {reason_2_description} (Источники: {reason_2_sources})
3.  **{reason_3_title}**. {reason_3_description} (Источники: {reason_3_sources})

### Позитивные сигналы (если есть)
{positive_signals}

### Ключевые метрики (12 мес)
| Метрика | Значение | Примечание |
| :--- | :--- | :--- |
| Упоминаний | {mentions_trend} | {mentions_note} |
| Медианный тон | {median_tone} | {tone_note} |
| Тяжёлые события | {hard_events_count} | {hard_events_note} |
| **Risk Score (итог)** | **{risk_score}** | **{risk_score_note}** |

### Что дальше (мониторинг)
- {monitoring_point_1}
- {monitoring_point_2}
- {monitoring_point_3}

### Доказательства (источники)
| ID | Дата | Заголовок/суть | Источник | Вес домена* |
| :--- | :--- | :--- | :--- | :--- |
{evidence_table}

---
*Веса доменов — ориентиры из политики источников; международные/нишевые не нормированы.
*Примечание: Финансовые показатели из внутренней системы не были включены в данный отчет, сфокусированный на репутационных и операционных рисках.

**ИСХОДНЫЕ ДАННЫЕ ДЛЯ АНАЛИЗА:**

**1. Данные из ЕГРЮЛ:**
{egrul_data}

**2. Ключевые финансовые показатели (из CSV):**
{financial_data}

**3. Сводка новостей и событий (результат предварительного анализа):**
{news_summary}

**4. Список релевантных статей (для таблицы доказательств):**
{relevant_articles_list}

**5. Политика весов доменов:**
{domain_weights_policy}

**ТВОЯ ЗАДАЧА — ЗАПОЛНИТЬ ШАБЛОН ВЫШЕ, ИСПОЛЬЗУЯ ПРЕДОСТАВЛЕННЫЕ ИСХОДНЫЕ ДАННЫЕ.**
- **Вердикт (Корзина):** 'Avoid', 'Monitor', 'Accept'. Выбери на основе тяжести рисков.
- **Top-line:** Краткая (2-3 предложения) выжимка главной причины вердикта.
- **3 причины (MECE):** Разбей Top-line на три взаимоисключающих, исчерпывающих фактора.
- **Ключевые метрики:** Сделай выводы на основе новостной сводки. Например, "всплеск из-за инцидента", "доминируют негативные сюжеты", "≥1".
- **Таблица доказательств:** Выбери 5-8 самых важных событий из списка релевантных статей. Укажи дату, краткую суть, название источника и вес домена из политики. Если домена нет в политике, ставь "—".
- **Источники:** Для каждого утверждения указывай в скобках ключевые СМИ, подтверждающие информацию (например, "РБК, Коммерсантъ").
- **CEO News:** Если в новостях есть информация о смене CEO, которая отличается от ЕГРЮЛ, кратко упомяни это.
- **Не выдумывай информацию.** Весь твой ответ должен строго базироваться на предоставленных исходных данных.
- **Результат верни в виде чистого Markdown, без дополнительных комментариев.**
"""

PROMPT_MARKET_DIGEST_NEWS = """
Ты — senior аналитик-эксперт. Тебе дана итоговая сводка по компании (ниже).
Нужно подготовить КОРОТКИЕ браузерные поисковые запросы на русском, чтобы найти свежие новости
о состоянии рынка этой компании, её конкурентах, а также отраслевые тренды и регуляторные изменения.

Требования к запросам:
- Верни РОВНО 1 варианта как текст, который я вставлю в поисковик
- Запрос 6–12 слов
- По возможности включай: отрасль/рынок, конкуренты/игроки, географию (город или регион), ключевые события (кризис, санкции, господдержка, тендеры и т.п.), год или «2024»/«2025» для свежести.
- Не используй операторы site:, кавычки, и/или сложные логические конструкции. Нужна естественная формулировка.
- Если город отсутствует, опусти его.
- Не выдумывай факты — ориентируйся на сводку.

Итоговая сводка компании:
---
{company_summary}
---
"""
