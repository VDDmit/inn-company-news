import os
from datetime import datetime
from typing import Optional

from internship_analytics.modules.request_to_gemini_api import call_to_gemini_api
from .config.logger_config import get_logger

logger = get_logger("merge_summary")

PROMPT_FUSE = r"""
You are a senior risk analyst. Operate in STRICT extractive mode: use ONLY the two provided summaries.  
Do NOT invent facts. If data is missing, output exactly: “нет данных”.

=== OUTPUT LANGUAGE ===
- Russian only.

=== TIME WINDOW ===
- 365 days ending on {generation_date} (inclusive).
- Dates must be in DD.MM.YYYY format.

=== INPUTS ===
- INN: {inn}
- Company: {company_full_name}
- CEO: {seo_full_name}
- City: {city}
- Company Summary: {company_summary}
- CEO Summary: {seo_summary}

=== GOAL ===
Produce ONE unified, consistent analytical report.  
Merge the two summaries, eliminate duplicates, and explicitly flag contradictions.  
If conflicting, prefer the version with:  
- more confirmations,  
- more recent date,  
- higher-weight sources.

=== REPORT STRUCTURE (strict order) ===

1) Title block (two lines):  
   Одностраничный отчёт — {company_full_name}  
   (руководитель: {seo_full_name}, ИНН {inn}, город {city})

2) Executive Summary (Резюме)  
   - 5–7 concise bullet points with the key findings about the company and its CEO.  
   - No duplication, fact-based only.  

3) Ключевые факты и события  
   Group as bullet points under subheadings:  
   - Финансы и право (lawsuits, taxes, reports, registration)  
   - Партнёры и контрагенты (links, alliances, conflicts)  
   - Репутация и PR (media, tone, CEO image)  
   - География и активы (offices, branches, foreign ties)  
   - Операционная деятельность (products, services, projects)  

4) Риски и возможности  
   - List business threats and risks for the CEO.  
   - List potential growth opportunities.  

5) Хронология  
   - Build a timeline of key events with dates.  
   - If no dates → output “нет данных”.  

6) Заключение  
   - One concise paragraph with the overall analytical judgment about the company and the CEO.  

7) Numerical and tabular data  
   - If JSON data or numerical indicators exist (revenue, debt, capital, employees):  
     • extract key values,  
     • add brief business interpretation (growth/decline/trend),  
     • present as a short list or mini-table.  
   - If none → “нет данных”.  

8) Источники  
   - Always place sources at the end in a table.  
   - If multiple tables exist → merge them.  
   - If no sources → output “нет данных”.  
   - Table format (Markdown, three columns):  
     | Дата | Заголовок/суть | Источник |

=== STYLE & RULES ===
- Business-like, concise, structured.  
- Output ONLY in Russian.  
- Do not add sections beyond the defined structure.  
- If field missing → output exactly “нет данных”.  
"""


def fuse_summaries(
        first_summary_path: str,
        second_summary_path: str,
        output_path: str,
        inn: Optional[str] = None,
        company_full_name: Optional[str] = None,
        seo_full_name: Optional[str] = None,
        city: Optional[str] = None,
        model: str = "models/gemini-2.5-pro",
        max_output_tokens: int = 3000,
) -> Optional[str]:
    """
    Синтезирует единый отчет на основе двух файлов-саммари.
    """
    try:
        def _read(path: str) -> str:
            if not path or not os.path.exists(path):
                return ""
            with open(path, "r", encoding="utf-8") as f:
                return f.read()

        company_summary = _read(first_summary_path)
        seo_summary = _read(second_summary_path)

        if not company_summary and not seo_summary:
            logger.error("Оба саммари пустые — нечего объединять.")
            return None

        prompt = PROMPT_FUSE.format(
            inn=inn or "",
            company_full_name=company_full_name or "",
            seo_full_name=seo_full_name or "",
            city=city or "",
            company_summary=company_summary or "—",
            seo_summary=seo_summary or "—",
            generation_date=datetime.now().strftime("%d.%m.%Y"),
        )

        fused_text = call_to_gemini_api(prompt, model=model, max_output_tokens=max_output_tokens)
        if not fused_text.strip():
            logger.error("Модель вернула пустой результат.")
            return None

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(fused_text)

        logger.info(f"Финальное саммари сохранено: {output_path}")
        return output_path

    except Exception as e:
        logger.error(f"Ошибка при объединении саммари: {e}")
        return None
