import os
from typing import Optional

from internship_analytics.modules.request_to_gemini_api import call_to_gemini_api
from .config.logger_config import get_logger

logger = get_logger("merge_summary")

PROMPT_FUSE = """
Ты — senior аналитик-эксперт. У тебя есть две итоговые сводки по одной компании: 
по самой компании и по её руководителю (SEO). 
Задача — подготовить ЕДИНЫЙ, целостный и консистентный аналитический отчет.
Пиши сразу по теме, без вступлений и обращений, без слов о себе.

Контекст:
- ИНН: {inn}
- Компания: {company_full_name}
- Руководитель: {seo_full_name}
- Город: {city}

ТРЕБОВАНИЯ К ОТЧЕТУ:
1. **Синтез**:
   - Объедини обе сводки в единую структуру.
   - Удали дублирующиеся факты, переформулируй их в одно ясное утверждение.
   - Если есть противоречия — отметь их явно. Основную версию выбери по принципу: 
     большее количество подтверждений, более свежая дата, более высокий вес источников.
   - Сохрани как факты, так и смежный контекст (партнеры, суды, география, проекты).

2. **Структура отчета**:
   - **Executive Summary (Резюме)**: 5–7 ключевых выводов о компании и её руководителе.
   - **Ключевые факты и события** (сгруппируй по блокам):
       • Финансы и право (судебные дела, налоги, отчетность, регистрационные данные).  
       • Партнёры и контрагенты (связи, альянсы, конфликты).  
       • Репутация и PR (медиа, позитив/негатив, имидж руководителя).  
       • География и активы (офисы, филиалы, зарубежные связи).  
       • Операционная деятельность (продукты, услуги, проекты).  
   - **Риски и возможности**: угрозы для бизнеса и управленца, а также перспективы роста.  
   - **Хронология**: если есть даты, построй последовательность ключевых событий.  
   - **Заключение**: общий аналитический вывод по состоянию компании и фигуре SEO.

3. **Работа с данными (числовыми и табличными)**:
   - Если в тексте встречаются данные в формате JSON или числовые показатели (например, выручка, долги, капитал, количество сотрудников), 
     не изменяй сам текст сводок, но:
       • вынеси ключевые значения,  
       • сделай краткие выводы по ним (например: рост/спад, соотношения, тренды).  
   - Представь такие данные в структурированном виде: список или мини-таблица.

4. **Тон и стиль**:
   - Четкий аналитический язык, без воды.  
   - Используй списки, подзаголовки, структурированную подачу.  
   - Русский язык, деловой стиль.  
   - Пиши так, как будто это доклад для совета директоров или инвесторов.

Сводка A (Компания):
---
{company_summary}
---

Сводка B (SEO):
---
{seo_summary}
---
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
