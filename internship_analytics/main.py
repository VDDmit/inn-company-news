import re
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Optional

from dotenv import load_dotenv

from internship_analytics.conf import *
from internship_analytics.modules.egrul_parser_json import run_egrul_parser_task
from internship_analytics.modules.gemini_3_factor_process_data import run_gemini_processing_pipeline
from internship_analytics.modules.market_digest import get_market_digest
from internship_analytics.modules.news import run_full_search_and_parse
from internship_analytics.modules.pandas_processor import *
from internship_analytics.modules.request_to_gemini_api import call_to_gemini_api
from modules.config.logger_config import get_logger
from modules.merge_summary import fuse_summaries

load_dotenv()
logger = get_logger("main")


# =========================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =========================

def _calculate_control_digit(digits_str: str, weights: list[int]) -> int:
    s = sum(int(d) * w for d, w in zip(digits_str, weights))
    control_digit = s % 11
    return 0 if control_digit == 10 else control_digit


def _extract_city_from_legal_address(legal_address_str: Optional[str]) -> Optional[str]:
    if not legal_address_str:
        return None
    city_pattern = r"(?:Г\.|ГОР\.)\s*([А-ЯЁ][А-ЯЁ\s-]+)"
    match = re.search(city_pattern, legal_address_str.upper())
    if match:
        return match.group(1).strip()
    return None


def validity_inn_check(target_inn: str) -> str:
    try:
        if not isinstance(target_inn, str):
            logger.error(f"ИНН '{target_inn}' невалиден: ИНН должен быть строкой.")
            return "ИНН невалиден: Неверный тип данных"

        if len(target_inn) != 10:
            logger.warning(
                f"ИНН '{target_inn}' невалиден: Неверная длина. Ожидается 10 цифр для ЮЛ, получено {len(target_inn)}."
            )
            return "ИНН невалиден: Неверная длина (ожидается 10 цифр для ЮЛ)"

        if not target_inn.isdigit():
            logger.error(f"ИНН '{target_inn}' невалиден: Содержит нецифровые символы.")
            return "ИНН невалиден: Содержит нецифровые символы"

        weights_for_10th_digit = [2, 4, 10, 3, 5, 9, 4, 6, 8]
        calculated_10th_digit = _calculate_control_digit(target_inn[0:9], weights_for_10th_digit)

        if calculated_10th_digit != int(target_inn[9]):
            logger.warning(
                f"ИНН '{target_inn}' невалиден: Ошибка контрольной суммы 10-й цифры. "
                f"Ожидалось {calculated_10th_digit}, получено {int(target_inn[9])}."
            )
            return "ИНН невалиден: Ошибка контрольной суммы (10-я цифра)"

        logger.info(f"ИНН '{target_inn}' успешно прошел все проверки и является валидным.")
        return target_inn

    except Exception as e:
        logger.exception(f"Непредвиденная ошибка при проверке ИНН '{target_inn}': {e}")
        return "ИНН невалиден: Внутренняя ошибка системы"


# =========================
# КОНТЕКСТ КОМПАНИИ
# =========================

@dataclass
class CompanyContext:
    inn: str
    egrul_json: dict[str, Any]
    csv_json: Any
    company_full_name: str
    seo_full_name: str
    city: Optional[str]

    @property
    def domains(self) -> list[str]:
        # те же домены, что и в DOMAIN_WEIGHTS
        return list(DOMAIN_WEIGHTS.keys())


# Глобально доступный текущий контекст (по желанию)
CURRENT_CONTEXT: Optional[CompanyContext] = None


def collect_company_context(valid_inn: str) -> CompanyContext:
    """
    Загружает ЕГРЮЛ и CSV-данные, извлекает ключевые поля и формирует контекст компании.
    Контекст возвращается и складывается в глобальную переменную CURRENT_CONTEXT для повторного использования.
    """
    logger.info("Загрузка данных ЕГРЮЛ/CSV и формирование контекста.")
    egrul_data_json = run_egrul_parser_task(valid_inn, EGRUL_PDF_DIR, EGRUL_JSON_DIR)
    csv_data_json = get_company_json(COMPANY_INFO_CSV, valid_inn)

    egrul_obj = json.loads(egrul_data_json)

    company_full_name = egrul_obj["company_info"]["full_name"]
    seo_full_name = egrul_obj["director"]["full_name"]
    city = _extract_city_from_legal_address(egrul_obj["company_info"]["legal_address"])

    ctx = CompanyContext(
        inn=valid_inn,
        egrul_json=egrul_obj,
        csv_json=csv_data_json,
        company_full_name=company_full_name,
        seo_full_name=seo_full_name,
        city=city
    )

    global CURRENT_CONTEXT
    CURRENT_CONTEXT = ctx
    return ctx


# =========================
# ОБРАБОТКА НОВОСТЕЙ
# =========================

def _process_news_block(*,
                        user_search_query: str,
                        context_query: str,
                        domains: list[str],
                        num_pages: int,
                        output_dir: str) -> dict[str, Optional[str]]:
    """
    Универсальная обёртка: поиск новостей + пайплайн Gemini.
    Возвращает пути ко всем уровням, если они были созданы.
    """
    paths: dict[str, Optional[str]] = {
        "raw_json_path": None,
        "level_1_cleaned_path": None,
        "level_2_filtered_path": None,
        "level_3_summary_path": None,
    }

    raw_path = run_full_search_and_parse(
        user_search_query=user_search_query,
        domains_to_search=domains,
        num_pages=num_pages,
        path_to_output=output_dir
    )
    paths["raw_json_path"] = raw_path

    if not raw_path:
        logger.info(f"По запросу '{user_search_query}' сырые новости не получены.")
        return paths

    # Запуск пайплайна Gemini
    summary_path = run_gemini_processing_pipeline(
        raw_json_file_path=raw_path,
        context_query=context_query,
        processed_data_dir=output_dir
    )
    paths["level_3_summary_path"] = summary_path

    # Предсказуемые имена файлов для уровней 1/2
    try:
        base_name = os.path.splitext(os.path.basename(raw_path))[0]
        paths["level_1_cleaned_path"] = os.path.join(output_dir, f"{base_name}_level_1_cleaned.json")
        paths["level_2_filtered_path"] = os.path.join(output_dir, f"{base_name}_level_2_filtered.json")
    except Exception as e:
        logger.warning(f"Не удалось определить пути промежуточных файлов для '{raw_path}': {e}")

    return paths


def process_company_news(ctx: CompanyContext) -> dict[str, Optional[str]]:
    """
    Формирует поисковый запрос по компании и обрабатывает новости пайплайном.
    """
    query = " ".join(filter(None, [ctx.inn, ctx.company_full_name, ctx.city])).strip()
    return _process_news_block(
        user_search_query=query,
        context_query=query,
        domains=ctx.domains,
        num_pages=PAGES_TO_SEARCH_COMPANY,
        output_dir=COMPANY_NEWS_OUTPUT_DIR
    )


def process_seo_news(ctx: CompanyContext) -> dict[str, Optional[str]]:
    """
    Формирует поисковый запрос по руководителю (SEO) и обрабатывает новости пайплайном.
    """
    query = " ".join(filter(None, [ctx.seo_full_name, ctx.city])).strip()
    return _process_news_block(
        user_search_query=query,
        context_query=query,
        domains=ctx.domains,
        num_pages=PAGES_TO_SEARCH_SEO,
        output_dir=SEO_NEWS_OUTPUT_DIR
    )


# =========================
# ТОЧКА ВХОДА
# =========================

def start_internship_analytics(target_inn: str) -> str:
    logger.info("Запуск валидации ИНН.")
    valid_inn = validity_inn_check(target_inn)

    if not valid_inn.isdigit() or len(valid_inn) != 10:
        return json.dumps({"error": valid_inn}, ensure_ascii=False, indent=2)

    ctx = collect_company_context(valid_inn)

    # Новости
    company_news = process_company_news(ctx)
    seo_news = process_seo_news(ctx)

    company_summary_path = company_news.get("level_3_summary_path")
    seo_summary_path = seo_news.get("level_3_summary_path")

    os.makedirs(FINAL_REPORTS_OUTPUT_DIR, exist_ok=True)

    company_seo_fused_output_path = os.path.join(FINAL_REPORTS_OUTPUT_DIR, f"{ctx.inn}_company_seo_fused_summary.txt")
    csv_fused_output_path = os.path.join(FINAL_REPORTS_OUTPUT_DIR, f"{ctx.inn}_csv_company_seo_fused_summary.txt")

    company_seo_fused_path = fuse_summaries(
        first_summary_path=company_summary_path,
        second_summary_path=seo_summary_path,
        output_path=company_seo_fused_output_path,
        inn=ctx.inn,
        company_full_name=ctx.company_full_name,
        seo_full_name=ctx.seo_full_name,
        city=ctx.city,
        model="models/gemini-2.5-pro",
        max_output_tokens=10000,
    )

    time.sleep(20)

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(ctx.csv_json, tmp, ensure_ascii=False, indent=2)
        tmp_path = tmp.name

    logger.info(f"{ctx.csv_json}")
    csv_company_seo_fused_path = fuse_summaries(
        first_summary_path=company_seo_fused_path,
        second_summary_path=tmp_path,
        output_path=csv_fused_output_path,
        inn=ctx.inn,
        company_full_name=ctx.company_full_name,
        seo_full_name=ctx.seo_full_name,
        city=ctx.city,
        model="models/gemini-2.5-pro",
        max_output_tokens=10000,
    )

    time.sleep(20)

    market_digest_path = ""
    seed_text = None
    if company_seo_fused_path and os.path.exists(company_seo_fused_path):
        seed_text = open(company_seo_fused_path, "r", encoding="utf-8").read()
    elif company_summary_path and os.path.exists(company_summary_path):
        seed_text = open(company_summary_path, "r", encoding="utf-8").read()
    elif seo_summary_path and os.path.exists(seo_summary_path):
        seed_text = open(seo_summary_path, "r", encoding="utf-8").read()
    else:
        seed_text = json.dumps(ctx.egrul_json, ensure_ascii=False)

    if seed_text:
        market_digest_path = get_market_digest(seed_text, domains=ctx.domains)

    time.sleep(20)

    # ---------- FINAL SUMMARY ----------
    final_summary_path = os.path.join(FINAL_REPORTS_OUTPUT_DIR, f"{ctx.inn}_final_summary.md")

    with open(csv_company_seo_fused_path, "r", encoding="utf-8") as f:
        base_summary_text = f.read().strip()

    market_digest_text: str = ""
    try:
        if market_digest_path and os.path.exists(market_digest_path):
            with open(market_digest_path, "r", encoding="utf-8") as mf:
                market_digest_text = mf.read().strip()
        else:
            if isinstance(market_digest_path, str):
                market_digest_text = market_digest_path.strip()
    except Exception as e:
        logger.warning(f"Не удалось прочитать market_digest: {e}")

    final_prompt = f"""
    {FINAL_REPORT_PROMPT_TEMPLATE_V2.format(
        generation_date=datetime.now().strftime("%d.%m.%Y"),
    )}


    БАЗОВОЕ САММАРИ (СОХРАНИТЬ ФОРМАТ):
    ----------------------------------------
    {base_summary_text}
    ----------------------------------------

    МАРКЕТ-ДАЙДЖЕСТ (ИСТОЧНИК ДЛЯ ДОПОЛНЕНИЯ):
    ----------------------------------------
    {market_digest_text}
    ----------------------------------------
    """

    # вызов Gemini; гибкая попытка на случай другой сигнатуры
    try:
        final_summary_text = call_to_gemini_api(
            prompt=final_prompt,
            model="models/gemini-2.5-pro",
            max_output_tokens=8000,
            temperature=0.2,
        )
    except TypeError:
        final_summary_text = call_to_gemini_api(
            final_prompt,
            model="models/gemini-2.5-pro",
            max_output_tokens=4000,
            temperature=0.2,
        )

    with open(final_summary_path, "w", encoding="utf-8") as f:
        f.write(final_summary_text if isinstance(final_summary_text, str) else str(final_summary_text))

    logger.info(f"Финальное саммари сохранено: {final_summary_path}")

    result = {
        "inn": ctx.inn,
        "company_full_name": ctx.company_full_name,
        "seo_full_name": ctx.seo_full_name,
        "city": ctx.city,
        "egrul_json": ctx.egrul_json,
        "csv_json": ctx.csv_json,
        "company_news": company_news,
        "seo_news": seo_news,
        "final_fused_summary_path": company_seo_fused_path,
        "csv_fused_summary_path": csv_company_seo_fused_path,
        "market_digest_path": market_digest_path,
        "final_summary_path": final_summary_path
    }
    return json.dumps(result, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    print(start_internship_analytics(str(7716902370)))
