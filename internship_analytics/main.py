import json

from dotenv import load_dotenv

from conf import *
from internship_analytics.modules.egrul_parser_json import *
from internship_analytics.modules.gemini_3_factor_process_data import run_gemini_processing_pipeline
from internship_analytics.modules.news import run_full_search_and_parse
from internship_analytics.modules.pandas_processor import *
from modules.config.logger_config import get_logger

load_dotenv()

logger = get_logger("main")


# TODO:   1. Найти способ добывать больше источников новостей <---ok
#         2. Разобраться с СЕО и его надежностью <---> делать запрос в интернет по ФИО СЕО ---> запрос в нейронку
#         3. Тонкая настройка генерации отчета, с разработкой показателей для параметров MECE
#         4. Вынуть данные из егрул, обогатить действующая / ликивдация и тп
#         5. улучшить скрапер по интернету и множественно его задействовать (задумка такая: закинуть инн+сео+город в промпт)
#         и получать различного рода запросы для интернета, тем самым базу информации накапливать и сразу ее оценивать)

def _calculate_control_digit(digits_str: str, weights: list) -> int:
    s = sum(int(d) * w for d, w in zip(digits_str, weights))
    control_digit = s % 11
    return 0 if control_digit == 10 else control_digit


def _extract_city_from_legal_address(legal_address_str: str) -> str | None:
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
                f"ИНН '{target_inn}' невалиден: Неверная длина. Ожидается 10 цифр для ЮЛ, получено {len(target_inn)}.")
            return "ИНН невалиден: Неверная длина (ожидается 10 цифр для ЮЛ)"

        if not target_inn.isdigit():
            logger.error(f"ИНН '{target_inn}' невалиден: Содержит нецифровые символы.")
            return "ИНН невалиден: Содержит нецифровые символы"

        weights_for_10th_digit = [2, 4, 10, 3, 5, 9, 4, 6, 8]
        calculated_10th_digit = _calculate_control_digit(target_inn[0:9], weights_for_10th_digit)

        if calculated_10th_digit != int(target_inn[9]):
            logger.warning(
                f"ИНН '{target_inn}' невалиден: Ошибка контрольной суммы 10-й цифры. Ожидалось {calculated_10th_digit}, получено {int(target_inn[9])}.")
            return "ИНН невалиден: Ошибка контрольной суммы (10-я цифра)"

        logger.info(f"ИНН '{target_inn}' успешно прошел все проверки и является валидным.")
        return target_inn

    except Exception as e:
        logger.exception(f"Непредвиденная ошибка при проверке ИНН '{target_inn}': {e}")
        return "ИНН невалиден: Внутренняя ошибка системы"


def start_internship_analytics(target_inn: str) -> str:
    logger.info("Запуск валидации ИНН.")
    valid_inn = validity_inn_check(target_inn)

    egrul_data_json = run_egrul_parser_task(valid_inn, EGRUL_PDF_DIR, EGRUL_JSON_DIR)
    csv_data_json = get_company_json(COMPANY_INFO_CSV, valid_inn)

    egrul_data_json_to_mas = json.loads(egrul_data_json)

    company_full_name = egrul_data_json_to_mas["company_info"]["full_name"]
    seo_full_name = egrul_data_json_to_mas["director"]["full_name"]
    city = _extract_city_from_legal_address(egrul_data_json_to_mas["company_info"]["legal_address"])

    DOMAIN = list(DOMAIN_WEIGHTS.keys())

    company_news_filepath = run_full_search_and_parse(
        user_search_query=" ".join([valid_inn, company_full_name, city]),
        domains_to_search=DOMAIN,
        num_pages=PAGES_TO_SEARCH_COMPANY,
        path_to_output=COMPANY_NEWS_OUTPUT_DIR
    )
    if company_news_filepath:
        run_gemini_processing_pipeline(
            raw_json_file_path=company_news_filepath,
            context_query=" ".join([valid_inn, company_full_name, city]),
            processed_data_dir=COMPANY_NEWS_OUTPUT_DIR
        )

    seo_news_filepath = run_full_search_and_parse(
        user_search_query=" ".join([seo_full_name, city]),
        domains_to_search=DOMAIN,
        num_pages=PAGES_TO_SEARCH_SEO,
        path_to_output=SEO_NEWS_OUTPUT_DIR
    )
    if seo_news_filepath:
        run_gemini_processing_pipeline(
            raw_json_file_path=seo_news_filepath,
            context_query=" ".join([seo_full_name, city]),
            processed_data_dir=SEO_NEWS_OUTPUT_DIR
        )

    return "Анализ завершен."


if __name__ == "__main__":
    start_internship_analytics(str(7810453178))
