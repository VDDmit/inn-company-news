import json

import pandas as pd

from .config.logger_config import get_logger

logger = get_logger("pandas_processor")


def clean_value(value):
    logger.debug(f"Attempting to clean value: '{value}' (type: {type(value)})")
    if pd.isna(value) or str(value).strip() == '-':
        logger.debug(f"Value '{value}' is NaN or '-', returning None.")
        return None

    value_str = str(value)

    if '%' in value_str:
        logger.debug(f"Value '{value_str}' contains '%', returning as string.")
        return value_str.strip()

    cleaned_str = value_str.replace('\xa0', '').replace(',', '').strip()
    logger.debug(f"String cleaned to: '{cleaned_str}'")

    try:
        num = float(cleaned_str)
        if num.is_integer():
            logger.debug(f"Value '{cleaned_str}' is an integer float, returning as int: {int(num)}")
            return int(num)
        logger.debug(f"Value '{cleaned_str}' is a float, returning as float: {num}")
        return num
    except ValueError:
        logger.debug(f"Value '{cleaned_str}' could not be converted to float, returning as string.")
        return value_str.strip()


def process_nd_ebit(value):
    logger.debug(f"Processing ND/EBIT value: '{value}'")
    if pd.isna(value) or str(value).strip() == '-':
        logger.debug(f"ND/EBIT value '{value}' is NaN or '-', returning None.")
        return None
    processed_value = str(value).strip()
    logger.debug(f"ND/EBIT value '{value}' processed to: '{processed_value}'")
    return processed_value


def extract_yearly_data(row, code, years):
    logger.debug(f"Extracting yearly data for code '{code}' for years {years}")
    data = {}
    for year in years:
        col_name = f"{year}_{code}"
        if col_name in row.index:
            value = row[col_name]
            cleaned = clean_value(value)
            data[year] = cleaned
            logger.debug(f"Year {year}, code {code}: '{value}' cleaned to '{cleaned}'")
        else:
            data[year] = None
            logger.debug(f"Column '{col_name}' not found for code {code}, year {year}. Setting to None.")
    return data


def get_company_json(csv_filepath, inn_to_find):
    logger.info(f"Начало обработки запроса для ИНН: {inn_to_find} из файла: {csv_filepath}")
    try:
        df = pd.read_csv(csv_filepath, sep=';', header=0, dtype=str)
        logger.info(f"CSV файл успешно прочитан: {csv_filepath}. Всего строк: {len(df)}")
    except FileNotFoundError:
        logger.error(f"Ошибка: Файл не найден по пути: {csv_filepath}")
        return {"error": f"Файл не найден по пути: {csv_filepath}"}
    except Exception as e:
        logger.critical(f"Критическая ошибка при чтении CSV файла '{csv_filepath}': {e}", exc_info=True)
        return {"error": f"Ошибка при чтении CSV файла: {e}"}

    inn_column_name = df.columns[0]
    logger.debug(f"ИНН колонка определена как: '{inn_column_name}'")

    company_row = df[df[inn_column_name] == inn_to_find]

    if company_row.empty:
        logger.warning(f"Компания с ИНН {inn_to_find} не найдена в файле.")
        return {"error": f"Компания с ИНН {inn_to_find} не найдена."}

    row = company_row.iloc[0]
    logger.info(f"Компания с ИНН {inn_to_find} найдена. Начинаем извлечение данных.")

    years = [str(y) for y in range(2019, 2025)]
    logger.debug(f"Годы для извлечения данных: {years}")

    result = {
        "inn": row[inn_column_name],
        "general_info": {
            "name": row.get('Name'),
            "okved_name": row.get('ОКВЭ name'),
            "okved_code": row.get('Основной ОКВЭД'),
            "ceo_name": row.get('CEO'),
            "employee_count": clean_value(row.get('Кол-во сотрудников'))
        },
        "financial_metrics": {
            "nd_ebit": process_nd_ebit(row.get('ND/EBIT')),
            "revenue_per_employee": clean_value(row.get('Revenue/employee'))
        },
        "growth_metrics": {
            "year_over_year_revenue_growth": {
                "21/20": clean_value(row.get('21/20')),
                "22/21": clean_value(row.get('22/21')),
                "23/22": clean_value(row.get('23/22')),
                "24/23": clean_value(row.get('24/23'))
            },
            "cagr": {
                "22-24": clean_value(row.get('CAGR 22-24')),
                "20-24": clean_value(row.get('CAGR 20-24'))
            }
        },
        "financial_statements": {
            "revenue": extract_yearly_data(row, '2110', years),
            "gross_profit": extract_yearly_data(row, '2100', years),
            "ebit": extract_yearly_data(row, '2200', years),
            "net_profit": extract_yearly_data(row, '2400', years),
            "operating_cash_flow": extract_yearly_data(row, '4100', years),
            "long_term_debt": extract_yearly_data(row, '1410', years),
            "short_term_debt": extract_yearly_data(row, '1510', years),
            "cash_and_equivalents": extract_yearly_data(row, '1250', years)
        }
    }

    logger.info(f"Данные для ИНН {inn_to_find} успешно извлечены и структурированы.")
    logger.debug(f"Результат для ИНН {inn_to_find}:\n{json.dumps(result, ensure_ascii=False, indent=2)}")
    return result
