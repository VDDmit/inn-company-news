import json
import os
import re
import time

import fitz
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from .config.logger_config import get_logger

logger = get_logger("egrul_parser_json")


def download_egrul_pdf(inn: str, download_directory: str = "../output/egrul_pdf") -> str | None:
    absolute_download_directory = os.path.abspath(download_directory)
    if not os.path.exists(absolute_download_directory):
        os.makedirs(absolute_download_directory)
        logger.info(f"Создана директория для загрузки: {absolute_download_directory}")

    chrome_options = Options()
    prefs = {
        "download.default_directory": absolute_download_directory,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_settings.popups": 0
    }
    chrome_options.add_experimental_option("prefs", prefs)
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Дополнительные аргументы для обхода обнаружения Selenium
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)

    driver = None
    final_renamed_path = None

    try:
        logger.info(f"Инициализация Chrome WebDriver для ИНН: {inn}")
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        logger.info("Переход на страницу https://egrul.nalog.ru/index.html")
        driver.get("https://egrul.nalog.ru/index.html")

        wait = WebDriverWait(driver, 20)
        logger.info(f"Ввод ИНН {inn} в поле запроса.")
        query_input = wait.until(EC.presence_of_element_located((By.ID, "query")))
        query_input.send_keys(inn)

        logger.info("Нажатие кнопки поиска.")
        search_button = wait.until(EC.element_to_be_clickable((By.ID, "btnSearch")))
        search_button.click()

        try:
            logger.info("Ожидание результатов поиска или сообщения 'данные не найдены'.")
            wait.until(
                EC.any_of(
                    EC.visibility_of_element_located((By.ID, "resultPanel")),
                    EC.visibility_of_element_located((By.ID, "noDataFound"))
                )
            )

            try:
                no_data_found_element = driver.find_element(By.ID, "noDataFound")
                if no_data_found_element.is_displayed():
                    logger.info(f"По ИНН {inn} данные не найдены.")
                    return None
            except NoSuchElementException:
                # Это ожидаемо, если данные найдены
                logger.debug("Элемент 'noDataFound' не обнаружен, предполагаем наличие результатов.")
                pass  # Продолжаем, если 'noDataFound' не отображается

            files_before_download = set(os.listdir(absolute_download_directory))
            logger.debug(f"Файлы в директории загрузки до начала скачивания: {len(files_before_download)}")

            logger.info(f"Нажатие кнопки загрузки для ИНН {inn}.")
            download_button = wait.until(
                EC.element_to_be_clickable((By.XPATH,
                                            "//div[@id='resultContent']/div[contains(@class, 'res-row')][1]//button[contains(@class, 'op-excerpt')]"))
            )
            download_button.click()

            logger.info(f"Запущено скачивание для ИНН {inn}. Ожидание файла...")

            timeout = 60
            start_time = time.time()
            downloaded_file = None

            while time.time() - start_time < timeout:
                current_files = set(os.listdir(absolute_download_directory))
                new_files = current_files - files_before_download
                if new_files:
                    potential_file_names = [f for f in new_files if not f.endswith('.crdownload')]
                    if potential_file_names:
                        downloaded_file_name = potential_file_names[0]  # Берем первый найденный файл
                        full_path = os.path.join(absolute_download_directory, downloaded_file_name)
                        try:
                            # Ждем, пока размер файла станет больше нуля, что означает завершение загрузки
                            if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
                                downloaded_file = downloaded_file_name
                                logger.debug(
                                    f"Найден файл {downloaded_file_name} размером {os.path.getsize(full_path)} байт.")
                                break
                            else:
                                logger.debug(
                                    f"Файл {downloaded_file_name} имеет нулевой размер или еще не завершен. Ожидание...")
                        except OSError as os_err:
                            logger.warning(f"Ошибка ОС при проверке файла {full_path}: {os_err}")
                    else:
                        logger.debug(f"Присутствуют только временные файлы (.crdownload). Ожидание завершения...")
                time.sleep(1)

            if not downloaded_file:
                logger.error(f"Ошибка: Файл не появился в директории для ИНН {inn} в течение {timeout} секунд.")
                return None

            original_path = os.path.join(absolute_download_directory, downloaded_file)
            logger.info(f"PDF успешно скачан: {original_path}")
            new_path = os.path.join(absolute_download_directory, f"{inn}.pdf")

            if os.path.exists(new_path):
                os.remove(new_path)
                logger.warning(f"Существующий файл {new_path} был удален перед переименованием.")

            os.rename(original_path, new_path)
            logger.info(f"Файл переименован в: {new_path}")
            final_renamed_path = new_path

        except TimeoutException as e:
            logger.error(f"Таймаут при ожидании элементов или загрузки для ИНН {inn}: {e}", exc_info=True)
            return None
        except NoSuchElementException as e:
            logger.error(f"Элемент не найден при обработке результатов поиска для ИНН {inn}: {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"Ошибка при обработке результатов поиска для ИНН {inn}: {e}", exc_info=True)
            return None

    except WebDriverException as e:
        logger.critical(
            f"Произошла ошибка WebDriver: {e}. Убедитесь, что версия ChromeDriver соответствует версии Chrome, и Chrome установлен.",
            exc_info=True)
        return None
    except Exception as e:
        logger.critical(f"Произошла общая ошибка при работе с Selenium для ИНН {inn}: {e}", exc_info=True)
        return None
    finally:
        if driver:
            logger.info("Закрытие Chrome WebDriver.")
            driver.quit()

    return final_renamed_path


def egrul_pars_pdf_to_json(inn: str, pdf_output_directory: str,
                           json_output_directory: str = "../output/egrul_json") -> str | None:
    logger.info(f"Попытка загрузить PDF ЕГРЮЛ для ИНН: {inn}")
    pdf_file_path = download_egrul_pdf(inn, pdf_output_directory)
    if not pdf_file_path:
        logger.error(f"Не удалось получить PDF для парсинга по ИНН {inn}.")
        return None

    try:
        logger.info(f"Открытие PDF-файла для парсинга: {pdf_file_path}")
        full_text = ""
        with fitz.open(pdf_file_path) as doc:
            for page_num, page in enumerate(doc):
                full_text += page.get_text()
                logger.debug(f"Извлечен текст со страницы {page_num + 1}.")

        logger.info("Начало извлечения данных из PDF-контента.")

        def extract_field(pattern, text, group=1, flags=re.DOTALL):
            match = re.search(pattern, text, flags)
            if match:
                return ' '.join(match.group(group).strip().split())
            return None

        def extract_list(pattern, text, flags=re.DOTALL):
            matches = re.findall(pattern, text, flags)
            return [tuple(' '.join(item.strip().split()) for item in match) for match in matches]

        ogrn_raw = extract_field(r'ОГРН\s+([\d\s]+)', full_text)
        capital_amount_raw = extract_field(r'Размер \(в рублях\)\s*([^\n]+)', full_text)
        data = {
            'company_info': {
                'full_name': extract_field(r'Полное наименование на русском языке\s(.*?)\s\d+\sГРН и дата', full_text),
                'short_name': extract_field(r'Сокращенное наименование на русском языке\s(.*?)\s\d+\sГРН и дата',
                                            full_text),
                'ogrn': ogrn_raw.replace(" ", "") if ogrn_raw else None,
                'inn': extract_field(r'ИНН юридического лица\s+(\d+)', full_text),
                'kpp': extract_field(r'КПП юридического лица\s+(\d+)', full_text),
                'registration_date': extract_field(r'Дата регистрации\s+([\d\.]+)', full_text),
                'legal_address': extract_field(r'Адрес юридического лица\s(.*?)\s\d+\sГРН и дата', full_text),
                'status': "Действующая"
            },
            'capital': {
                'type': extract_field(r'Сведения об уставном капитале.*?Вид\s(.*?)\s\d+\sРазмер', full_text),
                'amount_rub': float(
                    capital_amount_raw.replace(" ", "").replace(",", ".")) if capital_amount_raw else None
            },
            'director': {
                'full_name': extract_field(r'Фамилия\s+Имя\s+Отчество\s(.*?)\s\d+\sИНН', full_text),
                'position': extract_field(r'Должность\s(.*?)\s\d+\sГРН и дата', full_text),
                'inn': extract_field(r'лице\s+\d+\s+Фамилия.*?ИНН\s+(\d+)', full_text)
            },
            'founders': [],
            'activities': {
                'primary': None,
                'additional': []
            },
            'licenses': [],
            'registrations': {
                'tax_authority': {
                    'name': extract_field(r'Сведения о налоговом органе, в котором.*?на учете\s(.*?)\s\d+\sГРН',
                                          full_text),
                    'registration_date': extract_field(r'Дата постановки на учет в налоговом органе\s+([\d\.]+)',
                                                       full_text)
                },
                'social_fund_pension': {
                    'reg_number': extract_field(
                        r'пенсионному страхованию.*?Регистрационный номер страхователя\s+([\d-]+)', full_text),
                    'registration_date': extract_field(
                        r'пенсионному страхованию.*?Дата постановки на учет в качестве страхователя\s+([\d\.]+)',
                        full_text)
                },
                'social_fund_social': {
                    'reg_number': extract_field(
                        r'социальному страхованию.*?Регистрационный номер страхователя\s+([\d]+)', full_text),
                    'registration_date': extract_field(
                        r'социальному страхованию.*?Дата постановки на учет в качестве страхователя\s+([\d\.]+)',
                        full_text)
                }
            }
        }
        logger.debug("Начальная структура данных заполнена извлеченными полями.")

        liquidation_status_match = re.search(
            r'Сведения о состоянии юридического лица.*?Состояние юридического лица\s(.*?)\s\d+\sГРН и дата', full_text,
            re.DOTALL)
        if liquidation_status_match:
            data['company_info']['status'] = ' '.join(liquidation_status_match.group(1).strip().split())
            logger.debug(f"Статус компании обновлен до: {data['company_info']['status']}")

        founders_block_match = re.search(
            r'Сведения об участниках / учредителях юридического лица(.*?)(?=Сведения об учете в налоговом органе|Сведения о держателе реестра акционеров)',
            full_text, re.DOTALL)
        if founders_block_match:
            founders_block = founders_block_match.group(1)
            founder_matches = re.finditer(
                r'Фамилия\s+Имя\s+Отчество\s+(?P<name>.*?)\s+\d+\s+ИНН\s+(?P<inn>\d+).*?Номинальная стоимость доли \(в рублях\)\s+(?P<share_rub>.*?)\s+\d+\s+Размер доли \(в процентах\)\s+(?P<share_pct>.*?)(\s+\d+\s+ГРН|$)',
                founders_block, re.DOTALL)
            for match in founder_matches:
                founder_data = match.groupdict()

                share_rub_str_match = re.search(r'[\d\s.,]+', founder_data.get('share_rub', '0'))
                share_pct_str_match = re.search(r'[\d\s.,]+', founder_data.get('share_pct', '0'))

                share_rub_str = share_rub_str_match.group(0) if share_rub_str_match else '0'
                share_pct_str = share_pct_str_match.group(0) if share_pct_str_match else '0'

                data['founders'].append({
                    'full_name': ' '.join(founder_data['name'].strip().split()),
                    'inn': founder_data['inn'],
                    'share_rub': float(share_rub_str.strip().replace(" ", "").replace(",", ".")),
                    'share_percent': float(share_pct_str.strip().replace(" ", "").replace(",", "."))
                })
            logger.debug(f"Извлечено {len(data['founders'])} учредителей.")

        primary_activity_match = re.search(
            r'Сведения об основном виде деятельности.*?Код и наименование вида деятельности\s+([\d\.]+)\s+(.*?)\s+\d+\s+ГРН',
            full_text, re.DOTALL)
        if primary_activity_match:
            data['activities']['primary'] = {
                'code': primary_activity_match.group(1).strip(),
                'name': ' '.join(primary_activity_match.group(2).strip().split())
            }
            logger.debug(f"Извлечен основной вид деятельности: {data['activities']['primary']['name']}")

        additional_activities_block_match = re.search(
            r'Сведения о дополнительных видах деятельности(.*?)Сведения о лицензиях', full_text, re.DOTALL)
        if additional_activities_block_match:
            additional_activities_block = additional_activities_block_match.group(1)
            additional_activities = extract_list(
                r'Код и наименование вида деятельности\s+([\d\.]+)\s+(.*?)\s+\d+\s+ГРН', additional_activities_block)
            for code, name in additional_activities:
                data['activities']['additional'].append({'code': code, 'name': name})
            logger.debug(f"Извлечено {len(data['activities']['additional'])} дополнительных видов деятельности.")

        licenses_block_match = re.search(r'Сведения о лицензиях(.*?)Сведения о записях, внесенных', full_text,
                                         re.DOTALL)
        if licenses_block_match:
            licenses_block = licenses_block_match.group(1)
            license_matches = re.finditer(
                r'Серия и номер лицензии\s+(?P<number>.*?)\s+\d+\s+Дата лицензии\s+(?P<issue_date>[\d\.]+)\s+\d+\s+Дата начала действия лицензии\s+(?P<start_date>[\d\.]+)\s+\d+\s+Дата окончания действия лицензии\s+(?P<end_date>.*?)\s+\d+\s+Наименование лицензируемого вида деятельности.*?\s+(?P<type>.*?)\s+\d+\s+Наименование лицензирующего органа\s+(?P<authority>.*?)\s+\d+\s+ГРН',
                licenses_block, re.DOTALL)
            for match in license_matches:
                lic_data = match.groupdict()
                data['licenses'].append({
                    'number': ' '.join(lic_data['number'].strip().split()),
                    'issue_date': lic_data['issue_date'],
                    'start_date': lic_data['start_date'],
                    'end_date': ' '.join(lic_data['end_date'].strip().split()),
                    'type': ' '.join(re.sub(r'\d+\s+Наименование лицензируемого вида деятельности.*', '',
                                            lic_data['type']).strip().split()),
                    'issuing_authority': ' '.join(lic_data['authority'].strip().split())
                })
            logger.debug(f"Извлечено {len(data['licenses'])} лицензий.")

        json_output = json.dumps(data, ensure_ascii=False, indent=4)

        os.makedirs(json_output_directory, exist_ok=True)
        json_file_path = os.path.join(json_output_directory, f"{inn}.json")

        with open(json_file_path, 'w', encoding='utf-8') as f:
            f.write(json_output)

        logger.info(f"JSON-файл сохранен в: {json_file_path}")
        logger.debug(f"Полное содержимое JSON для ИНН {inn}:\n{json_output}")

        return json_output

    except Exception as e:
        logger.error(f"Ошибка при парсинге PDF-файла {pdf_file_path} для ИНН {inn}: {e}", exc_info=True)
        return None
    finally:
        if os.path.exists(pdf_file_path):
            pass


def run_egrul_parser_task(inn: str, pdf_download_dir: str, json_output_dir: str) -> str | None:
    logger.info(f"--- Запуск задачи парсинга ЕГРЮЛ PDF для ИНН: {inn} ---")

    json_output = egrul_pars_pdf_to_json(inn, pdf_download_dir, json_output_dir)

    if json_output:
        logger.info(f"Парсинг PDF для ИНН {inn} успешно завершен. JSON-данные сохранены.")
        logger.info(f"\n--- Результат парсинга (JSON) для ИНН {inn} ---\n{json_output}")
    else:
        logger.error(f"Не удалось получить или спарсить данные для ИНН {inn}.")

    logger.info(f"--- Задача парсинга ЕГРЮЛ PDF для ИНН: {inn} завершена ---")
    return json_output
