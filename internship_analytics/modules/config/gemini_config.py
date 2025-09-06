import os
import sys

from dotenv import load_dotenv
from google import genai
from google.genai import Client

from internship_analytics.modules.config.logger_config import get_logger

load_dotenv()
logger = get_logger("gemini_config")


def get_gemini_config(api_key: str | None = None) -> Client:
    """
    Возвращает сконфигурированный клиент Gemini.

    :param api_key: API-ключ (по умолчанию берётся из переменной окружения GENAI_API_KEY)
    :return: экземпляр google.genai.Client
    """
    try:
        key = api_key or os.environ.get("GENAI_API_KEY")
        if not key:
            raise KeyError("GENAI_API_KEY пуст или не задан.")

        return genai.Client(api_key=key)

    except KeyError as e:
        logger.critical(f"{e} Завершение работы.")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Ошибка инициализации Gemini Client: {e}")
        sys.exit(1)
