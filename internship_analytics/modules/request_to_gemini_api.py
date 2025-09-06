from dotenv import load_dotenv

from .config.gemini_config import get_gemini_config
from .config.logger_config import get_logger

load_dotenv()
logger = get_logger("request_to_gemini_api")


def call_to_gemini_api(
        prompt: str,
        model: str,
        max_output_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        top_k: int | None = None,
        system_instruction: str | None = None,
) -> str:
    """
    Вызов Gemini API (google-genai). Поддерживает ограничение длины ответа и базовые параметры генерации.
    """
    gemini_client = get_gemini_config()
    try:
        config: dict = {}
        if max_output_tokens is not None:
            config["max_output_tokens"] = max_output_tokens
        if temperature is not None:
            config["temperature"] = temperature
        if top_p is not None:
            config["top_p"] = top_p
        if top_k is not None:
            config["top_k"] = top_k
        if system_instruction is not None:
            config["system_instruction"] = system_instruction

        response = gemini_client.models.generate_content(
            model=model,
            contents=prompt,
            config=config or None,
        )

        return (getattr(response, "text", "") or "").strip()

    except ValueError:
        logger.warning(f"Получен пустой или заблокированный ответ от модели {model}.")
        return ""
    except Exception as e:
        logger.error(f"Ошибка при вызове API Gemini для модели {model}: {e}")
        return ""
