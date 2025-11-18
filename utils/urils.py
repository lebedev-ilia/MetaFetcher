try:
    from transliterate import translit
    TRANSLIT_AVAILABLE = True
except ImportError:
    TRANSLIT_AVAILABLE = False
    print("Warning: transliterate is not installed. Using fallback transliteration.")
from datetime import datetime, timedelta
from typing import Optional
import re


def transliterate(text: str, lang_code: str = 'ru') -> str:
    """
    Транслитерация текста.
    
    Args:
        text: Текст для транслитерации
        lang_code: Код языка
        
    Returns:
        Транслитерированный текст
    """
    if TRANSLIT_AVAILABLE:
        try:
            return translit(text, lang_code, reversed=True)
        except:
            pass
    
    # Простая замена для fallback
    replacements = {
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'H', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya',
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        ' ': '_'
    }
    
    result = text
    for ru, en in replacements.items():
        result = result.replace(ru, en)
    
    return result


def parse_duration_iso(duration_iso: str) -> Optional[int]:
    """
    Парсит ISO 8601 длительность в секунды.
    
    Args:
        duration_iso: Длительность в формате ISO 8601 (например, PT3M34S)
        
    Returns:
        Длительность в секундах или None
    """
    if not duration_iso:
        return None
    
    import re
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_iso)
    if not match:
        return None
    
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    
    return hours * 3600 + minutes * 60 + seconds


def get_time_interval(published_at: str) -> str:
    """
    Определяет временной интервал для видео на основе даты публикации.
    
    Args:
        published_at: Дата публикации в формате ISO 8601
        
    Returns:
        Название временного интервала
    """
    try:
        published_date = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
        now = datetime.now(published_date.tzinfo)
        age = now - published_date
        
        if age < timedelta(days=1):
            return "less-1day"
        elif age < timedelta(days=7):
            return "1day-1week"
        elif age < timedelta(days=30):
            return "1week-1month"
        elif age < timedelta(days=90):
            return "1month-3month"
        elif age < timedelta(days=180):
            return "3month-6month"
        elif age < timedelta(days=365):
            return "6month-1year"
        elif age < timedelta(days=1095):  # 3 years
            return "1year-3year"
        else:
            return "3year-more"
    except:
        return "3year-more"  # По умолчанию для старых видео


def _is_russian_query(query: str) -> bool:
    """
    Определяет, является ли запрос русским.
    
    Args:
        query: Поисковый запрос
        
    Returns:
        True если запрос на русском, False иначе
    """
    # Проверяем наличие кириллических символов (Unicode диапазон 0400-04FF)
    return any('\u0400' <= char <= '\u04FF' for char in query)

# Функция для преобразования ISO 8601 длительности в секунды
def parse_duration_iso(duration_iso: str) -> int:
    """Парсит ISO 8601 длительность в секунды."""
    if not duration_iso:
        return None
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_iso)
    if not match:
        return None
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds

# Функция для извлечения тегов из текста
def extract_tags_from_text(text: str) -> list:
    """
    Извлекает теги (слова, начинающиеся с #) из текста.
    Теги могут содержать буквы, цифры, подчеркивания, дефисы и другие символы.
    """
    if not text:
        return []
    # Находим все теги: # за которым следуют буквы, цифры, подчеркивания, дефисы и другие допустимые символы
    # Останавливаемся на пробеле, знаке препинания или конце строки
    tags = re.findall(r'#[\w\-_]+', text)
    # Убираем # и возвращаем уникальные теги (в нижнем регистре для единообразия)
    unique_tags = list(set([tag[1:].lower() for tag in tags if len(tag) > 1]))
    return unique_tags

# Функция для очистки текста от тегов
def clean_text_from_tags(text: str) -> str:
    """Удаляет теги (слова, начинающиеся с #) из текста."""
    if not text:
        return text
    # Удаляем все теги и лишние пробелы
    # Паттерн соответствует тегам с возможными пробелами после них
    cleaned = re.sub(r'#[\w\-_]+\s*', '', text)
    # Убираем множественные пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned)
    # Убираем пробелы в начале и конце
    return cleaned.strip()