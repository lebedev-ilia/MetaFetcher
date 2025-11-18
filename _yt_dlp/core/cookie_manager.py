import os
import re
from pathlib import Path
from typing import Optional


class CookieRotationManager:
    """
    Менеджер для ротации куков при ошибках от yt-dlp.
    Thread-safe для параллельной обработки.
    """
    
    def __init__(self, cookies_dir: Optional[str] = None):
        """
        Инициализация менеджера ротации куков.
        
        Args:
            cookies_dir: Путь к директории с файлами куков (если None, используется путь относительно корня проекта)
        """
        if cookies_dir is None:
            # Определяем корень проекта относительно этого файла
            _current_file = os.path.abspath(__file__)
            _project_root = os.path.dirname(os.path.dirname(os.path.dirname(_current_file)))
            cookies_dir = os.path.join(_project_root, "_yt_dlp", "cookies")
        
        self.cookies_dir = Path(cookies_dir)
        self.cookie_files = []
        self.current_index = 0
        
        # Загружаем список файлов куков
        self._load_cookie_files()
        
        if not self.cookie_files:
            print(f"Warning: No cookie files found in {cookies_dir}")
    
    def _load_cookie_files(self):
        """Загружает список файлов куков из директории."""
        if not self.cookies_dir.exists():
            print(f"Warning: Cookies directory {self.cookies_dir} does not exist")
            return
        
        # Ищем все .txt файлы в директории
        cookie_files = sorted([
            str(self.cookies_dir / f)
            for f in os.listdir(self.cookies_dir)
            if f.endswith('.txt')
        ])
        
        self.cookie_files = cookie_files
        print(f"[COOKIE ROTATION] Загружено {len(self.cookie_files)} файлов куков")
    
    def get_current_cookie(self) -> Optional[str]:
        """
        Возвращает текущий файл куков.
        Thread-safe.
        
        Returns:
            Путь к файлу куков или None если куков нет
        """
        if not self.cookie_files:
            return None
        return self.cookie_files[self.current_index]
    
    def rotate_to_next(self) -> Optional[str]:
        """
        Переключается на следующий файл куков.
        Thread-safe.
        
        Returns:
            Путь к следующему файлу куков или None
        """
        if not self.cookie_files:
            return None
        
        # Переключаемся на следующий куки (циклически)
        self.current_index = (self.current_index + 1) % len(self.cookie_files)
        current_cookie = self.cookie_files[self.current_index]
        
        cookie_name = os.path.basename(current_cookie)
        print(f"[COOKIE ROTATION] Переключился на куки: {cookie_name} "
                f"({self.current_index + 1}/{len(self.cookie_files)})")
        
        return current_cookie
    
    def is_blocked_error(self, error: Exception) -> bool:
        """
        Определяет, является ли ошибка блокировкой YouTube.
        
        Args:
            error: Исключение от yt-dlp
            
        Returns:
            True если ошибка указывает на блокировку
        """
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        # Проверяем специфичные признаки блокировки
        block_indicators = [
            '429',  # Too Many Requests
            '403',  # Forbidden
            'blocked',
            'rate limit',
            'too many requests',
            'unable to extract',
            'private video',
            'video unavailable',
            'sign in to confirm your age',
            'http error',
            'unable to download',
            'extractor error'
        ]
        
        # Проверяем тип ошибки
        if error_type in ['ExtractorError', 'DownloadError', 'UnsupportedError']:
            for indicator in block_indicators:
                if indicator in error_str:
                    return True
        
        # Проверяем HTTP коды в сообщении об ошибке
        http_code_match = re.search(r'(\d{3})', error_str)
        if http_code_match:
            code = int(http_code_match.group(1))
            if code in [429, 403, 503]:
                return True
        
        return False