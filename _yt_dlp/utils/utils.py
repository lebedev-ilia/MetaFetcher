import json
import re
import os
import shutil
import tempfile
import random
import time
import glob
import yt_dlp
from typing import Optional

def _parse_json3_to_text(file_path: str) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception:
        return ''

    events = data.get('events') or []
    chunks = []
    for ev in events:
        segs = ev.get('segs') or []
        if not segs:
            continue
        text_parts = []
        for s in segs:
            t = s.get('utf8') or ''
            if t:
                text_parts.append(t)
        if text_parts:
            chunks.append(''.join(text_parts))
    text = ' '.join(chunks)
    # Нормализуем пробелы и переносы строк
    text = re.sub(r"\s+", " ", text, flags=re.UNICODE).strip()
    return text

def _cleanup_paths(paths) -> None:
    """Удаляет файлы по списку путей, игнорируя ошибки."""
    if not paths:
        return
    for p in paths:
        try:
            if not p:
                continue
            if os.path.isfile(p):
                os.remove(p)
            elif os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
        except Exception:
            # Игнорируем любые ошибки удаления
            pass

def _download_subtitles_via_api(url: str, languages: list[str], manual: bool, cookies_file: Optional[str], jitter_range: tuple[float, float] = (0.1, 0.2)):
    """Скачивает субтитры через Python API yt_dlp (без subprocess).
    Возвращает: (lang_to_text: dict, tmpdir_path: str, elapsed_seconds: float, saw_429: bool)
    tmpdir нужно будет удалить после парсинга. Время возвращается суммарное для вызова.
    """
    # Определяем корень проекта относительно этого файла
    _current_file = os.path.abspath(__file__)
    _project_root = os.path.dirname(os.path.dirname(os.path.dirname(_current_file)))
    tmpdir = os.path.join(_project_root, "_yt_dlp", ".tmp")
    saw_429 = False

    class _SilentLogger:
        def debug(self, msg):
            pass
        def warning(self, msg):
            pass
        def error(self, msg):
            nonlocal saw_429
            try:
                if isinstance(msg, str) and ('429' in msg or 'Too Many Requests' in msg):
                    saw_429 = True
            except Exception:
                pass

    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'skip_download': True,
        'writesubtitles': manual,
        'writeautomaticsub': not manual,
        'subtitleslangs': languages,
        'subtitlesformat': 'json3',
        'outtmpl': '%(id)s',
        'paths': {'home': tmpdir},
        # Сетевые настройки
        'socket_timeout': 5,
        'retries': 0,
        'geo_bypass': True,
        'logger': _SilentLogger(),
    }
    if cookies_file:
        ydl_opts['cookiefile'] = cookies_file

    # Небольшой джиттер перед сетевыми запросами, чтобы уменьшить шанс 429
    try:
        delay = random.uniform(*jitter_range) if jitter_range else 0.0
        if delay > 0:
            time.sleep(delay)
    except Exception:
        pass

    t0 = time.perf_counter()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            # download работает со skip_download=True для субтитров
            ydl.download([url])
    except Exception:
        # В случае ошибки вернём пустой результат; tmpdir всё равно удалим позже
        pass

    # Собираем тексты по каждому языку из найденных файлов *.lang.json3
    lang_to_text = {}
    for lang in languages:
        candidates = glob.glob(os.path.join(tmpdir, f"*.{lang}.json3"))
        if not candidates:
            continue
        # Берём первый подходящий
        lang_to_text[lang] = _parse_json3_to_text(candidates[0])
    elapsed = round(time.perf_counter() - t0, 3)
    return lang_to_text, tmpdir, elapsed, saw_429
