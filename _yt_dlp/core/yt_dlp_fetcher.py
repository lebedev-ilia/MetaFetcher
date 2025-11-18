import os
from typing import Optional
import time
import yt_dlp
from core.cookie_manager import CookieRotationManager
from utils.utils import _download_subtitles_via_api, _cleanup_paths

SUBS_DELAY_SEC = float(os.getenv('SUBS_DELAY_SEC', '0.5'))
RUNTIME_SUBS_DELAY_SEC: Optional[float] = None

def fetch_from_ytdlp(video_url: str, cookie_manager: CookieRotationManager):
    result = {}
    timings = {
        'extract_info_seconds': None,
        'captions_seconds_total': 0.0,
        'captions_per_lang': {},
        'total_seconds': None,
    }
    total_start = time.perf_counter()
    
    # Максимальное количество попыток (один раз для каждого доступного куки)
    max_attempts = max(1, len(cookie_manager.cookie_files)) if cookie_manager.cookie_files else 1
    
    for attempt in range(max_attempts):
        # Получаем текущий куки
        current_cookie = cookie_manager.get_current_cookie()
        
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
        }
        
        # Добавляем куки если есть
        if current_cookie:
            ydl_opts['cookiefile'] = current_cookie
        
        try:
            # Последовательный режим: сначала metadata/info, затем (опционально) сабы
            t0 = time.perf_counter()
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
            timings['extract_info_seconds'] = round(time.perf_counter() - t0, 3)
            
            if not info:
                print(f"[yt-dlp] No info found for video {video_url}")
                return {}
            
            # Базовые поля
            result['webpage_url'] = info.get('webpage_url')
            result['age_limit'] = info.get('age_limit')
            
            
            catptions_start_time = time.perf_counter()
            
            # Единичная попытка авто-субтитров (без ретраев), строго после info
            result['subtitles'] = {}
            result['automatic_captions'] = {}
            languages = ['en', 'ru']
            cookie_file = cookie_manager.get_current_cookie()
            # троттлинг перед попыткой сабов, чтобы снизить риск 429
            delay_val = RUNTIME_SUBS_DELAY_SEC if RUNTIME_SUBS_DELAY_SEC is not None else SUBS_DELAY_SEC
            if delay_val > 0:
                time.sleep(delay_val)
            auto_map, tmpdir_auto, auto_sec, _ = _download_subtitles_via_api(result.get('webpage_url') or video_url, languages, False, cookie_file)

            for lang in languages:
                txt_m = None
                txt_a = auto_map.get(lang) if auto_map else None
                timings['captions_per_lang'][lang] = {
                    'manual_seconds': 0.0,
                    'auto_seconds': auto_sec,
                    'chosen': 'auto' if txt_a else '',
                }
                if txt_a:
                    result['automatic_captions'][lang] = txt_a

            # Чистим временный каталог авто-субтитров
            _cleanup_paths([tmpdir_auto])

            timings['captions_seconds_total'] = round(time.perf_counter() - catptions_start_time, 3)

            result['chapters'] = info.get('chapters')
            
            # После info определяем доступные языки и при необходимости догружаем manual
            # Manual субтитры сейчас не критичны — пропускаем попытки для максимальной скорости

            # Форматы - только полные видео (видео + аудио)
            formats = info.get('formats', [])
            # Фильтруем: оставляем только форматы с видео и аудио
            # Исключаем storyboard, только аудио и только видео форматы
            full_video_formats = []
            for fmt in formats:
                
                # Пропускаем storyboard форматы
                if fmt.get('format_note', '') == 'storyboard':
                    continue
                
                clean_fmt = {}
                
                clean_fmt['vcodec'] = fmt.get('vcodec', 'none')
                clean_fmt['acodec'] = fmt.get('acodec', 'none')
                
                # Оставляем только форматы с видео И аудио (оба не "none")
                if clean_fmt['vcodec'] and clean_fmt['vcodec'] != 'none' and clean_fmt['acodec'] and clean_fmt['acodec'] != 'none':
                
                    clean_fmt['format_id'] = fmt.get('format_id', 'none')
                    clean_fmt['fps'] = fmt.get('fps', 'none')
                    clean_fmt['ext'] = fmt.get('ext', 'none')
                    clean_fmt['video_ext'] = fmt.get('video_ext', 'none')
                    clean_fmt['audio_ext'] = fmt.get('audio_ext', 'none')
                    clean_fmt['resolution'] = fmt.get('resolution', 'none')
                    clean_fmt['format'] = fmt.get('format', 'none')
                    
                    full_video_formats.append(clean_fmt)    
            
            result['formats'] = full_video_formats[-2:]
            
            # Миниатюры - только preference -1
            thumbnails = info.get('thumbnails', [])
            result['thumbnails_ytdlp'] = [thumb for thumb in thumbnails if thumb.get('preference') == -1]
            
            # Длительность
            duration = info.get('duration')
            if duration:
                result['duration_seconds'] = int(duration)
            
            timings['total_seconds'] = round(time.perf_counter() - total_start, 3)
            result['timings_ytdlp'] = timings

            # Успешно получили данные
            return result
            
        except Exception as e:
            print(e)
            error_str = str(e).lower()
            
            # Явно пропускаем недоступные видео (удалено/копирайт/ограничено), без ротации кук
            is_unavailable = any(indicator in error_str for indicator in [
                'video unavailable',
                'this video is no longer available',
                'copyright claim',
                'copyright',
                'unavailable',
                'private video',
                'this video is private',
                'video is private',
                'not available',
                'blocked in your country'
            ])
            if is_unavailable:
                # Возвращаем пустой результат — на уровне вызывающего кода будет считаться EMPTY
                return {}

            # Проверяем таймаут по сообщению об ошибке
            is_timeout = any(indicator in error_str for indicator in [
                'timeout', 'timed out', 'connection timed out', 'socket timeout'
            ])
            
            # Проверяем, является ли ошибка блокировкой
            is_blocked = cookie_manager.is_blocked_error(e)
            
            # Если таймаут или блокировка - пробуем другой куки
            if (is_timeout or is_blocked) and attempt < max_attempts - 1:
                next_cookie = cookie_manager.rotate_to_next()
                if next_cookie:
                    error_type = "Таймаут" if is_timeout else "Блокировка"
                    print(f"[COOKIE ROTATION] {error_type} при запросе, повторяю с новым куки...")
                    continue

            # Если это последняя попытка, выводим ошибку
            if attempt == max_attempts - 1:
                print(f"Error fetching from yt-dlp: {e}")
            return {}
    
    return {}
