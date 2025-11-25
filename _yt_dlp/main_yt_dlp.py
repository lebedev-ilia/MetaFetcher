import json
import re
import os
import sys
import time
import signal
import logging
from datetime import datetime
from typing import Optional, Tuple, Dict, Any, List

# Add project root to path
project_root = "/content/MetaFetcher"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from core.yt_dlp_fetcher import fetch_from_ytdlp
from core.cookie_manager import CookieRotationManager

# Настройка глобального логгера для записи в файл
_global_logger = logging.getLogger('main_yt_dlp')
_global_logger.setLevel(logging.INFO)
if not _global_logger.handlers:
    handler = logging.FileHandler('main_yt_dlp.log', encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _global_logger.addHandler(handler)


COOKIE_MANAGER = CookieRotationManager()

# Путь к sequence.json
SEQUENCE_PATH = os.path.join("/content/drive/MyDrive", ".results", "fetcher", "meta_snapshot", "sequence.json")
# Директория для сохранения результатов yt_dlp
YT_DLP_RESULTS_DIR = os.path.join("/content/drive/MyDrive", ".results", "fetcher", "yt_dlp")
# Прогресс обработки
PROGRESS_PATH = os.path.join(YT_DLP_RESULTS_DIR, "progress.json")
# Размер файла данных (количество видео)
DATA_FILE_SIZE = 500
# Интервал проверки sequence.json на новые видео (в секундах)
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '5'))

# Флаг для корректного завершения
shutdown_requested = False


def video_id_to_url(video_id: str) -> str:
    """Преобразует video_id в YouTube URL."""
    return f"https://www.youtube.com/watch?v={video_id}"


def load_sequence() -> List[str]:
    """
    Загружает sequence.json и возвращает список всех video_id в порядке появления.
    
    Returns:
        Список video_id в порядке появления в sequence.json
    """
    if not os.path.exists(SEQUENCE_PATH):
        _global_logger.warning(f"[yt-dlp] sequence.json not found at {SEQUENCE_PATH}")
        return []
    
    try:
        with open(SEQUENCE_PATH, "r", encoding="utf-8") as f:
            sequence = json.load(f)
        
        # Собираем все video_id в порядке появления (по timestamp'ам)
        video_ids = []
        # Сортируем timestamp'ы для правильного порядка
        sorted_timestamps = sorted(sequence.keys())
        for timestamp in sorted_timestamps:
            video_ids.extend(sequence[timestamp])
        
        _global_logger.info(f"[yt-dlp] Loaded {len(video_ids)} video IDs from sequence.json")
        return video_ids
    except Exception as e:
        _global_logger.warning(f"[yt-dlp] Error loading sequence.json: {e}")
        return []


def load_progress() -> set[str]:
    """
    Загружает прогресс обработки.
    
    Returns:
        Множество обработанных video_id
    """
    if not os.path.exists(PROGRESS_PATH):
        return set()
    
    try:
        with open(PROGRESS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            processed_ids = data.get("processed_video_ids", [])
            return set(processed_ids) if isinstance(processed_ids, list) else set()
    except Exception as e:
        _global_logger.warning(f"[yt-dlp] Error loading progress: {e}")
        return set()


def save_progress(processed_ids: set[str]) -> None:
    """
    Сохраняет прогресс обработки.
    
    Args:
        processed_ids: Множество обработанных video_id
    """
    os.makedirs(YT_DLP_RESULTS_DIR, exist_ok=True)
    try:
        payload = {
            "processed_video_ids": sorted(list(processed_ids)),
            "count": len(processed_ids)
        }
        with open(PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _global_logger.warning(f"[yt-dlp] Error saving progress: {e}")


def get_existing_data_files() -> List[str]:
    """
    Возвращает список существующих файлов data_{date}.json.
    
    Returns:
        Список путей к существующим файлам данных
    """
    if not os.path.isdir(YT_DLP_RESULTS_DIR):
        return []
    
    data_files = []
    for file in os.listdir(YT_DLP_RESULTS_DIR):
        if file.startswith("data_") and file.endswith(".json"):
            data_files.append(os.path.join(YT_DLP_RESULTS_DIR, file))
    
    return sorted(data_files)


def get_next_data_file_path() -> str:
    """
    Определяет путь к следующему файлу данных.
    Если последний файл содержит меньше DATA_FILE_SIZE видео, используем его.
    Иначе создаем новый файл с текущей датой.
    
    Returns:
        Путь к файлу данных
    """
    existing_files = get_existing_data_files()
    
    if existing_files:
        # Проверяем последний файл
        last_file = existing_files[-1]
        try:
            with open(last_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                # Считаем количество видео (исключаем служебные ключи)
                video_count = sum(1 for k, v in data.items() if k != "_metadata" and isinstance(v, dict))
                
                if video_count < DATA_FILE_SIZE:
                    # Используем существующий файл
                    return last_file
        except Exception:
            pass
    
    # Создаем новый файл с текущей датой
    date_str = datetime.now().strftime("%Y-%m-%d")
    # Если файл с такой датой уже существует, добавляем номер
    counter = 1
    while True:
        if counter == 1:
            filename = f"data_{date_str}.json"
        else:
            filename = f"data_{date_str}_{counter}.json"
        
        file_path = os.path.join(YT_DLP_RESULTS_DIR, filename)
        if not os.path.exists(file_path):
            return file_path
        counter += 1


def load_data_file(file_path: str) -> Dict[str, Any]:
    """
    Загружает данные из файла.
    
    Args:
        file_path: Путь к файлу данных
        
    Returns:
        Словарь с данными
    """
    if not os.path.exists(file_path):
        return {"_metadata": {"created_at": datetime.now().isoformat()}}
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        _global_logger.warning(f"[yt-dlp] Error loading data file {file_path}: {e}")
        return {"_metadata": {"created_at": datetime.now().isoformat()}}


def save_data_file(file_path: str, data: Dict[str, Any]) -> None:
    """
    Сохраняет данные в файл.
    
    Args:
        file_path: Путь к файлу данных
        data: Словарь с данными
    """
    os.makedirs(YT_DLP_RESULTS_DIR, exist_ok=True)
    try:
        # Обновляем метаданные
        if "_metadata" not in data:
            data["_metadata"] = {}
        data["_metadata"]["updated_at"] = datetime.now().isoformat()
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _global_logger.warning(f"[yt-dlp] Error saving data file {file_path}: {e}")


def signal_handler(signum, frame):
    """Обработчик сигнала для корректного завершения."""
    global shutdown_requested
    _global_logger.warning(f"\n[yt-dlp] Received signal {signum}. Shutting down gracefully...")
    shutdown_requested = True


def process_videos(video_ids_to_process: List[str], processed_ids: set[str], 
                   current_data_file: str, current_data: Dict[str, Any]) -> Tuple[set[str], str, Dict[str, Any]]:
    """
    Обрабатывает список видео.
    
    Returns:
        Кортеж (processed_ids, current_data_file, current_data)
    """
    batch_size = 5  # Размер батча для сохранения результатов (каждые 5 видео)
    processed_count = 0
    
    for i, video_id in enumerate(video_ids_to_process):
        # Проверяем флаг завершения
        if shutdown_requested:
            _global_logger.info("[yt-dlp] Shutdown requested. Saving progress and exiting...")
            break
        
        video_url = video_id_to_url(video_id)
        
        _global_logger.info(f"[yt-dlp] Processing {i+1}/{len(video_ids_to_process)}: {video_id}")
        
        # Получаем данные через yt-dlp
        data = fetch_from_ytdlp(video_url, COOKIE_MANAGER)
        
        if data:
            # Добавляем video_id в данные для удобства
            current_data[video_id] = data
            
            status = "OK"
            if "timings_ytdlp" in data:
                ext_time = data["timings_ytdlp"].get("extract_info_seconds", 0)
                captions_time = data["timings_ytdlp"].get("captions_seconds_total", 0)
                total_time = data["timings_ytdlp"].get("total_seconds", 0)
                
                _global_logger.info(f"[yt-dlp] {status} | {video_url} | ext_time: {ext_time} | captions_time: {captions_time} | total_time: {total_time}")
            else:
                _global_logger.info(f"[yt-dlp] {status} | {video_url}")
        else:
            _global_logger.warning(f"[yt-dlp] EMPTY | {video_url}")
        
        # Обновляем прогресс
        processed_ids.add(video_id)
        processed_count += 1
        
        # Сохраняем прогресс и файл данных каждые batch_size видео (каждые 5 видео)
        if processed_count % batch_size == 0:
            save_progress(processed_ids)
            _global_logger.info(f"[yt-dlp] Progress saved: {len(processed_ids)} videos processed")
            
            # Сохраняем файл данных
            video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
            save_data_file(current_data_file, current_data)
            _global_logger.info(f"[yt-dlp] Saved data file: {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
        
        # Проверяем, нужно ли перейти к новому файлу (если достигли лимита размера файла)
        video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
        if video_count_in_file >= DATA_FILE_SIZE:
            # Если файл уже был сохранен выше, просто переходим к новому
            if processed_count % batch_size != 0:
                save_data_file(current_data_file, current_data)
                _global_logger.info(f"[yt-dlp] Saved data file (size limit): {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
            
            # Переходим к новому файлу
            current_data_file = get_next_data_file_path()
            current_data = load_data_file(current_data_file)
            _global_logger.info(f"[yt-dlp] Switched to new data file: {os.path.basename(current_data_file)}")
    
    return processed_ids, current_data_file, current_data


def main():
    """Основная функция с динамическим сканированием sequence.json."""
    global shutdown_requested
    
    # Регистрируем обработчики сигналов для корректного завершения
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    _global_logger.info(f"[yt-dlp] Starting dynamic scanner. Scan interval: {SCAN_INTERVAL} seconds")
    _global_logger.info(f"[yt-dlp] Press Ctrl+C to stop gracefully")
    
    # Загружаем прогресс один раз при старте
    processed_ids = load_progress()
    _global_logger.info(f"[yt-dlp] Already processed: {len(processed_ids)} videos")
    
    # Загружаем текущий файл данных
    current_data_file = get_next_data_file_path()
    current_data = load_data_file(current_data_file)
    _global_logger.info(f"[yt-dlp] Using data file: {os.path.basename(current_data_file)}")
    
    last_sequence_mtime = 0  # Время последней модификации sequence.json
    
    # Основной цикл сканирования
    while not shutdown_requested:
        try:
            # Проверяем, существует ли sequence.json и изменился ли он
            if os.path.exists(SEQUENCE_PATH):
                current_mtime = os.path.getmtime(SEQUENCE_PATH)
                
                # Если файл изменился или это первая итерация
                if current_mtime != last_sequence_mtime:
                    last_sequence_mtime = current_mtime
                    
                    # Загружаем sequence.json
                    all_video_ids = load_sequence()
                    
                    if all_video_ids:
                        # Фильтруем уже обработанные видео
                        video_ids_to_process = [vid for vid in all_video_ids if vid not in processed_ids]
                        
                        if video_ids_to_process:
                            _global_logger.info(f"[yt-dlp] Found {len(video_ids_to_process)} new videos to process")
                            
                            # Обрабатываем новые видео
                            processed_ids, current_data_file, current_data = process_videos(
                                video_ids_to_process, processed_ids, current_data_file, current_data
                            )
                            
                            # Сохраняем финальные результаты после обработки батча
                            if current_data:
                                video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
                                if video_count_in_file > 0:
                                    save_data_file(current_data_file, current_data)
                                    _global_logger.info(f"[yt-dlp] Saved data file: {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
                            
                            # Финальное сохранение прогресса
                            save_progress(processed_ids)
                            _global_logger.info(f"[yt-dlp] Progress saved: {len(processed_ids)} videos processed")
                        else:
                            _global_logger.info(f"[yt-dlp] No new videos to process (total in sequence: {len(all_video_ids)}, processed: {len(processed_ids)})")
                    else:
                        _global_logger.warning(f"[yt-dlp] sequence.json is empty or invalid")
                else:
                    # Файл не изменился, просто ждем
                    pass
            else:
                # Файл не существует, ждем его появления
                if last_sequence_mtime == 0:
                    _global_logger.info(f"[yt-dlp] Waiting for sequence.json to appear at {SEQUENCE_PATH}")
                last_sequence_mtime = 0
            
            # Ждем перед следующей проверкой (если не запрошено завершение)
            if not shutdown_requested:
                # Используем sleep с проверкой каждую секунду для более быстрой реакции на сигналы
                for _ in range(SCAN_INTERVAL):
                    if shutdown_requested:
                        break
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            _global_logger.info("\n[yt-dlp] Keyboard interrupt received")
            shutdown_requested = True
            break
        except Exception as e:
            _global_logger.warning(f"[yt-dlp] Error in main loop: {e}")
            import traceback
            _global_logger.warning(traceback.format_exc())
            # Продолжаем работу после ошибки
            time.sleep(SCAN_INTERVAL)
    
    # Финальное сохранение при завершении
    if current_data:
        video_count_in_file = sum(1 for k, v in current_data.items() if k != "_metadata" and isinstance(v, dict))
        if video_count_in_file > 0:
            save_data_file(current_data_file, current_data)
            _global_logger.info(f"[yt-dlp] Saved final data file: {os.path.basename(current_data_file)} ({video_count_in_file} videos)")
    
    save_progress(processed_ids)
    _global_logger.info(f"[yt-dlp] Shutdown complete. Total processed: {len(processed_ids)} videos")


if __name__ == "__main__":
    main()
