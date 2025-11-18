import sys
import os
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from utils._huggingface_uploader import HuggingFaceUploader
from utils._static import CATEGORY_KEYWORDS
import json
import time
import tempfile
import shutil
import logging
from datetime import datetime
from typing import Dict, Set, List, Any, Optional
from collections import defaultdict

# Настройка глобального логгера для записи в файл
_global_logger = logging.getLogger('main_hf')
_global_logger.setLevel(logging.INFO)
if not _global_logger.handlers:
    handler = logging.FileHandler('main_hf.log', encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _global_logger.addHandler(handler)

try:
    from huggingface_hub import HfApi, upload_large_folder, login
    HF_HUB_AVAILABLE = True
except ImportError:
    HF_HUB_AVAILABLE = False
    _global_logger.warning("huggingface_hub is not installed. Install it with: pip install huggingface_hub")


# Константы
FETCHER_RESULTS_DIR = os.path.join(project_root, ".results", "fetcher")
YT_DLP_RESULTS_DIR = os.path.join(project_root, ".results", "fetcher", "yt_dlp")
META_SNAPSHOT_DIR = os.path.join(FETCHER_RESULTS_DIR, "meta_snapshot")
CHECK_INTERVAL = 300  # 300 секунд = 5 минут


def load_category_data(category: str) -> Dict[str, Any]:
    """
    Загружает данные категории из meta_snapshot.
    Возвращает словарь с интервалами и видео.
    """
    category_file = os.path.join(META_SNAPSHOT_DIR, f"{category}.json")
    if not os.path.exists(category_file):
        return {}
    
    try:
        with open(category_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        _global_logger.error(f"Error loading category {category}: {e}")
        return {}


def load_yt_dlp_videos() -> Dict[str, Dict[str, Any]]:
    """
    Загружает все видео из yt_dlp файлов data_{date}.json.
    Возвращает словарь {video_id: data}
    """
    videos = {}
    
    if not os.path.isdir(YT_DLP_RESULTS_DIR):
        return videos
    
    # Загружаем все файлы data_{date}.json
    for file in os.listdir(YT_DLP_RESULTS_DIR):
        if file.startswith("data_") and file.endswith(".json") and file != "progress.json":
            file_path = os.path.join(YT_DLP_RESULTS_DIR, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Структура: {video_id: video_data, "_metadata": {...}}
                for video_id, video_data in data.items():
                    if video_id != "_metadata" and isinstance(video_data, dict):
                        videos[video_id] = video_data
            except Exception as e:
                _global_logger.error(f"Error loading yt_dlp file {file}: {e}")
    
    return videos


def merge_video_data(meta_data: Dict[str, Any], yt_dlp_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Мерджит данные из meta_snapshot и yt_dlp.
    Данные из yt_dlp добавляются к данным из meta_snapshot.
    """
    merged = meta_data.copy()
    
    # Добавляем данные из yt_dlp
    if yt_dlp_data:
        # Добавляем все поля из yt_dlp, которые не конфликтуют с meta_snapshot
        for key, value in yt_dlp_data.items():
            if key not in merged or not merged[key]:
                merged[key] = value
            elif isinstance(merged[key], dict) and isinstance(value, dict):
                # Мерджим вложенные словари
                merged[key] = {**merged[key], **value}
            elif isinstance(merged[key], list) and isinstance(value, list):
                # Объединяем списки
                merged[key] = list(set(merged[key] + value))
    
    return merged


def load_progress_from_hf(uploader: HuggingFaceUploader, snapshot_path: str = "meta_snapshot") -> Set[str]:
    """
    Загружает progress.json из HF репозитория.
    Возвращает множество video_id уже загруженных видео.
    
    Args:
        uploader: HuggingFaceUploader instance
        snapshot_path: Путь к snapshot (meta_snapshot или snapshot_N)
    """
    if not HF_HUB_AVAILABLE:
        return set()
    
    try:
        api = HfApi(token=uploader.token)
        progress_path = f"{snapshot_path}/progress.json"
        
        # Пытаемся скачать файл
        try:
            progress_data = api.hf_hub_download(
                repo_id=uploader.repo_id,
                repo_type=uploader.repo_type,
                filename=progress_path,
                token=uploader.token
            )
            
            with open(progress_data, "r", encoding="utf-8") as f:
                progress = json.load(f)
            
            # Структура progress.json: {"processed_video_ids": [...], "count": N}
            if isinstance(progress, dict) and "processed_video_ids" in progress:
                return set(progress["processed_video_ids"])
            elif isinstance(progress, list):
                return set(progress)
            else:
                return set()
        except Exception as e:
            # Файл не существует или ошибка загрузки
            return set()
    except Exception as e:
        return set()


def get_available_snapshots() -> List[int]:
    """
    Возвращает список доступных snapshot_N директорий.
    """
    snapshots = []
    if not os.path.isdir(FETCHER_RESULTS_DIR):
        return snapshots
    
    for item in os.listdir(FETCHER_RESULTS_DIR):
        if item.startswith("snapshot_") and os.path.isdir(os.path.join(FETCHER_RESULTS_DIR, item)):
            try:
                num = int(item.split("_")[1])
                snapshots.append(num)
            except (ValueError, IndexError):
                continue
    
    return sorted(snapshots)


def load_snapshot_timestamp_files(snapshot_num: int) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Загружает все timestamp файлы из snapshot_N.
    Возвращает словарь {timestamp: {video_id: video_data}}
    """
    snapshot_dir = os.path.join(FETCHER_RESULTS_DIR, f"snapshot_{snapshot_num}")
    timestamp_data = {}
    
    if not os.path.isdir(snapshot_dir):
        return timestamp_data
    
    # Загружаем все файлы timestamp'ов
    for file in os.listdir(snapshot_dir):
        if file.endswith(".json") and file not in ["progress.json", "target2ids.json"]:
            timestamp = file[:-5]  # убираем .json
            file_path = os.path.join(snapshot_dir, file)
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    timestamp_data[timestamp] = data
            except Exception as e:
                _global_logger.error(f"Error loading snapshot_{snapshot_num} file {file}: {e}")
    
    return timestamp_data


def merge_snapshot_timestamp_with_yt_dlp(
    timestamp_data: Dict[str, Dict[str, Any]],
    yt_dlp_videos: Dict[str, Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """
    Мерджит данные timestamp файла с данными yt_dlp.
    Возвращает обновленные данные timestamp.
    """
    merged = timestamp_data.copy()
    
    # Мерджим каждое видео с данными из yt_dlp
    for video_id, video_data in merged.items():
        if isinstance(video_data, dict) and video_id in yt_dlp_videos:
            merged[video_id] = merge_video_data(video_data, yt_dlp_videos[video_id])
    
    return merged


def get_videos_from_category(category_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Извлекает все видео из данных категории.
    Возвращает словарь {video_id: video_data}
    """
    videos = {}
    
    for interval, interval_videos in category_data.items():
        if interval in ["_used_queries", "completed"]:
            continue
        if isinstance(interval_videos, dict):
            for video_id, video_data in interval_videos.items():
                if isinstance(video_data, dict):
                    videos[video_id] = video_data
    
    return videos


def merge_category_with_yt_dlp(
    category_data: Dict[str, Any],
    yt_dlp_videos: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Мерджит данные категории с данными yt_dlp.
    Возвращает обновленные данные категории.
    """
    merged_category = category_data.copy()
    
    # Проходим по всем интервалам
    for interval, interval_videos in merged_category.items():
        if interval in ["_used_queries", "completed"]:
            continue
        if isinstance(interval_videos, dict):
            # Мерджим каждое видео с данными из yt_dlp
            for video_id, video_data in interval_videos.items():
                if isinstance(video_data, dict) and video_id in yt_dlp_videos:
                    interval_videos[video_id] = merge_video_data(
                        video_data,
                        yt_dlp_videos[video_id]
                    )
    
    return merged_category


def upload_category_to_hf(
    uploader: HuggingFaceUploader,
    category: str,
    category_data: Dict[str, Any],
    new_video_ids: Set[str],
    temp_dir: str
) -> bool:
    """
    Загружает файл категории в HF.
    Создает файл meta_snapshot/{category}.json во временной директории.
    """
    try:
        # Создаем путь к файлу категории
        hf_category_path = os.path.join(temp_dir, "meta_snapshot", f"{category}.json")
        os.makedirs(os.path.dirname(hf_category_path), exist_ok=True)
        
        # Сохраняем данные категории
        with open(hf_category_path, "w", encoding="utf-8") as f:
            json.dump(category_data, f, ensure_ascii=False, indent=2)
        
        _global_logger.info(f"  Подготовлен файл {category}.json ({len(new_video_ids)} новых видео)")
        return True
    except Exception as e:
        _global_logger.error(f"  Ошибка подготовки файла {category}.json: {e}")
        return False


def upload_progress_to_hf(
    uploader: HuggingFaceUploader,
    all_uploaded_video_ids: Set[str],
    temp_dir: str,
    snapshot_path: str = "meta_snapshot"
) -> bool:
    """
    Создает progress.json во временной директории с загруженными видео.
    
    Args:
        uploader: HuggingFaceUploader instance
        all_uploaded_video_ids: Множество video_id загруженных видео
        temp_dir: Временная директория для подготовки файлов
        snapshot_path: Путь к snapshot (meta_snapshot или snapshot_N)
    """
    try:
        progress_path = os.path.join(temp_dir, snapshot_path, "progress.json")
        os.makedirs(os.path.dirname(progress_path), exist_ok=True)
        
        progress_data = {
            "processed_video_ids": sorted(list(all_uploaded_video_ids)),
            "count": len(all_uploaded_video_ids)
        }
        
        with open(progress_path, "w", encoding="utf-8") as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
        
        _global_logger.info(f"  Подготовлен {snapshot_path}/progress.json ({len(all_uploaded_video_ids)} видео)")
        return True
    except Exception as e:
        _global_logger.error(f"  Ошибка подготовки {snapshot_path}/progress.json: {e}")
        return False


def process_and_upload(uploader: HuggingFaceUploader):
    """
    Основная функция обработки и загрузки данных.
    Мерджит данные meta_snapshot и yt_dlp, загружает в HF одним коммитом.
    Также обрабатывает snapshot_N директории.
    
    Логика:
    1. Загружает все категории из meta_snapshot
    2. Загружает все данные из yt_dlp
    3. Для каждой категории мерджит данные по videoId
    4. Определяет новые видео (есть в meta_snapshot и yt_dlp, но не в progress.json)
    5. Загружает файлы категорий (meta_snapshot/{category}.json) со всеми видео из meta_snapshot (мердженными с yt_dlp где есть)
    6. Загружает progress.json только с видео, у которых есть данные в yt_dlp
    7. Обрабатывает snapshot_N директории аналогично
    8. Все одним коммитом раз в 300 секунд
    """
    _global_logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Проверка данных...")
    
    # Загружаем данные
    yt_dlp_videos = load_yt_dlp_videos()
    _global_logger.info(f"  yt_dlp: {len(yt_dlp_videos)} видео")
    
    # Создаем временную директорию для загрузки
    temp_dir = tempfile.mkdtemp()
    
    try:
        has_data_to_upload = False
        
        # ========== Обработка meta_snapshot ==========
        _global_logger.info("\n[meta_snapshot] Обработка...")
        
        # Загружаем progress из HF для meta_snapshot
        uploaded_meta_video_ids = load_progress_from_hf(uploader, "meta_snapshot")
        _global_logger.info(f"  Уже загружено в HF: {len(uploaded_meta_video_ids)} видео")
        
        categories_to_upload = []
        all_new_meta_video_ids = set()
        all_uploaded_meta_video_ids = uploaded_meta_video_ids.copy()
        
        # Обрабатываем каждую категорию
        for category in CATEGORY_KEYWORDS.keys():
            category_data = load_category_data(category)
            if not category_data:
                continue
            
            # Извлекаем все видео из категории
            category_videos = get_videos_from_category(category_data)
            if not category_videos:
                continue
            
            _global_logger.info(f"  {category}: {len(category_videos)} видео в meta_snapshot")
            
            # Определяем видео, которые есть и в meta_snapshot и в yt_dlp
            category_video_ids = set(category_videos.keys())
            yt_dlp_video_ids = set(yt_dlp_videos.keys())
            available_video_ids = category_video_ids & yt_dlp_video_ids
            
            # Определяем новые видео (которые еще не загружены)
            new_video_ids = available_video_ids - uploaded_meta_video_ids
            
            # Мерджим данные категории с yt_dlp (мерджим все, где есть данные в yt_dlp)
            merged_category_data = merge_category_with_yt_dlp(category_data, yt_dlp_videos)
            
            # Загружаем файл категории, если есть новые видео для загрузки
            if new_video_ids:
                _global_logger.info(f"  {category}: {len(new_video_ids)} новых видео для загрузки")
                
                # Подготавливаем файл категории для загрузки
                if upload_category_to_hf(uploader, category, merged_category_data, new_video_ids, temp_dir):
                    categories_to_upload.append(category)
                    all_new_meta_video_ids.update(new_video_ids)
                    all_uploaded_meta_video_ids.update(new_video_ids)
                    has_data_to_upload = True
            else:
                _global_logger.info(f"  {category}: нет новых видео для загрузки")
        
        # Подготавливаем progress.json для meta_snapshot
        if categories_to_upload:
            yt_dlp_video_ids = set(yt_dlp_videos.keys())
            uploaded_with_yt_dlp = all_uploaded_meta_video_ids & yt_dlp_video_ids
            upload_progress_to_hf(uploader, uploaded_with_yt_dlp, temp_dir, "meta_snapshot")
        
        # ========== Обработка snapshot_N ==========
        available_snapshots = get_available_snapshots()
        _global_logger.info(f"\n[snapshots] Найдено {len(available_snapshots)} snapshot'ов: {available_snapshots}")
        
        for snapshot_num in available_snapshots:
            _global_logger.info(f"\n[snapshot_{snapshot_num}] Обработка...")
            
            # Загружаем progress из HF для этого snapshot
            uploaded_snapshot_video_ids = load_progress_from_hf(uploader, f"snapshot_{snapshot_num}")
            _global_logger.info(f"  Уже загружено в HF: {len(uploaded_snapshot_video_ids)} видео")
            
            # Загружаем все timestamp файлы из snapshot
            timestamp_files = load_snapshot_timestamp_files(snapshot_num)
            if not timestamp_files:
                _global_logger.info(f"  Нет данных в snapshot_{snapshot_num}")
                continue
            
            _global_logger.info(f"  Найдено {len(timestamp_files)} timestamp файлов")
            
            # Собираем все уникальные video_id из всех timestamp файлов
            all_snapshot_video_ids = set()
            
            for timestamp, timestamp_data in timestamp_files.items():
                timestamp_video_ids = set(timestamp_data.keys())
                all_snapshot_video_ids.update(timestamp_video_ids)
            
            _global_logger.info(f"  Всего уникальных видео в snapshot_{snapshot_num}: {len(all_snapshot_video_ids)}")
            
            # Определяем видео, которые есть и в snapshot и в yt_dlp
            yt_dlp_video_ids = set(yt_dlp_videos.keys())
            available_video_ids = all_snapshot_video_ids & yt_dlp_video_ids
            
            # Определяем новые видео (которые еще не загружены)
            new_video_ids = available_video_ids - uploaded_snapshot_video_ids
            
            if not new_video_ids:
                _global_logger.info(f"  Нет новых видео для загрузки в snapshot_{snapshot_num}")
                continue
            
            _global_logger.info(f"  Новых видео для загрузки: {len(new_video_ids)}")
            
            # Обрабатываем каждый timestamp файл
            all_uploaded_snapshot_video_ids = uploaded_snapshot_video_ids.copy()
            timestamps_to_upload = []
            
            for timestamp, timestamp_data in timestamp_files.items():
                # Определяем новые видео в этом timestamp файле
                timestamp_video_ids = set(timestamp_data.keys())
                new_timestamp_video_ids = (timestamp_video_ids & new_video_ids)
                
                if new_timestamp_video_ids:
                    # Мерджим данные timestamp с yt_dlp только для новых видео
                    merged_timestamp_data = {}
                    
                    # Копируем все видео из timestamp файла
                    for video_id, video_data in timestamp_data.items():
                        if video_id in new_timestamp_video_ids:
                            # Мерджим с yt_dlp для новых видео
                            if video_id in yt_dlp_videos:
                                merged_timestamp_data[video_id] = merge_video_data(
                                    video_data,
                                    yt_dlp_videos[video_id]
                                )
                            else:
                                merged_timestamp_data[video_id] = video_data
                        else:
                            # Для уже загруженных видео просто копируем данные
                            merged_timestamp_data[video_id] = video_data
                    
                    # Подготавливаем файл timestamp для загрузки
                    snapshot_path = os.path.join(temp_dir, f"snapshot_{snapshot_num}", f"{timestamp}.json")
                    os.makedirs(os.path.dirname(snapshot_path), exist_ok=True)
                    
                    with open(snapshot_path, "w", encoding="utf-8") as f:
                        json.dump(merged_timestamp_data, f, ensure_ascii=False, indent=2)
                    
                    _global_logger.info(f"  {timestamp}: {len(new_timestamp_video_ids)} новых видео для загрузки")
                    timestamps_to_upload.append(timestamp)
                    all_uploaded_snapshot_video_ids.update(new_timestamp_video_ids)
                    has_data_to_upload = True
                else:
                    _global_logger.info(f"  {timestamp}: нет новых видео для загрузки")
            
            # Подготавливаем progress.json для snapshot_N
            # Записываем только уникальные video_id, которые были загружены (есть в yt_dlp)
            if timestamps_to_upload:
                yt_dlp_video_ids = set(yt_dlp_videos.keys())
                uploaded_with_yt_dlp = all_uploaded_snapshot_video_ids & yt_dlp_video_ids
                upload_progress_to_hf(uploader, uploaded_with_yt_dlp, temp_dir, f"snapshot_{snapshot_num}")
        
        # Проверяем, есть ли данные для загрузки
        if not has_data_to_upload:
            _global_logger.info("\n  Нет новых данных для загрузки")
            return
        
        # Загружаем все файлы одним коммитом
        _global_logger.info(f"\n  Загрузка всех файлов одним коммитом...")
        try:
            upload_large_folder(
                folder_path=temp_dir,
                repo_id=uploader.repo_id,
                repo_type=uploader.repo_type,
            )
            _global_logger.info(f"  ✓ Успешно загружено")
        except Exception as e:
            error_str = str(e)
            # Проверяем, является ли это ошибкой rate limit
            if "429" in error_str or "rate limit" in error_str.lower() or "Too Many Requests" in error_str:
                _global_logger.warning(f"  Rate limit reached. Error: {e}")
                _global_logger.info("  Waiting 60 seconds before retry...")
                time.sleep(60)
                # Пробуем еще раз
                try:
                    upload_large_folder(
                        folder_path=temp_dir,
                        repo_id=uploader.repo_id,
                        repo_type=uploader.repo_type,
                    )
                    _global_logger.info(f"  ✓ Успешно загружено")
                except Exception as e2:
                    _global_logger.error(f"  Retry failed: {e2}")
                    raise
            else:
                _global_logger.error(f"  Error uploading batch: {e}")
                raise
    
    finally:
        # Удаляем временную директорию
        shutil.rmtree(temp_dir, ignore_errors=True)


def main():
    token = os.getenv("HF_TOKEN")
    
    if not token:
        _global_logger.error("HF_TOKEN environment variable is not set!")
        sys.exit(1)
    
    login(token)
    
    uploader = HuggingFaceUploader(
        repo_id="Ilialebedev/snapshots",
        token=token,
        cache_dir=os.path.join(project_root, ".results", "fetcher", ".cache")
    )
    
    _global_logger.info(f"Инициализирован main_hf.py")
    _global_logger.info(f"  Проверка каждые {CHECK_INTERVAL} секунд")
    _global_logger.info(f"  Репозиторий: {uploader.repo_id}")
    
    while True:
        try:
            process_and_upload(uploader)
            _global_logger.info(f"\n  Ожидание {CHECK_INTERVAL} секунд до следующей проверки...")
            time.sleep(CHECK_INTERVAL)
            
        except KeyboardInterrupt:
            _global_logger.info("\nОстановка...")
            break
        except Exception as e:
            _global_logger.error(f"Ошибка в main loop: {e}")
            import traceback
            _global_logger.error(traceback.format_exc())
            time.sleep(60)  # Ждем минуту перед повтором при ошибке


if __name__ == "__main__":
    main()
