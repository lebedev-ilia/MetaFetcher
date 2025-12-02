from ast import Not
import os
from pathlib import Path
import time
import json
import logging
import warnings
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from huggingface_hub import upload_large_folder, login, upload_file
from threading import Lock

import numpy as np

login("")

# Подавляем предупреждение о версии Python от google.api_core
warnings.filterwarnings('ignore', category=FutureWarning, module='google.api_core')

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from utils._static import CATEGORY_KEYWORDS
from utils.urils import extract_tags_from_text, clean_text_from_tags, parse_duration_iso, _is_russian_query

# Настройка глобального логгера для записи в файл
_global_logger = logging.getLogger('fetcher')
_global_logger.setLevel(logging.INFO)
if not _global_logger.handlers:
    handler = logging.FileHandler('fetcher.log', encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    _global_logger.addHandler(handler)

class GlobalComplete(Exception):
    pass

class CompleteSnapshot(Exception):
    pass

class QuotaError(Exception):
    pass

# Thread-safe управление ключами
class KeyManager:
    def __init__(self, keys):
        self.keys = keys
        self.current_key_index = 0
        self.lock = Lock()
        # Thread-local storage для youtube_service
        self.local = threading.local()
        # Версия ключа для инвалидации кэша потоков
        self.key_version = 0
    
    def get_service(self):
        """Получает или создает youtube_service для текущего потока"""
        # Проверяем, нужно ли обновить service (ключ был переключен)
        # Сначала проверяем без блокировки для быстрого пути
        if not hasattr(self.local, 'service') or not hasattr(self.local, 'key_version'):
            # Нужно создать service
            with self.lock:
                # Проверяем, что есть доступные ключи
                if self.current_key_index >= len(self.keys):
                    raise RuntimeError("Все ключи API исчерпаны")
                # Двойная проверка под блокировкой
                if not hasattr(self.local, 'service') or not hasattr(self.local, 'key_version') or self.local.key_version != self.key_version:
                    key = self.keys[self.current_key_index]
                    self.local.service = build('youtube', 'v3', developerKey=key)
                    self.local.key_index = self.current_key_index
                    self.local.key_version = self.key_version
        else:
            # Service существует, проверяем версию
            # Читаем версию под блокировкой для атомарности
            with self.lock:
                current_version = self.key_version
            if self.local.key_version != current_version:
                # Версия изменилась, нужно обновить service
                with self.lock:
                    # Проверяем, что есть доступные ключи
                    if self.current_key_index >= len(self.keys):
                        raise RuntimeError("Все ключи API исчерпаны")
                    # Двойная проверка под блокировкой
                    if self.local.key_version != self.key_version:
                        key = self.keys[self.current_key_index]
                        self.local.service = build('youtube', 'v3', developerKey=key)
                        self.local.key_index = self.current_key_index
                        self.local.key_version = self.key_version
        return self.local.service
    
    def try_switch_key_if_needed(self, current_key_index_in_thread: int) -> bool:
        """Пытается переключить ключ только если он еще не был переключен другим потоком."""
        with self.lock:
            # Если ключ уже был переключен другим потоком (индекс увеличился)
            if self.current_key_index > current_key_index_in_thread:
                return True
            
            # Если текущий индекс совпадает, значит ключ еще не переключен - переключаем
            if self.current_key_index == current_key_index_in_thread:
                self.current_key_index += 1
                if self.current_key_index >= len(self.keys):
                    return False
                # Увеличиваем версию, чтобы все потоки пересоздали service
                self.key_version += 1
                _global_logger.info(f"    [КЛЮЧ ПЕРЕКЛЮЧЕН] Ключ #{self.current_key_index + 1} ({self.keys[self.current_key_index][:10]}...{self.keys[self.current_key_index][-5:]})")
                return True
            
            return False
    
    def get_thread_key_index(self):
        """Получает индекс ключа, который использует текущий поток"""
        if hasattr(self.local, 'key_index'):
            return self.local.key_index
        # Если ключ еще не был создан для потока, возвращаем текущий глобальный индекс
        with self.lock:
            return self.current_key_index
    
    def get_current_key_index(self):
        """Получает текущий индекс ключа (thread-safe)"""
        with self.lock:
            return self.current_key_index

def wait_until_quota_reset():
    """
    Ожидает до обновления квоты YouTube API.
    Квота обновляется в 10:00 по московскому времени (UTC+3).
    """
    # Московское время (UTC+3)
    moscow_tz = timezone(timedelta(hours=3))
    now_moscow = datetime.now(moscow_tz)
    
    # Определяем время обновления квоты (10:00 МСК)
    reset_time_today = now_moscow.replace(hour=11, minute=1, second=0, microsecond=0)
    
    # Выбираем ближайшее время обновления
    if now_moscow < reset_time_today:
        # До 10:00 - ждем до 10:00 сегодня
        reset_time = reset_time_today
    else:
        # После 10:00 - ждем до 10:00 следующего дня
        reset_time = reset_time_today + timedelta(days=1)
    
    # Вычисляем время ожидания
    wait_seconds = (reset_time - now_moscow).total_seconds()
    
    if wait_seconds > 0:
        wait_hours = int(wait_seconds // 3600)
        wait_minutes = int((wait_seconds % 3600) // 60)
        wait_secs = int(wait_seconds % 60)
        
        _global_logger.info(f"\n{'='*60}")
        _global_logger.info(f"Квота исчерпана. Ожидание до обновления квоты...")
        _global_logger.info(f"Текущее время (МСК): {now_moscow.strftime('%Y-%m-%d %H:%M:%S')}")
        _global_logger.info(f"Время обновления квоты (МСК): {reset_time.strftime('%Y-%m-%d %H:%M:%S')}")
        _global_logger.info(f"Ожидание: {wait_hours}ч {wait_minutes}м {wait_secs}с")
        _global_logger.info(f"{'='*60}\n")
        
        # Ожидание с выводом счетчика каждые 10 минут
        remaining_seconds = wait_seconds
        update_interval = 600  # 10 минут в секундах
        
        while remaining_seconds > 0:
            if remaining_seconds > update_interval:
                # Ждем 10 минут
                time.sleep(update_interval)
                remaining_seconds -= update_interval
            else:
                # Ждем оставшееся время
                time.sleep(remaining_seconds)
                remaining_seconds = 0
            
            # Выводим оставшееся время
            if remaining_seconds > 0:
                current_moscow = datetime.now(timezone(timedelta(hours=3)))
                remaining_hours = int(remaining_seconds // 3600)
                remaining_minutes = int((remaining_seconds % 3600) // 60)
                remaining_secs = int(remaining_seconds % 60)
                
                _global_logger.info(f"[{current_moscow.strftime('%H:%M:%S')}] Осталось ждать: {remaining_hours}ч {remaining_minutes}м {remaining_secs}с")
        
        _global_logger.info(f"Ожидание завершено. Квота должна быть обновлена.")
    else:
        _global_logger.info("Квота уже должна быть обновлена.")

class Fetcher():
    def __init__(self, current_key_index: None) -> None:

        # Используем глобальный logger для единообразия
        self.logger = _global_logger
        
        self.KEYS = []
        self.current_key_index = current_key_index if current_key_index else 0
        # Инициализируем KeyManager для thread-safe управления ключами
        self.key_manager = KeyManager(self.KEYS)
        # Устанавливаем начальный индекс ключа в KeyManager
        if self.current_key_index > 0:
            for _ in range(self.current_key_index):
                if self.key_manager.current_key_index < len(self.KEYS) - 1:
                    self.key_manager.current_key_index += 1
                    self.key_manager.key_version += 1
        # Для обратной совместимости создаем youtube_service (но в параллельных методах используется key_manager)
        self.youtube_service = build('youtube', 'v3', developerKey=self.KEYS[self.current_key_index])
        self.RESULTS_PATH = os.path.join("/content/drive/MyDrive", ".results/fetcher")
        os.makedirs(self.RESULTS_PATH, exist_ok=True)
        
        # Thread-safe кэш каналов (инициализируется на уровне снапшота)
        self.channel_cache = {}
        self.channel_cache_lock = Lock()
        
        # Блокировки для каждого channel_id (чтобы избежать дублирующих запросов)
        self.channel_locks = {}
        self.channel_locks_lock = Lock()
        
        # Количество параллельных потоков для обработки
        self.MAX_WORKERS = 5

        self.load_urls_data = True
        self.quota = 0
        self.time_now = datetime.now()
        self.result_final_data = None
        self.INTERVAL_BETWEEN_SNAPSHOTS = 24 * 60 * 60 * 7
        self.first_start = False
        self.temporal = False
        self.snapshot_num = 0  # Устанавливаем по умолчанию для использования в get_snapshot_data
        
        self.tmp_dir = "/content/MetaFetcher/tmp_dir"
        os.makedirs(self.tmp_dir, exist_ok=True)
        
        self.last_commit_time = None
        self.last_progress_commit_time = None

        self.existing_meta_data, self.seq, self.existing_snapshot_data, self.target2ids, self.latest_snapshot_folder = self.get_snapshot_data()
        
        self.existing_meta_ids = set()
        if self.seq:
            for timestamp, vids in self.seq.items():
                for vid in vids:
                    self.existing_meta_ids.add(vid)
            self.logger.info(f"init | existing_meta_ids: {len(self.existing_meta_ids)}")
        else:           
            self.logger.warning("init | existing_meta_ids is empty")
        
        if not self.existing_meta_data and not self.seq and not self.existing_snapshot_data and not self.target2ids:
            self.snapshot_num = 0
            self.first_start = True
        else:
            if self.existing_meta_data:
                self.snapshot_num = 0
            elif self.latest_snapshot_folder == "meta_snapshot":
                self.snapshot_num = 0
            else:
                # Извлекаем номер из "snapshot_N"
                snapshot_num_str = self.latest_snapshot_folder.split("_")[1]
                self.snapshot_num = int(snapshot_num_str) if snapshot_num_str.isdigit() else 0

        self.LIKES_ARR = []
        self.COMMENTS_ARR = []
        self.VIEWS_ARR = []
        self.DURATION_ARR = []  # Массив для хранения duration_seconds
        self.MIN_VIEW_COUNT = 0
        self.MIN_LIKE_COUNT = 0
        self.MIN_COMMENT_COUNT = 0
        self.MAX_DURATION_SECONDS = 1200  # Максимальная длительность видео в секундах
        self.MAX_VIEW_COUNT = float('inf')  # Бесконечность по умолчанию (нет ограничения)
        self.MAX_LIKE_COUNT = float('inf')
        self.MAX_COMMENT_COUNT = float('inf')
        
        # Восстанавливаем массивы и пороги из существующих данных при продолжении meta_snapshot
        if not self.first_start and self.snapshot_num == 0 and self.existing_meta_data:
            self._restore_arrays_and_thresholds()
            
        # Логика фильтрации: 'OR' (хотя бы одна метрика >= порога), 'AND' (все метрики >= порогов), 
        # 'MAJORITY' (хотя бы 2 из 3 метрик >= порогов)
        self.FILTER_LOGIC = 'MAJORITY'  # Рекомендуется для баланса качества и количества
        
        # Логируем выбранную логику фильтрации
        self.logger.info(f"init | Логика фильтрации: {self.FILTER_LOGIC}")

        self.TIME_INTERVALS_NUM_VIDEOS = {
            "less-1day": 900, 
            "1day-1week": 1130, 
            "1week-1month": 1400, 
            "1month-3month": 1640, 
            "3month-6month": 1250, 
            "6month-1year": 1080, 
            "1year-3year": 950, 
            "3year-more": 700
        } 

        self.VIDEOS_PER_CAT = sum(self.TIME_INTERVALS_NUM_VIDEOS.values())
        self.K_BEFORE_FILT = 1
        self._videos_since_last_correction = 0
        # Интервал корректировки порогов (каждые 50 новых видео)
        # Это позволяет быстро адаптироваться к новым данным
        self._correction_interval = 50

        if self.first_start:
            self.snapshot_num = 0
            if not os.path.exists(os.path.join(self.RESULTS_PATH, f"meta_snapshot")):
                os.mkdir(os.path.join(self.RESULTS_PATH, f"meta_snapshot"))
            # Инициализируем progress.json для всех категорий как False (не завершены)
            initial_progress = {cat: False for cat in CATEGORY_KEYWORDS.keys()}
            self._save_progress(initial_progress)
            self.logger.info(f"init | Инициализирован progress.json для {len(initial_progress)} категорий")
            self.current = {cat: self.TIME_INTERVALS_NUM_VIDEOS for cat in CATEGORY_KEYWORDS.keys()}
        else:
            self.current = self.check_not_completed_snapshot()
            # Проверяем, что current не False и не пустой словарь
            if self.current is not False and self.current:
                if self.snapshot_num == 0:
                    self.logger.info(f"init | Продолжаем мета-снапшот")
                else:
                    self.logger.info(f"init | Продолжаем снапшот (№{self.snapshot_num})")
                    self.temporal = True
                    self.logger.info(f"init | Temporal: {self.temporal}")
                    if self.target2ids:
                        l = len(self.target2ids)
                        self.logger.info(f"Len target2ids: {l}")
                    else:
                        self.logger.info(f"Создаем target2ids...")
                        self.target2ids = self._create_target2ids()
                        l = len(self.target2ids)
                        self.logger.info(f"Len target2ids: {l}")
            else:
                self.temporal = True
                self.logger.info(f"init | Temporal: {self.temporal}")
                self.snapshot_num += 1
                if self.snapshot_num > 3:
                    self.logger.info(f"init | GlobalComplete")
                    raise GlobalComplete
                self.logger.info(f"init | Создаем новый снапшот snapshot_{self.snapshot_num}")
                os.makedirs(os.path.join(self.RESULTS_PATH, f"snapshot_{self.snapshot_num}"), exist_ok=True)
                self.target2ids = self._create_target2ids()
                self.current = self.target2ids



    def _get_category_file_path(self, category: str) -> str:
        """
        Возвращает путь к файлу данных категории (для meta_snapshot) или timestamp (для snapshot_).
        
        Args:
            category: Название категории (для meta_snapshot) или timestamp (для snapshot_)
            
        Returns:
            Путь к файлу данных категории или timestamp
        """
        if self.snapshot_num == 0:
            return os.path.join(self.RESULTS_PATH, "meta_snapshot", f"{category}.json")
        else:
            return f"Ilialebedev/snapshot_{self.snapshot_num}", os.path.join(self.tmp_dir, f"{category}.json")
    
    def _get_progress_file_path(self) -> str:
        """
        Возвращает путь к файлу прогресса.
        
        Returns:
            Путь к файлу progress.json
        """
        if self.snapshot_num == 0:
            return os.path.join(self.RESULTS_PATH, "meta_snapshot", "progress.json")
        else:
            return f"Ilialebedev/snapshot_{self.snapshot_num}", os.path.join("/content/MetaFetcher", "progress.json")
    
    def _load_progress(self) -> dict:
        repo, path = self._get_progress_file_path()
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    progress = json.load(f)
                if self.snapshot_num == 0:
                    self.logger.info(f"_load_progress | Загружен прогресс: {len(progress)} категорий")
                else:
                    self.logger.info(f"_load_progress | Загружен прогресс: {len(progress)} timestamp'ов")
                return progress
            except Exception as e:
                self.logger.warning(f"_load_progress | Exception | {e}")
                return {}
        return {}
    
    def _save_progress(self, progress: dict) -> None:
        repo, path = self._get_progress_file_path()
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(progress, f, ensure_ascii=False, indent=4)
                
            t = time.time()
            
            if self.last_progress_commit_time:
                if t - self.last_progress_commit_time > 54:
                    upload_file(
                        path_or_fileobj=path,
                        repo_id=repo,
                        repo_type="dataset",
                    )
                    self.last_progress_commit_time = t
                    self.logger.info("Обновлен файл прогресса")
                    return
                    
            self.logger.info(f"Файл прогресса не обновлен так как прошло {t - self.last_progress_commit_time} < 54 сек")
            
        except Exception as e:
            self.logger.warning(f"_save_progress | Exception | {e}")
    
    def _load_category_data(self, category: str) -> dict:
        """
        Загружает данные категории (для meta_snapshot) или timestamp (для snapshot_) из файла.
        
        Args:
            category: Название категории (для meta_snapshot) или timestamp (для snapshot_)
            
        Returns:
            Словарь с данными категории или timestamp, или None
        """
        category_path = self._get_category_file_path(category)
        if os.path.exists(category_path):
            try:
                with open(category_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if self.snapshot_num == 0:
                    self.logger.info(f"_load_category_data | Загружены данные категории {category}: {len(data.get('_used_queries', []))} использованных запросов")
                else:
                    video_count = len(data) if isinstance(data, dict) else 0
                    self.logger.info(f"_load_category_data | Загружены данные timestamp {category}: {video_count} видео")
                return data
            except json.JSONDecodeError as e:
                self.logger.warning(f"_load_category_data | JSONDecodeError | Файл {category_path} поврежден: {e}")
                return None
            except Exception as e:
                self.logger.warning(f"_load_category_data | Exception | {e}")
                return None
        return None
    
    def _save_category_data(self, category: str, data: dict) -> None:
        """
        Сохраняет данные категории (для meta_snapshot) или timestamp (для snapshot_) в файл.
        
        Args:
            category: Название категории (для meta_snapshot) или timestamp (для snapshot_)
            data: Словарь с данными категории или timestamp
        """
        try:
            repo, path = self._get_category_file_path(category)
            
            t = time.time()
            
            if self.last_commit_time:
                if t - self.last_commit_time > 54:
                    
                    upload_large_folder(
                        folder_path=self.tmp_dir,
                        repo_id=repo,
                        repo_type="dataset",
                    )
                    self.last_commit_time = t
                    
                    self.logger.info(f"_save_category_data | Результаты загружены в HF: {len(os.listdir(self.tmp_dir))}")
                    
                    for file in os.listdir(self.tmp_dir):
                        os.remove(f"{self.tmp_dir}/{file}")
                    
                    return
                
            self.logger.info(f"_save_category_data | Результаты не загружены в HF: {len(os.listdir(self.tmp_dir))} | time: {t - self.last_commit_time}")
            
            with open(path, 'w') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
        except Exception as e:
            self.logger.warning(f"_save_category_data | Exception | {e}")
    
    def _load_all_categories_data(self) -> dict:
        """
        Загружает данные всех категорий для восстановления массивов.
        
        Returns:
            Словарь со всеми данными категорий {category: data}
        """
        all_data = {}
        progress = self._load_progress()
        
        # Загружаем все категории из CATEGORY_KEYWORDS
        for category in CATEGORY_KEYWORDS.keys():
            category_data = self._load_category_data(category)
            if category_data:
                all_data[category] = category_data
        
        self.logger.info(f"_load_all_categories_data | Загружено категорий: {len(all_data)}")
        return all_data

    def save_sequence(self, timestamp: str, vids: list) -> dict:
        sequence_path = os.path.join(self.RESULTS_PATH, "meta_snapshot", "sequence.json")
        sequence = {}

        if os.path.exists(sequence_path):
            try:
                with open(sequence_path, "r") as f:
                    sequence = json.load(f)
            except Exception as e:
                self.logger.warning(f"save_sequence | Exception | {e}")
                sequence = {}

        if not vids:
            return sequence

        time_now = datetime.now()
        target_timestamp = timestamp

        if not sequence:
            sequence[target_timestamp] = []
        else:
            last_timestamp_str = list(sequence.keys())[-1]
            last_timestamp = datetime.strptime(last_timestamp_str, "%Y_%m_%d_%H_%M")

            if (last_timestamp + timedelta(seconds=60)) < time_now:
                sequence[target_timestamp] = []
            else:
                target_timestamp = last_timestamp_str
                sequence.setdefault(target_timestamp, [])

        existing_ids = set(sequence[target_timestamp])
        for vid in vids:
            if vid not in existing_ids:
                sequence[target_timestamp].append(vid)
                existing_ids.add(vid)

        with open(sequence_path, "w") as f:
            json.dump(sequence, f, ensure_ascii=False, indent=4)

        return sequence

    def save_progress(self, results: dict, category: str = None, timestamp: str = None) -> None:
        """
        Сохраняет прогресс. Для meta_snapshot (snapshot_num == 0) сохраняет данные категории.
        Для temporal snapshots сохраняет данные timestamp в отдельный файл.
        
        Args:
            results: Словарь с результатами (для meta_snapshot содержит все категории, для temporal - timestamp -> videos)
            category: Название категории (обязательно для meta_snapshot)
            timestamp: Timestamp (обязательно для snapshot_, если не указан, берется из results)
        """
        if self.snapshot_num == 0:
            # Для meta_snapshot сохраняем только данные указанной категории
            if category is None:
                self.logger.warning("save_progress | category не указана для meta_snapshot")
                return
            
            if category not in results:
                self.logger.warning(f"save_progress | Категория {category} не найдена в results")
                return
            
            # Сохраняем данные категории
            self._save_category_data(category, results[category])
            
            # Обновляем progress.json
            progress = self._load_progress()
            # Проверяем, завершена ли категория
            is_completed = results[category].get("completed", False)
            progress[category] = is_completed
            self._save_progress(progress)
        else:
            # Для temporal snapshots сохраняем каждый timestamp в отдельный файл
            if timestamp is None:
                # Если timestamp не указан, берем первый ключ из results
                if not results:
                    self.logger.warning("save_progress | results пуст для snapshot_")
                    return
                timestamp = list(results.keys())[0]
            
            if timestamp not in results:
                self.logger.warning(f"save_progress | Timestamp {timestamp} не найден в results")
                return
            
            # Сохраняем данные timestamp
            self._save_category_data(timestamp, results[timestamp])
            
            # Обновляем progress.json
            progress = self._load_progress()
            progress[timestamp] = True
            self._save_progress(progress)

    def check_not_completed_snapshot(self) -> bool:
        if self.snapshot_num == 0:
            self.logger.info(f"check_not_completed_snapshot | meta_snapshot")
            # Новая архитектура: работаем с progress.json и отдельными файлами категорий
            progress = self._load_progress()
            cats = {}
            c_m = 0
            _m = 0
            all_completed = True
            
            # Проверяем все категории из CATEGORY_KEYWORDS
            for cat in CATEGORY_KEYWORDS.keys():
                is_completed_in_progress = progress.get(cat, False)
                
                # Загружаем данные категории для проверки
                category_data = self._load_category_data(cat)
                
                # Определяем, действительно ли категория завершена
                is_really_completed = False
                if category_data:
                    # Проверяем флаг completed
                    is_really_completed = category_data.get("completed", False)
                    
                    # Если флага нет, проверяем, все ли интервалы заполнены
                    if not is_really_completed:
                        all_intervals_complete = True
                        for interval in self.TIME_INTERVALS_NUM_VIDEOS.keys():
                            videos = category_data.get(interval, {})
                            video_count = len(videos) if isinstance(videos, dict) else 0
                            if video_count < self.TIME_INTERVALS_NUM_VIDEOS[interval]:
                                all_intervals_complete = False
                                break
                        
                        if all_intervals_complete:
                            # Все интервалы заполнены - категория завершена
                            is_really_completed = True
                            category_data["completed"] = True
                            self._save_category_data(cat, category_data)
                            # Обновляем progress.json
                            progress[cat] = True
                            self._save_progress(progress)
                            self.logger.info(f"check_not_completed_snapshot | Категория {cat} завершена (все интервалы заполнены)")
                
                # Если в progress помечена как завершенная, но на самом деле не завершена - исправляем
                if is_completed_in_progress and not is_really_completed:
                    self.logger.warning(f"check_not_completed_snapshot | Категория {cat} помечена как завершенная в progress, но не завершена. Исправляем.")
                    progress[cat] = False
                    self._save_progress(progress)
                    all_completed = False
                
                # Если категория действительно завершена
                if is_really_completed:
                    cats[cat] = "completed"
                    # Обновляем progress, если там было False
                    if not is_completed_in_progress:
                        progress[cat] = True
                        self._save_progress(progress)
                    continue
                
                # Категория не завершена
                all_completed = False
                
                if category_data:
                    # Категория не завершена - проверяем интервалы
                    cats[cat] = {}
                    c_m += 1
                    for interval, videos in category_data.items():
                        if interval == "_used_queries":
                            cats[cat][interval] = videos
                        elif interval == "completed":
                            continue
                        elif interval in self.TIME_INTERVALS_NUM_VIDEOS:
                            video_count = len(videos) if isinstance(videos, dict) else 0
                            if video_count < self.TIME_INTERVALS_NUM_VIDEOS[interval]:
                                m = self.TIME_INTERVALS_NUM_VIDEOS[interval] - video_count
                                cats[cat][interval] = m
                                _m += m
                else:
                    # Категория не найдена - начинаем с нуля
                    cats[cat] = {}
                    c_m += 1
                    # Инициализируем все интервалы как незавершенные
                    for interval in self.TIME_INTERVALS_NUM_VIDEOS.keys():
                        cats[cat][interval] = self.TIME_INTERVALS_NUM_VIDEOS[interval]
                        _m += self.TIME_INTERVALS_NUM_VIDEOS[interval]
            
            if all_completed:
                self.logger.info(f"check_not_completed_snapshot | Все категории завершены")
                return False
            
            self.logger.info(f"check_not_completed_snapshot | Недостающих категорий: {c_m} | Всего недостающих видео: {_m}")
            return cats
        else:
            self.logger.info(f"check_not_completed_snapshot | snapshot_{self.snapshot_num}")
            if not self.target2ids:
                self.target2ids = self._create_target2ids()
                self.logger.info(f"check_not_completed_snapshot | target2ids был создан | Length: {len(self.target2ids)}")
            else:
                self.logger.info(f"check_not_completed_snapshot | target2ids существует | Length: {len(self.target2ids)}")

            # Новая архитектура: работаем с progress.json и отдельными файлами timestamp'ов
            progress = self._load_progress()
            missing = {}
            all_completed = True
            
            for timestamp, expected_ids in self.target2ids.items():
                is_completed_in_progress = progress.get(timestamp, False)
                
                if not is_completed_in_progress:
                    if all_completed:
                        all_completed = False
                    missing[timestamp] = expected_ids
                    self.logger.info(f"check_not_completed_snapshot | timestamp: {timestamp} в progress = False")
            
            if all_completed:
                self.logger.info(f"check_not_completed_snapshot | Все timestamp'ы завершены")
                return False
            
            return missing

    def _create_target2ids(self) -> dict:
        # Создаем директорию snapshot, если её нет
        snapshot_dir = os.path.join(self.RESULTS_PATH, f"snapshot_{self.snapshot_num}")
        os.makedirs(snapshot_dir, exist_ok=True)
        
        target2ids = {}
        for timestamp, vids in self.seq.items():
            target_time = datetime.strftime(datetime.strptime(timestamp, "%Y_%m_%d_%H_%M") + \
                timedelta(seconds=self.INTERVAL_BETWEEN_SNAPSHOTS*self.snapshot_num), "%Y_%m_%d_%H_%M")

            target2ids[target_time] = vids

        with open(os.path.join(snapshot_dir, "target2ids.json"), "w") as f:
            json.dump(target2ids, f, ensure_ascii=False, indent=4)
        
        # Инициализируем progress.json для всех timestamp'ов как False (не завершены)
        progress_path = os.path.join(snapshot_dir, "progress.json")
        if not os.path.exists(progress_path):
            initial_progress = {timestamp: False for timestamp in target2ids.keys()}
            with open(progress_path, "w", encoding="utf-8") as f:
                json.dump(initial_progress, f, ensure_ascii=False, indent=4)
            self.logger.info(f"_create_target2ids | Инициализирован progress.json для {len(initial_progress)} timestamp'ов")
        
        return target2ids

    def get_snapshot_data(self, s=True):
        existing_meta_data = None
        seq = None
        existing_snapshot_data = None
        target2ids = None

        l = len(sorted([file for file in os.listdir(os.path.join("/content/drive/MyDrive", ".results/fetcher")) if file.startswith("snapshot_") or file == "meta_snapshot"]))
        
        if l == 0:
            return None, None, None, None, None
        elif l == 1:
            latest_snapshot_folder = "meta_snapshot"
        else:
            latest_snapshot_folder = f"snapshot_{l-1}"

        if latest_snapshot_folder == "meta_snapshot":
            # Новая архитектура: читаем progress.json и определяем текущую категорию
            # Временно сохраняем snapshot_num и устанавливаем 0 для meta_snapshot
            temp_snapshot_num = self.snapshot_num
            self.snapshot_num = 0  # Для meta_snapshot всегда 0
            
            progress = self._load_progress()
            
            # Определяем первую незавершенную категорию
            current_category = None
            for category in CATEGORY_KEYWORDS.keys():
                if not progress.get(category, False):  # False или отсутствует = не завершена
                    current_category = category
                    break
            
            if current_category:
                # Загружаем данные только текущей категории
                category_data = self._load_category_data(current_category)
                if category_data:
                    # Создаем структуру, совместимую со старой архитектурой
                    existing_meta_data = {current_category: category_data}
                    self.logger.info(f"get_snapshot_data | Загружены данные категории {current_category} из новой архитектуры")
                else:
                    # Категория не найдена, но она в progress - возможно файл был удален
                    self.logger.warning(f"get_snapshot_data | Категория {current_category} в progress, но файл данных не найден")
            else:
                # Все категории завершены или progress пуст
                if progress:
                    self.logger.info(f"get_snapshot_data | Все категории завершены согласно progress.json")
                else:
                    self.logger.info(f"get_snapshot_data | progress.json не найден или пуст - первый запуск")
            
            # Восстанавливаем snapshot_num
            self.snapshot_num = temp_snapshot_num
            
            # Обратная совместимость: проверяем старый data.json
            old_data_path = os.path.join(self.RESULTS_PATH, "meta_snapshot", "data.json")
            if os.path.exists(old_data_path) and not existing_meta_data:
                try:
                    with open(old_data_path, "r") as f:
                        existing_meta_data = json.load(f)
                    self.logger.info(f"get_snapshot_data | Получены данные из старого data.json (обратная совместимость): {len(existing_meta_data)}")
                except json.JSONDecodeError as e:
                    self.logger.warning(f"get_snapshot_data | JSONDecodeError | Файл meta_snapshot/data.json поврежден: {e}")
                    existing_meta_data = None

        if s:
            if os.path.exists(os.path.join(self.RESULTS_PATH, "meta_snapshot", "sequence.json")):
                with open(os.path.join(self.RESULTS_PATH, "meta_snapshot", "sequence.json"), "r") as f:
                    seq = json.load(f)
                self.logger.info(f"get_snapshot_data | Получены данные из sequence: {len(seq)}")

        if os.path.exists(os.path.join(self.RESULTS_PATH, latest_snapshot_folder, "target2ids.json")):
            try:
                with open(os.path.join(self.RESULTS_PATH, latest_snapshot_folder, "target2ids.json"), "r") as f:
                    target2ids = json.load(f)
                self.logger.info(f"get_snapshot_data | Получены данные из target2ids: {len(target2ids)}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"get_snapshot_data | JSONDecodeError | Файл meta_snapshot/target2ids.json поврежден: {e}")
                target2ids = None

        if latest_snapshot_folder != "meta_snapshot":
            # Новая архитектура: загружаем данные из отдельных файлов timestamp'ов
            snapshot_num_for_load = int(latest_snapshot_folder.split("_")[-1])
            
            # Временно сохраняем snapshot_num и устанавливаем правильный для загрузки данных
            temp_snapshot_num = self.snapshot_num
            self.snapshot_num = snapshot_num_for_load
            
            # Загружаем progress.json для получения списка timestamp'ов
            progress_path = os.path.join(self.RESULTS_PATH, latest_snapshot_folder, "progress.json")
            existing_snapshot_data = {}
            timestamps_to_load = []
            
            if os.path.exists(progress_path):
                try:
                    with open(progress_path, "r", encoding="utf-8") as f:
                        progress = json.load(f)
                    timestamps_to_load = list(progress.keys())
                except json.JSONDecodeError as e:
                    self.logger.warning(f"get_snapshot_data | JSONDecodeError | Файл {progress_path} поврежден: {e}")
                except Exception as e:
                    self.logger.warning(f"get_snapshot_data | Exception | {e}")
            
            # Если progress.json не существует или пуст, загружаем timestamp'ы из target2ids.json
            if not timestamps_to_load:
                target2ids_path = os.path.join(self.RESULTS_PATH, latest_snapshot_folder, "target2ids.json")
                if os.path.exists(target2ids_path):
                    try:
                        with open(target2ids_path, "r", encoding="utf-8") as f:
                            target2ids = json.load(f)
                        timestamps_to_load = list(target2ids.keys())
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"get_snapshot_data | JSONDecodeError | Файл {target2ids_path} поврежден: {e}")
                    except Exception as e:
                        self.logger.warning(f"get_snapshot_data | Exception | {e}")
            
            # Загружаем данные всех timestamp'ов из отдельных файлов
            for timestamp in timestamps_to_load:
                timestamp_data = self._load_category_data(timestamp)
                if timestamp_data:
                    existing_snapshot_data[timestamp] = timestamp_data
            
            if existing_snapshot_data:
                self.logger.info(f"get_snapshot_data | Получены данные из последнего snapshot (№{snapshot_num_for_load}): {len(existing_snapshot_data)} timestamp'ов")
            else:
                existing_snapshot_data = None
            
            # Восстанавливаем snapshot_num
            self.snapshot_num = temp_snapshot_num
            
            # Обратная совместимость: проверяем старый data.json
            if not existing_snapshot_data:
                old_data_path = os.path.join(self.RESULTS_PATH, latest_snapshot_folder, "data.json")
                if os.path.exists(old_data_path):
                    try:
                        with open(old_data_path, "r") as f:
                            existing_snapshot_data = json.load(f)
                        self.logger.info(f"get_snapshot_data | Получены данные из старого data.json (обратная совместимость): {len(existing_snapshot_data)}")
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"get_snapshot_data | JSONDecodeError | Файл {latest_snapshot_folder}/data.json поврежден: {e}")
                        existing_snapshot_data = None
            
            # Устанавливаем snapshot_num для дальнейшего использования
            self.snapshot_num = snapshot_num_for_load

        return existing_meta_data, seq, existing_snapshot_data, target2ids, latest_snapshot_folder

    def _add_quota(self, quota: int) -> None:
        self.quota += quota

    def _switch_to_next_key(self) -> bool:
        """Переключает на следующий ключ (для обратной совместимости с непараллельными методами)"""
        self.current_key_index += 1
        if self.current_key_index >= len(self.KEYS):
            return False
        self.youtube_service = build('youtube', 'v3', developerKey=self.KEYS[self.current_key_index])
        # Синхронизируем с KeyManager
        if self.key_manager.current_key_index < self.current_key_index:
            self.key_manager.current_key_index = self.current_key_index
            self.key_manager.key_version += 1
        return True
    
    def get_channel_lock(self, channel_id: str) -> Lock:
        """Получает или создает блокировку для конкретного channel_id"""
        with self.channel_locks_lock:
            if channel_id not in self.channel_locks:
                self.channel_locks[channel_id] = Lock()
            return self.channel_locks[channel_id]

    def _check_http_error_parallel(self, error: HttpError, retry_count: int = 0) -> tuple:
        """
        Обрабатывает HTTP ошибки для параллельных методов с использованием KeyManager.
        """
        error_code = error.resp.status if hasattr(error.resp, 'status') else None
        error_message = str(error)
        error_lower = error_message.lower()
        
        # Проверяем reason в деталях ошибки
        error_reason = None
        try:
            if hasattr(error, 'error_details') and error.error_details:
                for detail in error.error_details:
                    if 'reason' in detail:
                        error_reason = detail['reason'].lower()
        except:
            pass

        if error_code == 404:
            return False, retry_count
        elif error_code == 403 and error_reason == 'commentsdisabled':
            return False, retry_count
        
        # Специфичные reason для ошибок квоты и доступа в YouTube API
        quota_reasons = ['quotaexceeded', 'dailylimitexceeded', 'userratelimitexceeded']
        access_reasons = ['accessnotconfigured', 'forbidden']
        suspended_reasons = ['suspended', 'accountdisabled']
        
        is_quota_error = error_code == 403 and error_reason in quota_reasons
        is_access_error = error_code == 403 and error_reason in access_reasons
        is_suspended_error = error_code == 403 and error_reason in suspended_reasons
        is_rate_limit = error_code == 429
        
        is_quota_by_text = error_code == 403 and ('quota' in error_lower or 'exceeded' in error_lower) and error_reason not in ['commentsdisabled', 'forbidden']
        is_access_by_text = error_code == 403 and ('has not been used' in error_lower or 'is disabled' in error_lower or 'accessnotconfigured' in error_lower)
        is_suspended_by_text = 'suspended' in error_lower
        
        is_key_error = is_quota_error or is_access_error or is_suspended_error or is_rate_limit or is_quota_by_text or is_access_by_text or is_suspended_by_text
        
        if is_key_error:
            if is_suspended_error or is_suspended_by_text:
                error_type = "заблокирован (suspended)"
            elif is_quota_error or is_rate_limit or is_quota_by_text:
                error_type = "квота исчерпана"
            elif is_access_error or is_access_by_text:
                error_type = "API не включен"
            else:
                error_type = "ошибка ключа"
            
            # Получаем индекс ключа, который использовал текущий поток
            thread_key_index = self.key_manager.get_thread_key_index()
            
            if 0 <= thread_key_index < len(self.KEYS):
                current_key = self.KEYS[thread_key_index]
                key_id = f"{current_key[:10]}...{current_key[-5:]}" if len(current_key) > 15 else current_key
                self.logger.warning(f"    [ОШИБКА КЛЮЧА] Тип: {error_type} | Ключ #{thread_key_index + 1} ({key_id}) | Reason: {error_reason or 'не указан'}")
            else:
                self.logger.warning(f"    [ОШИБКА КЛЮЧА] Тип: {error_type} | Ключ #{thread_key_index + 1} (индекс вне диапазона) | Reason: {error_reason or 'не указан'}")
            
            # Пытаемся переключиться на следующий ключ
            if self.key_manager.try_switch_key_if_needed(thread_key_index):
                retry_count += 1
                self.logger.warning(f"    Повторная попытка с новым ключом (попытка {retry_count})")
                return True, retry_count
            else:
                self.logger.warning("Возвращаем результаты - все ключи исчерпаны")
                return False, retry_count
        else:
            self.logger.warning(f"    Ошибка при запросе (не связана с ключом): {error_code} | Reason: {error_reason or 'не указан'} | {error}")
            return False, retry_count

    def check_http_error(self, error: HttpError, retry_count: int = 0) -> bool:
        # Проверяем ошибку квоты, заблокированного ключа или отключенного API
        error_code = error.resp.status if hasattr(error.resp, 'status') else None
        error_message = str(error)
        error_lower = error_message.lower()
        
        # Проверяем reason в деталях ошибки
        error_reason = None
        try:
            if hasattr(error, 'error_details') and error.error_details:
                for detail in error.error_details:
                    if 'reason' in detail:
                        error_reason = detail['reason'].lower()
        except:
            pass

        if error_code == 404:
            return False, retry_count
        # Обрабатываем ошибку 403 с reason 'commentsDisabled' (комментарии отключены)
        elif error_code == 403 and error_reason == 'commentsdisabled':
            return False, retry_count
        
        # Специфичные reason для ошибок квоты и доступа в YouTube API
        quota_reasons = ['quotaexceeded', 'dailylimitexceeded', 'userratelimitexceeded']
        access_reasons = ['accessnotconfigured', 'forbidden']
        suspended_reasons = ['suspended', 'accountdisabled']
        
        # Проверяем, является ли это ошибкой ключа (квота, доступ, блокировка)
        is_quota_error = error_code == 403 and error_reason in quota_reasons
        is_access_error = error_code == 403 and error_reason in access_reasons
        is_suspended_error = error_code == 403 and error_reason in suspended_reasons
        is_rate_limit = error_code == 429  # 429 всегда означает rate limit
        
        # Также проверяем по тексту сообщения (на случай, если reason не указан)
        is_quota_by_text = error_code == 403 and ('quota' in error_lower or 'exceeded' in error_lower) and error_reason not in ['commentsdisabled', 'forbidden']
        is_access_by_text = error_code == 403 and ('has not been used' in error_lower or 'is disabled' in error_lower or 'accessnotconfigured' in error_lower)
        is_suspended_by_text = 'suspended' in error_lower
        
        is_key_error = is_quota_error or is_access_error or is_suspended_error or is_rate_limit or is_quota_by_text or is_access_by_text or is_suspended_by_text
        
        if is_key_error:
            # Определяем тип ошибки для логирования
            if is_suspended_error or is_suspended_by_text:
                error_type = "заблокирован (suspended)"
            elif is_quota_error or is_rate_limit or is_quota_by_text:
                error_type = "квота исчерпана"
            elif is_access_error or is_access_by_text:
                error_type = "API не включен"
            else:
                error_type = "ошибка ключа"
            
            # Безопасное получение информации о ключе
            if 0 <= self.current_key_index < len(self.KEYS):
                current_key = self.KEYS[self.current_key_index]
                key_id = f"{current_key[:10]}...{current_key[-5:]}" if len(current_key) > 15 else current_key
                self.logger.warning(f"    [ОШИБКА КЛЮЧА] Тип: {error_type} | Ключ #{self.current_key_index + 1} ({key_id}) | Reason: {error_reason or 'не указан'}")
            else:
                self.logger.warning(f"    [ОШИБКА КЛЮЧА] Тип: {error_type} | Ключ #{self.current_key_index + 1} (индекс вне диапазона) | Reason: {error_reason or 'не указан'}")
                return False, retry_count
            
            # Пытаемся переключиться на следующий ключ
            if self._switch_to_next_key():
                retry_count += 1
                self.logger.warning(f"    Повторная попытка с новым ключом (попытка {retry_count})")
                return True, retry_count
            else:
                self.logger.warning("Возвращаем результаты")
                return False, retry_count
        else:
            # Другая ошибка HTTP - не переключаем ключ
            self.logger.warning(f"    Ошибка при поиске (не связана с ключом): {error_code} | Reason: {error_reason or 'не указан'} | {error}")
            return False, retry_count

    def search_videos_with_pagination(
        self, 
        query: str, 
        published_after = None, 
        max_results = 50,
        max_pages = 10
    ) -> list:
        """
        Ищет видео с пагинацией.
        
        Args:
            query: Поисковый запрос
            published_after: Минимальная дата публикации
            max_results: Максимальное количество результатов
            
        Returns:
            Список видео
        """
        responses = []
        quota_cost = 0

        # Определяем язык запроса и устанавливаем соответствующие параметры
        is_russian = _is_russian_query(query)
        relevance_language = 'ru' if is_russian else 'en'
        region_code = 'RU' if is_russian else 'US'
        start_time = time.time()
        page_token = None
        for page in range(max_pages):
            max_retries = len(self.KEYS)
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    
                    params = {
                        'part': 'snippet',
                        'q': query,
                        'type': 'video',
                        'order': 'date',
                        'maxResults': max_results,
                        'safeSearch': 'none',
                        'relevanceLanguage': relevance_language,
                        'regionCode': region_code,
                        'publishedAfter': published_after,
                        'pageToken': page_token if page_token else None
                    }

                    response = self.youtube_service.search().list(**params).execute()

                    responses += [item["id"]["videoId"] for item in response.get('items', [])]

                    quota_cost += response.get('searchCost', 100)

                    page_token = response.get('nextPageToken')
                    if not page_token:
                        break
                    
                    # Успешный запрос, выходим из цикла retry
                    break

                except HttpError as e:
                    status, retry_count = self.check_http_error(e, retry_count)
                    if status:
                        self.logger.info(f"search_video_with_pagination | HttpError | page | {retry_count}")
                        continue
                    else:
                        self.logger.info(f"search_video_with_pagination | HttpError | page | False")
                        return False, responses, time.time() - start_time
                        
                except Exception as e:
                    self.logger.error(f"    Ошибка при поиске (страница {page + 1}): {e}")
                    break
            
            if retry_count >= max_retries:
                self.logger.warning("    Превышено максимальное количество попыток")
                break
        
        # Фиксируем израсходованную квоту за этот вызов
        self._add_quota(quota_cost)
        return True, responses, time.time() - start_time

    def _restore_arrays_and_thresholds(self):
        """
        Восстанавливает массивы VIEWS_ARR, LIKES_ARR, COMMENTS_ARR, DURATION_ARR
        и пересчитывает пороги из существующих данных meta_snapshot.
        Вызывается при продолжении meta_snapshot после остановки.
        Теперь читает данные из всех категорий последовательно.
        """
        self.logger.info("_restore_arrays_and_thresholds | Восстанавливаем массивы из всех категорий")
        
        # Загружаем данные всех категорий
        all_categories_data = self._load_all_categories_data()
        
        if not all_categories_data:
            # Пробуем использовать existing_meta_data для обратной совместимости
            if self.existing_meta_data:
                all_categories_data = self.existing_meta_data
                self.logger.info("_restore_arrays_and_thresholds | Используем existing_meta_data для обратной совместимости")
            else:
                self.logger.warning("_restore_arrays_and_thresholds | Нет данных для восстановления")
                return
        
        restored_count = 0
        for category, intervals in all_categories_data.items():
            for interval, videos in intervals.items():
                if interval not in ["_used_queries", "completed"]:
                    if isinstance(videos, dict):
                        for video_id, video_data in videos.items():
                            if isinstance(video_data, dict):
                                # Извлекаем значения метрик
                                viewC = video_data.get("viewCount")
                                likeC = video_data.get("likeCount")
                                commentC = video_data.get("commentCount")
                                duration = video_data.get("duration")
                                
                                # Добавляем в массивы только если все значения валидны
                                if viewC is not None and likeC is not None and commentC is not None:
                                    try:
                                        view_val = int(viewC)
                                        like_val = int(likeC)
                                        comment_val = int(commentC)
                                        
                                        # Добавляем все значения в массивы при восстановлении
                                        # (при восстановлении мы восстанавливаем все данные, которые уже прошли фильтрацию ранее)
                                        # Пороги будут пересчитаны после восстановления всех данных
                                        self.VIEWS_ARR.append(view_val)
                                        self.LIKES_ARR.append(like_val)
                                        self.COMMENTS_ARR.append(comment_val)
                                        
                                        # Добавляем duration_seconds если он есть
                                        if duration is not None:
                                            try:
                                                duration_val = int(duration) if isinstance(duration, (int, float)) else duration
                                                if isinstance(duration_val, (int, float)) and duration_val > 0:
                                                    self.DURATION_ARR.append(duration_val)
                                            except (ValueError, TypeError):
                                                pass
                                        
                                        restored_count += 1
                                    except (ValueError, TypeError):
                                        continue
        
        self.logger.info(f"_restore_arrays_and_thresholds | Восстановлено значений: {restored_count}")
        self.logger.info(f"_restore_arrays_and_thresholds | Размеры массивов: VIEWS={len(self.VIEWS_ARR)}, LIKES={len(self.LIKES_ARR)}, COMMENTS={len(self.COMMENTS_ARR)}, DURATION={len(self.DURATION_ARR)}")
        
        # Пересчитываем пороги на основе восстановленных данных
        if len(self.VIEWS_ARR) >= 50:
            self.logger.info("_restore_arrays_and_thresholds | Пересчитываем пороги на основе восстановленных данных")
            self._correct_min_values(force=True)
            # Сбрасываем счетчик после восстановления, чтобы корректировка могла начаться с нуля
            self._videos_since_last_correction = 0
            self.logger.info(f"_restore_arrays_and_thresholds | Установлены пороги: MIN_VIEW={self.MIN_VIEW_COUNT}, MIN_LIKE={self.MIN_LIKE_COUNT}, MIN_COMMENT={self.MIN_COMMENT_COUNT}")
        else:
            self.logger.info(f"_restore_arrays_and_thresholds | Недостаточно данных для пересчета порогов (нужно >= 50, есть {len(self.VIEWS_ARR)})")
            # Если данных мало, устанавливаем минимальные пороги для начальной фильтрации
            # Это поможет не тратить квоту на совсем плохие видео
            if len(self.VIEWS_ARR) > 0:
                self.MIN_VIEW_COUNT = max(0, int(np.percentile(self.VIEWS_ARR, 10)))
                self.MIN_LIKE_COUNT = max(0, int(np.percentile(self.LIKES_ARR, 10)))
                self.MIN_COMMENT_COUNT = max(0, int(np.percentile(self.COMMENTS_ARR, 10)))
                self.logger.info(f"_restore_arrays_and_thresholds | Установлены минимальные пороги (10-й перцентиль): MIN_VIEW={self.MIN_VIEW_COUNT}, MIN_LIKE={self.MIN_LIKE_COUNT}, MIN_COMMENT={self.MIN_COMMENT_COUNT}")

    def _correct_min_values(self, force: bool = False):
        """
        Корректирует минимальные пороги для более равномерного распределения данных.
        Анализирует текущее распределение в VIEWS_ARR, LIKES_ARR, COMMENTS_ARR
        и корректирует MIN_VIEW_COUNT, MIN_LIKE_COUNT, MIN_COMMENT_COUNT.
        
        Args:
            force: Если True, выполняет корректировку независимо от интервала
        """
        # Минимальное количество данных для анализа
        MIN_SAMPLES = 50
        
        # Проверяем, есть ли достаточно данных для анализа
        if len(self.VIEWS_ARR) < MIN_SAMPLES:
            return
        
        # Если пороги еще не установлены (равны 0), устанавливаем их сразу при накоплении 50+ видео
        # Это важно для начала фильтрации на раннем этапе
        if (self.MIN_VIEW_COUNT == 0 and self.MIN_LIKE_COUNT == 0 and self.MIN_COMMENT_COUNT == 0):
            force = True  # Принудительно устанавливаем пороги при первом запуске
        
        # Проверяем, нужно ли выполнять корректировку (если не принудительно)
        if not force:
            # Корректируем пороги каждые N новых видео (определяется self._correction_interval)
            # Это позволяет адаптироваться к новым данным
            if self._videos_since_last_correction < self._correction_interval:
                return  # Еще не накопилось достаточно новых видео
        
        # Сбрасываем счетчик
        self._videos_since_last_correction = 0
        
        # Анализируем каждую метрику и корректируем пороги
        self._correct_metric_threshold(
            arr=self.VIEWS_ARR,
            threshold_name='MIN_VIEW_COUNT',
            metric_name='просмотры'
        )
        self._correct_metric_threshold(
            arr=self.LIKES_ARR,
            threshold_name='MIN_LIKE_COUNT',
            metric_name='лайки'
        )
        self._correct_metric_threshold(
            arr=self.COMMENTS_ARR,
            threshold_name='MIN_COMMENT_COUNT',
            metric_name='комментарии'
        )
    
    def _correct_metric_threshold(self, arr: list, threshold_name: str, metric_name: str):
        """
        Упрощенная корректировка порога для одной метрики.
        Использует простую стратегию на основе квантилей для быстрой фильтрации.
        Цель: эффективно использовать квоту, не тратя время на сложные вычисления.
        
        Args:
            arr: Массив значений метрики
            threshold_name: Название атрибута порога (например, 'MIN_VIEW_COUNT')
            metric_name: Название метрики для логирования
        """
        if not arr or len(arr) < 50:
            return
        
        # Преобразуем в numpy array и фильтруем
        np_arr = np.array(arr)
        np_arr = np_arr[np_arr > 0]  # Убираем нули и отрицательные
        
        if len(np_arr) < 50:
            return
        
        # Получаем текущий порог
        current_threshold = getattr(self, threshold_name, 0)
        
        # Упрощенная стратегия: используем 25-й перцентиль как целевой минимальный порог
        # Это отсекает 75% самых низких значений, оставляя более качественные видео
        q25 = np.percentile(np_arr, 25)
        q50 = np.percentile(np_arr, 50)  # Медиана
        
        # Если минимальный порог еще не установлен (равен 0) или очень низкий - устанавливаем на q25
        if current_threshold == 0 or current_threshold < q25 * 0.5:
            new_threshold = int(q25)
        else:
            # Если порог уже установлен, плавно корректируем его к q25
            # Используем более агрессивную корректировку (30% вместо 10%) для быстрой адаптации
            adjustment_factor = 0.3
            new_threshold = current_threshold + (q25 - current_threshold) * adjustment_factor
        
        # Устанавливаем новый минимальный порог (не ниже 0)
        new_threshold = max(0, int(new_threshold))
        setattr(self, threshold_name, new_threshold)
        
        # Логируем только при значительных изменениях
        if abs(new_threshold - current_threshold) > current_threshold * 0.1 or current_threshold == 0:
            self.logger.info(f"_correct_metric_threshold | {metric_name} | MIN: {current_threshold} -> {new_threshold} (q25: {int(q25)}, q50: {int(q50)})")

    def _filter_comment(self, comment_thread: dict) -> dict:
        """
        Фильтрует комментарий, оставляя только нужные поля.
        
        Args:
            comment_thread: Объект комментария от YouTube API
            
        Returns:
            Словарь с полями: text, likeCount, repliesCount, publishedAt, authorName
        """
        snippet = comment_thread.get('snippet', {})
        top_level_comment = snippet.get('topLevelComment', {})
        top_snippet = top_level_comment.get('snippet', {})
        
        return {
            "text": top_snippet.get('textDisplay', ''),
            "likeCount": top_snippet.get('likeCount', 0),
            "repliesCount": snippet.get('totalReplyCount', 0),
            "publishedAt": top_snippet.get('publishedAt', ''),
            "authorName": top_snippet.get('authorDisplayName', '')
        }

    def _get_comments_single(self, video_id: str) -> tuple:
        """
        Получает комментарии для одного видео.
        Возвращает (video_id, comments, quota, success)
        """
        video_comments = []
        max_retries = len(self.KEYS)
        retry_count = 0
        quota = 0
        
        while retry_count < max_retries:
            try:
                # Получаем service для текущего потока
                youtube_service = self.key_manager.get_service()
                
                # Получаем только 100 самых релевантных комментариев (без пагинации)
                params = {
                    "part": "snippet",
                    "maxResults": 100,
                    "order": "relevance",
                    "videoId": video_id
                }
                
                response = youtube_service.commentThreads().list(**params).execute()
                # Успешный запрос - обрабатываем ответ
                items = response.get('items', [])
                # Фильтруем комментарии, оставляя только нужные поля
                video_comments = [self._filter_comment(item) for item in items]
                quota = response.get('searchCost', 1)
                
                # Успешно получили комментарии
                return (video_id, video_comments, quota, True)

            except HttpError as e:
                # Используем версию check_http_error для параллельных методов
                status, retry_count = self._check_http_error_parallel(e, retry_count)
                if status:
                    # Инвалидируем thread-local service, чтобы при следующем вызове get_service() использовался новый ключ
                    if hasattr(self.key_manager.local, 'service'):
                        delattr(self.key_manager.local, 'service')
                    if hasattr(self.key_manager.local, 'key_version'):
                        delattr(self.key_manager.local, 'key_version')
                    if hasattr(self.key_manager.local, 'key_index'):
                        delattr(self.key_manager.local, 'key_index')
                    self.logger.warning(f"get_comment | HttpError | Уровень: video_id: {video_id} | Статус: {status} | Повторяем попытку: {retry_count}")
                    continue
                else:
                    self.logger.warning(f"get_comment | HttpError | Уровень: video_id: {video_id} | Статус: {status} | Пропускаем видео")
                    return (video_id, [], 0, True)
            except Exception as e:
                self.logger.warning(f"get_comment | Exception | Уровень: video_id | {video_id} | {e}")
                return (video_id, [], 0, False)
        
        # Если все попытки исчерпаны
        return (video_id, [], 0, False)

    def _get_comments(self, vids: list) -> dict:
        """
        Получает комментарии для списка видео параллельно.
        
        Args:
            vids: Список ID видео
        
        Returns:
            (all_comments, total_quota, failed_videos)
        """
        all_comments = {}
        total_quota = 0
        failed_videos = set()
        status = True

        if not vids:
            self.logger.info("_get_comments | Пустой список видео | Пропускаем запрос")
            return all_comments, total_quota, failed_videos

        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Запускаем задачи для всех видео
            future_to_video = {executor.submit(self._get_comments_single, video_id): video_id for video_id in vids}
            
            # Собираем результаты по мере выполнения
            for future in as_completed(future_to_video):
                video_id = future_to_video[future]
                try:
                    vid, comments, quota, success = future.result()
                    all_comments[vid] = comments
                    total_quota += quota
                    if not quota:
                        failed_videos.add(vid)
                    if not success:
                        status = False
                        break
                except Exception as e:
                    self.logger.warning(f"_get_comments | Ошибка при обработке {video_id}: {e}")
                    failed_videos.add(video_id)
                    all_comments[video_id] = []
        
        if len(failed_videos) > 0:
            self.logger.warning(f"    Пропущено видео с ошибками при получении комментариев: {len(failed_videos)}")
            
        if self.current_key_index >= len(self.KEYS):
            status = False
        
        return all_comments, total_quota, failed_videos, status

    def _min_val_filter(self, response: dict):
        """
        Фильтрует видео по минимальным порогам метрик.
        Поддерживает разные логики фильтрации:
        - 'OR': видео проходит, если хотя бы одна метрика >= порога (мягкая)
        - 'AND': видео проходит, если все метрики >= порогов (строгая)
        - 'MAJORITY': видео проходит, если хотя бы 2 из 3 метрик >= порогов (компромисс, рекомендуется)
        
        Также фильтрует видео по максимальной длительности (MAX_DURATION_SECONDS).
        """
        filter_items = {}
        main_cnt = 0
        filtered_cnt = 0
        duration_less_900 = 0  # Счетчик видео < 900 секунд
        duration_more_900 = 0   # Счетчик видео >= 900 секунд
        
        for item in response["items"]:
            id = item["id"]
            viewC = item["statistics"].get("viewCount", None)
            likeC = item["statistics"].get("likeCount", None)
            commentC = item["statistics"].get("commentCount", None)
            if viewC is None or likeC is None or commentC is None:
                continue
            main_cnt += 1
            
            view_val = int(viewC)
            like_val = int(likeC)
            comment_val = int(commentC)
            
            # Получаем duration_seconds из contentDetails
            duration_seconds = None
            duration_iso = item.get("contentDetails", {}).get("duration")
            if duration_iso:
                duration_seconds = parse_duration_iso(duration_iso)
            
            # Подсчитываем метрики по длительности (для всех видео, не только прошедших фильтр)
            if duration_seconds is not None:
                if duration_seconds < 900:
                    duration_less_900 += 1
                else:
                    duration_more_900 += 1
            
            # Фильтруем по длительности: только видео <= MAX_DURATION_SECONDS
            if duration_seconds is not None and duration_seconds > self.MAX_DURATION_SECONDS:
                continue  # Пропускаем видео длиннее 900 секунд
            
            # Проверяем минимальные пороги в зависимости от выбранной логики
            passes_filter = False
            
            if self.FILTER_LOGIC == 'OR':
                # OR: хотя бы одна метрика >= порога
                passes_filter = (view_val >= self.MIN_VIEW_COUNT or 
                                like_val >= self.MIN_LIKE_COUNT or 
                                comment_val >= self.MIN_COMMENT_COUNT)
            
            elif self.FILTER_LOGIC == 'AND':
                # AND: все метрики >= порогов
                passes_filter = (view_val >= self.MIN_VIEW_COUNT and 
                                like_val >= self.MIN_LIKE_COUNT and 
                                comment_val >= self.MIN_COMMENT_COUNT)
            
            elif self.FILTER_LOGIC == 'MAJORITY':
                # MAJORITY: хотя бы 2 из 3 метрик >= порогов (рекомендуется)
                metrics_passed = sum([
                    view_val >= self.MIN_VIEW_COUNT,
                    like_val >= self.MIN_LIKE_COUNT,
                    comment_val >= self.MIN_COMMENT_COUNT
                ])
                passes_filter = metrics_passed >= 2
            
            else:
                # По умолчанию используем OR для обратной совместимости
                passes_filter = (view_val >= self.MIN_VIEW_COUNT or 
                                like_val >= self.MIN_LIKE_COUNT or 
                                comment_val >= self.MIN_COMMENT_COUNT)
            
            if passes_filter:
                filtered_cnt += 1
                self.VIEWS_ARR.append(view_val)
                self.LIKES_ARR.append(like_val)
                self.COMMENTS_ARR.append(comment_val)
                # Добавляем duration_seconds в массив метрик
                if duration_seconds is not None:
                    self.DURATION_ARR.append(duration_seconds)
                # Увеличиваем счетчик для корректировки порогов
                self._videos_since_last_correction += 1
                del item["id"]
                filter_items[id] = item
        
        # Логируем метрики по длительности
        total_duration_videos = duration_less_900 + duration_more_900
        
        # Логируем распределение duration_seconds для прошедших фильтр
        if len(self.DURATION_ARR) > 0:
            duration_arr_np = np.array(self.DURATION_ARR)

        return filter_items, main_cnt, filtered_cnt

    def _text_processing(self, snippet: dict) -> dict:
        title = snippet.get("title", "")
        description = snippet.get("description", "")
        existing_tags = snippet.get("tags", []) or []
        
        # Извлекаем теги из title
        tags_from_title = extract_tags_from_text(title)
        # Извлекаем теги из description
        tags_from_description = extract_tags_from_text(description)
        
        # Объединяем все теги
        # Сохраняем оригинальные теги из API (с их регистром)
        # Добавляем извлеченные теги (в нижнем регистре) только если их нет в оригинальных
        all_tags = list(existing_tags)  # Копируем существующие теги
        existing_tags_lower = [tag.lower() for tag in existing_tags]
        
        # Добавляем теги из title, которых нет в существующих
        for tag in tags_from_title:
            if tag not in existing_tags_lower:
                all_tags.append(tag)
        
        # Добавляем теги из description, которых нет в существующих
        for tag in tags_from_description:
            if tag not in existing_tags_lower and tag not in [t.lower() for t in all_tags]:
                all_tags.append(tag)
        
        # Очищаем title и description от тегов
        clean_title = clean_text_from_tags(title)
        clean_description = clean_text_from_tags(description)

        return clean_title, clean_description, all_tags

    def _base_attributes_filter(self, response: dict, temporal: bool = False) -> dict:
        items = {}
        
        if "items" in response:
            items_list = response["items"]
        else:
            items_list = []
            for video_id, item_data in response.items():
                item_data["id"] = video_id  # Восстанавливаем id для единообразия
                items_list.append(item_data)
        
        for item in items_list:
            id = item["id"]
            if not temporal:
                try:
                    title, description, tags = self._text_processing(item["snippet"])
                    duration_iso = item["contentDetails"].get("duration")
                    duration_seconds = None
                    if duration_iso:
                        duration_seconds = parse_duration_iso(duration_iso)
                    items[id] = {
                        "title": title,
                        "description": description,
                        "tags": tags,
                        "channelId": item["snippet"]["channelId"],
                        "channelTitle": item["snippet"]["channelTitle"],
                        "publishedAt": item["snippet"]["publishedAt"],
                        "language": item["snippet"]["defaultLanguage"],
                        "viewCount": item["statistics"]["viewCount"],
                        "likeCount": item["statistics"]["likeCount"],
                        "commentCount": item["statistics"]["commentCount"],
                        "madeForKids": item.get("status", {}).get("madeForKids", False),
                        "thumbnails": item["snippet"]["thumbnails"],
                        "duration": duration_seconds,
                        "thumbnails": {
                            "standard": item["snippet"]["thumbnails"].get("standard", {})
                        }
                    }
                except Exception as e:
                    self.logger.warning(f"_base_attributes_filter | Meta | id: {id} | Error: {e}")
            else:
                try:
                    items[id] = {
                        "viewCount": item["statistics"]["viewCount"],
                        "likeCount": item["statistics"]["likeCount"],
                        "commentCount": item["statistics"]["commentCount"],
                        "channelId": item["snippet"]["channelId"],
                    }
                except KeyError:
                    self.logger.warning(f"_base_attributes_filter | Temporal | id: {id} | KeyError")
                except Exception as e:
                    self.logger.warning(f"_base_attributes_filter | Temporal | id: {id} | Error: {e}")
        return items

    def _get_basic_info(self, vids: list) -> dict:
        len_vids = len(vids)
        
        if len_vids > 1:
            vids = ",".join(vids)
        else:
            vids = vids[0]

        max_retries = len(self.KEYS)
        retry_count = 0
        quota = 0
        
        start_time = time.time()

        while retry_count < max_retries:
            try:
                response = self.youtube_service.videos().list(
                    part="snippet,contentDetails,statistics,status",
                    id=vids
                ).execute()
                quota += response.get('searchCost', 1)
                duration = time.time() - start_time

                if not self.temporal:
                    filter_items, main_cnt, filtered_cnt = self._min_val_filter(response)
                    self.logger.info(f"get_basic_info | Min val filter | После фильтрации: {filtered_cnt}")
                    self._correct_min_values()

                items = self._base_attributes_filter(response=response if self.temporal else filter_items, temporal=self.temporal)   

                return items, quota, duration, True

            except HttpError as e:
                status, retry_count = self.check_http_error(e, retry_count)
                if status:
                    self.logger.warning(f"get_basic_info | HttpError | Уровень: video_ids | len(vids): {len_vids} | Статус: {status} | Повторяем попытку: {retry_count}")
                    continue
                else:
                    self.logger.warning(f"get_basic_info | HttpError | Уровень: video_ids | len(vids): {len_vids} | Статус: {status}")
                    break
        return {}, 0, 0, False

    def _get_channel_info_single(self, vid: str, channel_id: str) -> tuple:
        """
        Получает информацию о канале для одного видео.
        Возвращает (vid, channel_data, quota, success)
        """
        # Получаем блокировку для конкретного channel_id
        channel_lock = self.get_channel_lock(channel_id)
        
        # Используем блокировку для предотвращения дублирующих запросов
        with channel_lock:
            # Double-check: проверяем кэш еще раз после получения блокировки
            # (на случай, если другой поток уже добавил данные)
            with self.channel_cache_lock:
                if channel_id in self.channel_cache:
                    return (vid, self.channel_cache[channel_id], 0, True)
            
            # Если данных нет в кэше, делаем запрос
            max_retries = len(self.KEYS)
            retry_count = 0
            quota = 0
            
            while retry_count < max_retries:
                try:
                    # Получаем service для текущего потока
                    youtube_service = self.key_manager.get_service()
                    
                    response = youtube_service.channels().list(
                        part="snippet,statistics",
                        id=channel_id
                    ).execute()
                    
                    # Проверяем наличие items в response
                    if 'items' in response and response.get('items'):
                        quota = response.get('searchCost', 1)
                        
                        statistics = response['items'][0].get('statistics', {})
                        snippet = response['items'][0].get('snippet', {})
                        
                        channel_data = {
                            "subscriberCount": int(statistics.get('subscriberCount', 0)) if statistics.get('subscriberCount') else None,
                            "videoCount": int(statistics.get('videoCount', 0)) if statistics.get('videoCount') else None,
                            "viewCount_channel": int(statistics.get('viewCount', 0)) if statistics.get('viewCount') else None,
                            "country": snippet.get('country', ''),
                            "channelTitle": snippet.get('title', '')
                        }
                        
                        # Сохраняем в кэш (thread-safe)
                        with self.channel_cache_lock:
                            self.channel_cache[channel_id] = channel_data
                        
                        return (vid, channel_data, quota, True)
                    else:
                        # Пустой ответ - канал не найден или невалидный channel_id
                        self.logger.warning(f"get_channel_info_single | Пустой ответ API для channel_id: {channel_id} (канал не найден или невалидный ID)")

                        with self.channel_cache_lock:
                            self.channel_cache[channel_id] = {}
                        
                        return (vid, {}, 0, True)
                    
                except HttpError as e:
                    status, retry_count = self._check_http_error_parallel(e, retry_count)
                    if status:
                        # Инвалидируем thread-local service
                        if hasattr(self.key_manager.local, 'service'):
                            delattr(self.key_manager.local, 'service')
                        if hasattr(self.key_manager.local, 'key_version'):
                            delattr(self.key_manager.local, 'key_version')
                        if hasattr(self.key_manager.local, 'key_index'):
                            delattr(self.key_manager.local, 'key_index')
                        self.logger.warning(f"get_channel_info_single | HttpError | Уровень: channel_id: {channel_id} | Статус: {status} | Повторяем попытку: {retry_count}")
                        continue
                    else:
                        self.logger.warning(f"get_channel_info_single | HttpError | Уровень: channel_id: {channel_id} | Статус: {status} | Создаем пустые данные")

                        with self.channel_cache_lock:
                            self.channel_cache[channel_id] = {}
                        return (vid, {}, 0, False)
                except Exception as e:
                    self.logger.warning(f"get_channel_info_single | Exception | Уровень: channel_id: {channel_id} | {e}")
   
                    with self.channel_cache_lock:
                        self.channel_cache[channel_id] = {}
                        
                    return (vid, {}, 0, False)

            with self.channel_cache_lock:
                self.channel_cache[channel_id] = {}
            return (vid, {}, 0, False)

    def _get_channel_info(self, base_info: dict) -> dict:
        """
        Получает информацию о каналах для списка видео параллельно.
        
        Args:
            base_info: Словарь {video_id: {channelId: ...}}
        
        Returns:
            (items, quota) где items - словарь {video_id: channel_data}, quota - использованная квота
        """
        items = {}
        total_quota = 0
        
        if not base_info:
            self.logger.info("_get_channel_info | Пустой список видео | Пропускаем запрос")
            return items, total_quota
        
        # Подготавливаем список задач для параллельной обработки
        # НЕ проверяем кэш здесь - проверка будет внутри _get_channel_info_single() для каждого видео
        # Это важно, так как несколько видео могут принадлежать одному каналу
        tasks = []
        for vid, item in base_info.items():
            channel_id = item.get("channelId")
            if not channel_id or not vid:
                continue
            
            tasks.append((vid, channel_id))
        
        if not tasks:
            self.logger.info(f"_get_channel_info | Нет задач для обработки")
            return items, total_quota
        
        status = True
        
        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            # Запускаем задачи для всех каналов
            future_to_task = {executor.submit(self._get_channel_info_single, vid, channel_id): (vid, channel_id) for vid, channel_id in tasks}
            
            # Собираем результаты по мере выполнения
            for future in as_completed(future_to_task):
                vid, channel_id = future_to_task[future]
                try:
                    result_vid, channel_data, quota, success = future.result()
                    items[result_vid] = channel_data
                    total_quota += quota
                    if not success:
                        status = False
                        break
                except Exception as e:
                    self.logger.warning(f"_get_channel_info | Ошибка при обработке {vid} (channel_id: {channel_id}): {e}")
        
        return items, total_quota, status

    def _batching(self, vids: list) -> list:
        batches = []
        for i in range(0, len(vids), 50):
            batches.append(vids[i:i + 50])
        return batches

    def _batch_aggregation(
        self, 
        vids: list, 
        base_data: dict, 
        channel_data: dict, 
        comments_data: dict, 
        batch_duration: float, 
        base_quota: float, 
        channel_quota: float, 
        comments_quota: float,
        failed_comments: Set[str],
        previous_data: Optional[Dict[str, Dict]] = None
    ) -> dict:
        batch_result = {}
        for vid in vids:
            prev_entry = previous_data.get(vid, {}) if previous_data else {}
            comments = comments_data.get(vid, [])
            if vid in failed_comments and prev_entry:
                comments = prev_entry.get("comments", comments)
                if comments:
                    self.logger.info(f"_batch_aggregation | Используем комментарии из предыдущего снапшота | video_id: {vid}")
            if not self.temporal:
                batch_result[vid] = {
                    "title": base_data[vid]["title"],
                    "description": base_data[vid]["description"],
                    "tags": base_data[vid]["tags"],
                    "language": base_data[vid]["language"],
                    "viewCount": base_data[vid]["viewCount"],
                    "likeCount": base_data[vid]["likeCount"],
                    "commentCount": base_data[vid]["commentCount"],
                    "madeForKids": base_data[vid]["madeForKids"],
                    "thumbnails": base_data[vid]["thumbnails"],
                    "duration": base_data[vid]["duration"],
                    "publishedAt": base_data[vid]["publishedAt"],
                    "channelTitle": channel_data[vid]["channelTitle"],
                    "subscriberCount": channel_data[vid]["subscriberCount"],
                    "videoCount": channel_data[vid]["videoCount"],
                    "viewCount_channel": channel_data[vid]["viewCount_channel"],
                    "country": channel_data[vid]["country"],
                    "comments": comments,
                }
            else:
                batch_result[vid] = {
                    "viewCount": base_data[vid]["viewCount"],
                    "likeCount": base_data[vid]["likeCount"],
                    "commentCount": base_data[vid]["commentCount"],
                    "subscriberCount": channel_data[vid]["subscriberCount"],
                    "videoCount": channel_data[vid]["videoCount"],
                    "viewCount_channel": channel_data[vid]["viewCount_channel"],
                    "comments": comments,
                }
          
        return batch_result

    def _batch_run(self, vids: list, previous_data: Optional[Dict[str, Dict]] = None) -> dict:
        batches = self._batching(vids)
        batch_results = []
        b_quota = 0
        _status = True
        for i, batch in enumerate(batches):
            start_time = time.time()
            base_data, base_quota, duration, status = self._get_basic_info(batch)
            b_quota += base_quota
            if not status:
                self.logger.info(f"Batch: {i+1} | на _get_basic_info | status: {status}")
                _status = False
                break
            if not base_data:
                self.logger.info("_batch_run | Все видео отфильтрованы | Пропускаем получение channel/comments")
                continue
            filtered_vids = list(base_data.keys())
            channel_data, channel_quota, status = self._get_channel_info(base_data)
            b_quota += channel_quota
            if not status:
                self.logger.info(f"Batch: {i+1} | на _get_channel_info | status: {status}")
                _status = False
                break
            comments_data, comments_quota, failed_comments, status = self._get_comments(filtered_vids)
            if not status:
                self.logger.info(f"Batch: {i+1} | на _get_comments | status: {status}")
                _status = False
                break
            b_quota += comments_quota
            batch_duration = time.time() - start_time
            valid_vids = [vid for vid in base_data.keys() if vid in channel_data and vid in comments_data]
            batch_result = self._batch_aggregation(
                valid_vids, base_data, channel_data, comments_data, batch_duration, base_quota, channel_quota, comments_quota, failed_comments, previous_data or {}
            )            
            batch_results.append(batch_result)
        return batch_results, _status, b_quota

    def _get_max_results_and_pages(self, nums):
        return min(100, nums), min(9, (nums // 100) + 1 if nums >= 100 else 1)

    def _datetime2interval(self, published_at: str) -> str:
        published_at = datetime.fromisoformat(published_at.replace("Z", ""))
        if published_at >= datetime.now() - timedelta(days=1):
            return "less-1day"
        elif published_at >= datetime.now() - timedelta(days=7):
            return "1day-1week"
        elif published_at >= datetime.now() - timedelta(days=30):
            return "1week-1month"
        elif published_at >= datetime.now() - timedelta(days=90):
            return "1month-3month"
        elif published_at >= datetime.now() - timedelta(days=180):
            return "3month-6month"
        elif published_at >= datetime.now() - timedelta(days=365):
            return "6month-1year"
        elif published_at >= datetime.now() - timedelta(days=1095):
            return "1year-3year"
        else:
            return "3year-more"

    def _interval2datetime(self, time_interval: str) -> list:
        _time = datetime.utcnow()

        interval = time_interval.split("-")
        result = []
        for timestamp in interval:
            if "less" in timestamp:
                result.append(_time)
            if "day" in timestamp:
                result.append(_time - timedelta(days=int(timestamp.split("day")[0])))
            if "week" in timestamp:
                result.append(_time - timedelta(days=int(timestamp.split("week")[0])*7))
            if "month" in timestamp:
                result.append(_time - timedelta(days=int(timestamp.split("month")[0])*30))
            if "year" in timestamp:
                result.append(_time - timedelta(days=int(timestamp.split("year")[0])*365))
            if "more" in timestamp:
                result.append(_time - timedelta(days=7300))
        return {"start_time":result[0], "end_time":result[1]}

    def _get_interval_info(self, results, cat, intervals):
        _inter = {}
        for interval in intervals:
            # Пропускаем специальные ключи, которые не являются временными интервалами
            if interval == "_used_queries" or interval not in self.TIME_INTERVALS_NUM_VIDEOS:
                continue
            target = self.TIME_INTERVALS_NUM_VIDEOS[interval]
            current = len(results[cat][interval])
            if current < target:
                _inter[interval] = target - current
            else:
                _inter[interval] = 0
        return _inter

    def _get_published_after(self, results, cat, intervals):
        interval_info = self._get_interval_info(results, cat, intervals)

        if interval_info:
            ordered_intervals = [
                interval for interval in self.TIME_INTERVALS_NUM_VIDEOS.keys()
                if interval in interval_info
            ]
        else:
            ordered_intervals = list(self.TIME_INTERVALS_NUM_VIDEOS.keys())

        published_after = None

        for interval in reversed(ordered_intervals):
            if interval_info.get(interval, 0) > 0:
                published_after = self._interval2datetime(interval)["end_time"]
                break

        if published_after is None:
            last_interval = ordered_intervals[-1] if ordered_intervals else None
            if last_interval:
                published_after = self._interval2datetime(last_interval)["end_time"]
            else:
                return None

        # Преобразуем datetime в строку в формате ISO с 'Z' (UTC)
        if isinstance(published_after, datetime):
            # Если datetime не имеет timezone, считаем его UTC (так как _interval2datetime использует datetime.now())
            # и просто добавляем timezone.utc
            if published_after.tzinfo is None:
                published_after = published_after.replace(tzinfo=timezone.utc)
            else:
                published_after = published_after.astimezone(timezone.utc)
            return published_after.strftime('%Y-%m-%dT%H:%M:%SZ')
        return published_after

    def distribute_to_intervals(self, batch_results, cat, results):
        added_vids = []
        for batch_result in batch_results:
            for vid, item in batch_result.items():
                interval = self._datetime2interval(item["publishedAt"])
                if vid not in results[cat][interval]:
                    results[cat][interval][vid] = item
                    added_vids.append(vid)
        return results, added_vids

    def _append_query(self, query, cat, results):
        if query not in results[cat].get("_used_queries", []):
            results[cat].setdefault("_used_queries", []).append(query)
        return results

    def log_progress(self, results, sequence=None, cat=None, timestamp=None, meta=False, new_vids_count=None):
        self.logger.info("")
        if meta:
            all_nums = 0
            cat_nums = 0
            for category, intervals in results.items():
                for interval, nums in intervals.items():
                    if interval not in [ "_used_queries", 'completed']:
                        all_nums += len(nums)
                        if category == cat:
                            cat_nums += len(nums)
                            self.logger.info(f"log_progress | {category} | {interval} | results: {len(nums)}")
            if sequence:
                all_seq = 0
                for t, v in sequence.items():
                    all_seq += len(v)
                log_msg = f"log_progress | Всего в results: {all_nums} | Всего в sequence: {all_seq}"
                if cat and cat_nums > 0:
                    log_msg += f" | В категории {cat}: {cat_nums}"
                if new_vids_count is not None:
                    log_msg += f" | Получено за запрос: {new_vids_count}"
                self.logger.info(log_msg)
                diff = all_nums - all_seq
                if diff != 0:
                    self.logger.info(f"log_progress | Несоответствие results vs sequence: {diff}")
            else:
                log_msg = f"log_progress | Всего в results: {all_nums}"
                if cat and cat_nums > 0:
                    log_msg += f" | В категории {cat}: {cat_nums}"
                if new_vids_count is not None:
                    log_msg += f" | Получено за запрос: {new_vids_count}"
                self.logger.info(log_msg)
        if timestamp and not meta:
            processed_now = len(results.get(timestamp, {}))
            processed_before = 0
            if self.existing_snapshot_data and timestamp in self.existing_snapshot_data:
                processed_before = len(self.existing_snapshot_data[timestamp])
            processed_total = processed_before + processed_now

            target_total = None
            if self.target2ids and timestamp in self.target2ids:
                target_total = len(self.target2ids[timestamp])

            current_total = None
            remaining_current = None
            if self.current and timestamp in self.current:
                current_entry = self.current[timestamp]
                if isinstance(current_entry, (list, set, tuple)):
                    current_total = len(current_entry)
                elif isinstance(current_entry, dict):
                    current_total = len(current_entry)
                elif isinstance(current_entry, int):
                    current_total = current_entry
                if isinstance(current_total, int):
                    remaining_current = max(current_total - processed_now, 0)

            remaining_total = None
            if isinstance(target_total, int):
                remaining_total = max(target_total - processed_total, 0)

            msg_parts = [f"log_progress | timestamp: {timestamp}", f"processed_now: {processed_now}"]
            if current_total is not None:
                msg_parts.append(f"current_batch: {processed_now}/{current_total}")
                if remaining_current is not None:
                    msg_parts.append(f"remaining_current: {remaining_current}")
            if target_total is not None:
                msg_parts.append(f"processed_total: {processed_total}/{target_total}")
                if remaining_total is not None:
                    msg_parts.append(f"remaining_total: {remaining_total}")
            else:
                msg_parts.append(f"processed_total: {processed_total}")

            self.logger.info(" | ".join(msg_parts))
        self.logger.info("")
        
    def filt_duplicates(self, vids: list) -> list:
        return [vid for vid in vids if vid not in self.existing_meta_ids]

    def search_categories(self):
        # Инициализируем results для работы с категориями
        # Для совместимости создаем структуру со всеми категориями, но загружаем данные только для текущих
        results = {}
        
        # Загружаем данные для всех категорий из self.current
        for cat in self.current.keys():
            if self.current[cat] == "completed":
                continue
            
            # Пробуем загрузить данные категории из файла
            category_data = self._load_category_data(cat)
            if category_data:
                results[cat] = category_data
            else:
                # Инициализируем новую категорию
                results[cat] = {interval: {} for interval in self.TIME_INTERVALS_NUM_VIDEOS.keys()}
                results[cat]["_used_queries"] = []

        # Убеждаемся, что у всех категорий есть _used_queries
        for cat in results.keys():
            if "_used_queries" not in results[cat]:
                results[cat]["_used_queries"] = []

        sequence = None

        for cat, intervals in self.current.items():

            if intervals == "completed":
                self.logger.info(f"search_categories | {cat} already completed")
                continue

            # Убеждаемся, что категория есть в results
            if cat not in results:
                results[cat] = {interval: {} for interval in self.TIME_INTERVALS_NUM_VIDEOS.keys()}
                results[cat]["_used_queries"] = []

            # Обнуляем границы фильтрации для каждой новой категории
            self.MIN_VIEW_COUNT = 0
            self.MIN_LIKE_COUNT = 0
            self.MIN_COMMENT_COUNT = 0
            self.MAX_DURATION_SECONDS = 900
            self.MAX_VIEW_COUNT = float('inf')
            self.MAX_LIKE_COUNT = float('inf')
            self.MAX_COMMENT_COUNT = float('inf')
            # Очищаем массивы метрик
            self.VIEWS_ARR = []
            self.LIKES_ARR = []
            self.COMMENTS_ARR = []
            self.DURATION_ARR = []
            # Обнуляем счетчик корректировки порогов
            self._videos_since_last_correction = 0
            self.logger.info(f"search_categories | {cat} | Границы фильтрации обнулены для новой категории")

            self.channel_cache = {}
            
            self.logger.info(f"search_categories | {cat}")
            
            published_after = self._get_published_after(results, cat, intervals.keys())

            nums = sum([j for i, j in intervals.items() if i != "_used_queries" and i != "completed"])

            full_cat = False
            _query_ids = []

            for query in CATEGORY_KEYWORDS[cat]:

                if query in results[cat]["_used_queries"]:
                    self.logger.info(f"Пропущено ключевое слово: {query}")
                    continue

                if full_cat:
                    break

                for _ in range(3):

                    try:
                        self.logger.info(f"Запрос: {query}")
                        self.logger.info(f"search_categories | published_after: {published_after}")
                        self.logger.info(f"search_categories | Фильтрация: {self.FILTER_LOGIC} | MIN_LIKE: {self.MIN_LIKE_COUNT} | MIN_VIEWS: {self.MIN_VIEW_COUNT} | MIN_COMMENT: {self.MIN_COMMENT_COUNT} | MAX_DURATION: {self.MAX_DURATION_SECONDS} сек")

                        max_results, pages = self._get_max_results_and_pages(nums)
                        
                        status, responses, _time = self.search_videos_with_pagination(query=query, published_after=published_after, max_results=max_results, max_pages=pages)
                        
                        responses_filt = self.filt_duplicates(responses)
                        
                        self.logger.info(f"Пришло с search: {len(responses)} | Пришло с filt: {len(responses_filt)} | Дубликатов: {len(responses) - len(responses_filt)}")

                        batch_results, status, b_quota = self._batch_run(responses)

                        results, added_vids = self.distribute_to_intervals(batch_results, cat, results)
                        results = self._append_query(query, cat, results)
                        
                        self.logger.info(f"Попало в Results: {len(batch_results)}")

                        new_vids = added_vids

                        nums -= len(new_vids)

                        self.logger.info(f"Осталось видео: {nums}")

                        published_after = self._get_published_after(results, cat, intervals.keys())
                        
                        timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M")
                        
                        # Передаем category в save_progress
                        self.save_progress(results, category=cat)
                        sequence = self.save_sequence(timestamp=timestamp, vids=new_vids)

                        self.log_progress(results, sequence, cat=cat, timestamp=timestamp, meta=True, new_vids_count=len(new_vids))

                        if nums <= 0:
                            results[cat]["completed"] = True
                            # Обновляем progress при завершении категории
                            self.save_progress(results, category=cat)
                            full_cat = True
                            break
                        
                        if not status:
                            self.save_progress(results, category=cat)
                            raise QuotaError
                        
                        break

                    except QuotaError:
                        self.logger.warning(f"search_categories | QuotaError")
                        raise QuotaError
                    except Exception as e:
                        self.logger.warning(f"search_categories | Exception | {e} | try: {_}")
                        continue
                    
            results[cat]["completed"] = True
            self.save_progress(results, category=cat)

        raise CompleteSnapshot

    def prepare_batch(self, batch_result):
        results = {}
        for batch in batch_result:
            for id, data in batch.items():
                results[id] = data
        return results

    def _sleep(self, seconds: int):
        if seconds > 60:
            self.logger.info(f"Ожидание: {seconds}")
            time.sleep(60)
            seconds -= 60
        else:
            self.logger.info(f"Ожидание: {seconds}")
            time.sleep(seconds)
            seconds = 0
        return seconds

    def search_snapshot(self):
        self.logger.info("[search_snapshot] Start")

        if not self.target2ids:
            self.logger.info("[search_snapshot] Еще раз создаем target2ids")
            self.target2ids = self._create_target2ids()

        self.channel_cache = {}

        results = {}

        for timestamp, vids in self.current.items():

            _time = datetime.now()
            _target_time = datetime.strptime(timestamp, "%Y_%m_%d_%H_%M")

            if _time < _target_time:
                seconds = round((_target_time - _time).total_seconds())
                while seconds > 0:
                    seconds = self._sleep(seconds)

            self.logger.info(f"[search_snapshot] Timestamp: {timestamp} | Видео: {len(vids)}")

            prev_data = {}
            if self.existing_snapshot_data and timestamp in self.existing_snapshot_data:
                prev_data = self.existing_snapshot_data[timestamp]

            tik = time.time()
            timestamp_results, status, b_quota = self._batch_run(vids, previous_data=prev_data)
            tok = round(time.time() - tik, 2)

            self.logger.info(f"[search_snapshot] Quota за батч: {b_quota} | Время батча: {tok}")

            if not status:
                raise QuotaError

            results[timestamp] = self.prepare_batch(timestamp_results)

            self.save_progress(results, timestamp=timestamp)

            self.log_progress(results, timestamp=timestamp)

        raise CompleteSnapshot

def main():
    current_key_index = 0
    while True:
        try:
            fetcher = Fetcher(current_key_index)
            if fetcher.snapshot_num == 0:
                fetcher.search_categories()
            else:
                fetcher.search_snapshot()
        except GlobalComplete:
            _global_logger.info("GlobalComplete")
            break
        except CompleteSnapshot:
            # Синхронизируем current_key_index с KeyManager (может быть обновлен в параллельных операциях)
            current_key_index = fetcher.key_manager.get_current_key_index()
            fetcher.current_key_index = current_key_index
            _global_logger.info("CompleteSnapshot")
            return
        except QuotaError:
            current_key_index = 0
            _global_logger.info("QuotaError")
            wait_until_quota_reset()
            continue

if __name__ == "__main__":
    main()
