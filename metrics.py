"""
Prometheus metrics collector for fetcher results.

This module collects detailed metrics from fetcher JSON files in .results/fetcher directory
and exposes them in Prometheus format. Can be used as a standalone HTTP server or integrated into
other Prometheus exporters.

Supports:
- meta_snapshot: metrics from initial snapshot (categories)
- snapshot_N: metrics from temporal snapshots (timestamps) with deltas
"""

import json
import os
import threading
import time
import statistics
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import Counter, defaultdict
from prometheus_client import CollectorRegistry, generate_latest
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily

# Настройка логирования
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

try:
    from huggingface_hub import HfApi
    HF_HUB_AVAILABLE = True
except ImportError:
    HF_HUB_AVAILABLE = False
    print("Warning: huggingface_hub is not installed. Install it with: pip install huggingface_hub")

# Определяем корень проекта относительно этого файла
# Здесь root — это директория, в которой лежит metrics.py (т.е. сам проект MetaFetcher)
_project_root = os.path.dirname(os.path.abspath(__file__))

# Основная директория результатов для fetcher (./.results/fetcher внутри проекта)
FETCHER_RESULTS_DIR = os.path.join(_project_root, ".results", "fetcher")
# Директория результатов для yt_dlp
YT_DLP_RESULTS_DIR = os.path.join(_project_root, ".results", "fetcher", "yt_dlp")


def _resolve_fetcher_results_dir(preferred_dir: Optional[str] = None) -> str:
    """Определяет директорию результатов для чтения fetcher файлов."""
    if preferred_dir and os.path.isdir(preferred_dir):
        return preferred_dir
    if os.path.isdir(FETCHER_RESULTS_DIR):
        return FETCHER_RESULTS_DIR
    return preferred_dir or FETCHER_RESULTS_DIR


def _safe_convert_to_number(value: Any) -> Optional[float]:
    """Безопасно конвертирует значение в число."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return None


def _parse_iso_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """Парсит ISO datetime строку."""
    if not date_str:
        return None
    try:
        # Пробуем разные форматы
        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        # Если не получилось, пробуем ISO формат
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except Exception:
        return None


class FetcherMetricsCollector:
    """Collector for fetcher metrics that can be registered with Prometheus."""

    def __init__(self, results_dir: Optional[str] = None, token = None):
        self.results_dir = _resolve_fetcher_results_dir(results_dir)
        self.token = token
        self.repo_id = "Ilialebedev/yt-metrics"
        self.repo_type = "dataset"
        self._upload_thread = None
        self._stop_upload_thread = False
        
        # Запускаем загрузку при инициализации и настраиваем периодический запуск
        if self.token and HF_HUB_AVAILABLE:
            self._start_periodic_upload()

    def _start_periodic_upload(self):
        """Запускает периодическую загрузку метрик в HF (каждый час и при инициализации)."""
        def upload_loop():
            # Первая загрузка при инициализации
            self.upload_list_metrics_to_hf()
            
            # Затем каждые 3600 секунд (1 час)
            while not self._stop_upload_thread:
                time.sleep(3600)
                if not self._stop_upload_thread:
                    self.upload_list_metrics_to_hf()
        
        self._upload_thread = threading.Thread(target=upload_loop, daemon=True)
        self._upload_thread.start()
    
    def stop_periodic_upload(self):
        """Останавливает периодическую загрузку метрик."""
        self._stop_upload_thread = True
        if self._upload_thread:
            self._upload_thread.join(timeout=5)

    def upload_list_metrics_to_hf(self):
        """
        Выгружает все списки со значениями метрик в HuggingFace.
        Собирает все метрики и загружает их как JSON файл в репозиторий.
        """
        if not HF_HUB_AVAILABLE:
            print("Warning: huggingface_hub is not available. Cannot upload metrics.")
            return False
        
        if not self.token:
            print("Warning: HuggingFace token is not provided. Cannot upload metrics.")
            return False
        
        try:
            # Собираем все метрики
            self._collect_metrics()
            
            # Подготавливаем данные для загрузки
            metrics_data = {
                "timestamp": datetime.now().isoformat(),
                "meta_snapshot": {
                    "videos_total": self.meta_videos_total,
                    "title_lengths": self.meta_title_lengths[:1000] if len(self.meta_title_lengths) > 1000 else self.meta_title_lengths,  # Ограничиваем для размера
                    "description_lengths": self.meta_description_lengths[:1000] if len(self.meta_description_lengths) > 1000 else self.meta_description_lengths,
                    "tags_counts": self.meta_tags_counts[:1000] if len(self.meta_tags_counts) > 1000 else self.meta_tags_counts,
                    "tags_top20": dict(Counter(self.meta_tags_all).most_common(20)) if self.meta_tags_all else {},
                    "languages": dict(Counter(self.meta_languages)) if self.meta_languages else {},
                    "view_counts": self.meta_view_counts[:1000] if len(self.meta_view_counts) > 1000 else self.meta_view_counts,
                    "like_counts": self.meta_like_counts[:1000] if len(self.meta_like_counts) > 1000 else self.meta_like_counts,
                    "comment_counts": self.meta_comment_counts[:1000] if len(self.meta_comment_counts) > 1000 else self.meta_comment_counts,
                    "durations": self.meta_durations[:1000] if len(self.meta_durations) > 1000 else self.meta_durations,
                    "subscriber_counts": self.meta_subscriber_counts[:1000] if len(self.meta_subscriber_counts) > 1000 else self.meta_subscriber_counts,
                    "video_counts": self.meta_video_counts[:1000] if len(self.meta_video_counts) > 1000 else self.meta_video_counts,
                    "view_count_channels": self.meta_view_count_channels[:1000] if len(self.meta_view_count_channels) > 1000 else self.meta_view_count_channels,
                    "countries_top20": dict(Counter(self.meta_countries).most_common(20)) if self.meta_countries else {},
                    "comments_counts": self.meta_comments_counts[:1000] if len(self.meta_comments_counts) > 1000 else self.meta_comments_counts,
                    "comment_text_lengths": self.meta_comment_text_lengths[:1000] if len(self.meta_comment_text_lengths) > 1000 else self.meta_comment_text_lengths,
                    "comment_like_counts": self.meta_comment_like_counts[:1000] if len(self.meta_comment_like_counts) > 1000 else self.meta_comment_like_counts,
                    "comment_reply_counts": self.meta_comment_reply_counts[:1000] if len(self.meta_comment_reply_counts) > 1000 else self.meta_comment_reply_counts,
                    "comment_authors_top20": dict(Counter(self.meta_comment_authors).most_common(20)) if self.meta_comment_authors else {},
                },
                "snapshots": {}
            }
            
            # Добавляем данные по snapshot'ам
            for snapshot_num in self.snapshot_numbers:
                metrics_data["snapshots"][f"snapshot_{snapshot_num}"] = {
                    "timestamps_count": self.snapshot_timestamps_counts.get(snapshot_num, 0),
                    "videos_count": self.snapshot_videos_counts.get(snapshot_num, 0),
                    "time_interval_hours": self.snapshot_time_intervals.get(snapshot_num, 0),
                    "deltas_view_count": self.snapshot_deltas_view_count.get(snapshot_num, [])[:1000],
                    "deltas_like_count": self.snapshot_deltas_like_count.get(snapshot_num, [])[:1000],
                    "deltas_comment_count": self.snapshot_deltas_comment_count.get(snapshot_num, [])[:1000],
                    "deltas_subscriber_count": self.snapshot_deltas_subscriber_count.get(snapshot_num, [])[:1000],
                    "deltas_video_count": self.snapshot_deltas_video_count.get(snapshot_num, [])[:1000],
                    "deltas_view_count_channel": self.snapshot_deltas_view_count_channel.get(snapshot_num, [])[:1000],
                    "deltas_comments_count": self.snapshot_deltas_comments_count.get(snapshot_num, [])[:1000],
                    "percent_changes_view_count": self.snapshot_percent_changes_view_count.get(snapshot_num, [])[:1000],
                    "percent_changes_like_count": self.snapshot_percent_changes_like_count.get(snapshot_num, [])[:1000],
                    "percent_changes_comment_count": self.snapshot_percent_changes_comment_count.get(snapshot_num, [])[:1000],
                    "growth_rates_view_count": self.snapshot_growth_rates_view_count.get(snapshot_num, [])[:1000],
                    "growth_rates_like_count": self.snapshot_growth_rates_like_count.get(snapshot_num, [])[:1000],
                    "growth_rates_comment_count": self.snapshot_growth_rates_comment_count.get(snapshot_num, [])[:1000],
                    "deltas_engagement_rate": self.snapshot_deltas_engagement_rate.get(snapshot_num, [])[:1000],
            }
            
            # Добавляем метрики yt_dlp
            metrics_data["yt_dlp"] = {
                "videos_total_count": self.ytdlp_videos_total_count,
                "age_limit": self.ytdlp_age_limit[:1000] if len(self.ytdlp_age_limit) > 1000 else self.ytdlp_age_limit,
                "subtitles_ru_len": self.ytdlp_subtitles_ru_len[:1000] if len(self.ytdlp_subtitles_ru_len) > 1000 else self.ytdlp_subtitles_ru_len,
                "subtitles_en_len": self.ytdlp_subtitles_en_len[:1000] if len(self.ytdlp_subtitles_en_len) > 1000 else self.ytdlp_subtitles_en_len,
                "subtitles_ru_count": self.ytdlp_subtitles_ru_count,
                "subtitles_en_count": self.ytdlp_subtitles_en_count,
                "empty_subtitles_ru_count": self.ytdlp_empty_subtitles_ru_count,
                "empty_subtitles_en_count": self.ytdlp_empty_subtitles_en_count,
                "automatic_captions_ru_len": self.ytdlp_automatic_captions_ru_len[:1000] if len(self.ytdlp_automatic_captions_ru_len) > 1000 else self.ytdlp_automatic_captions_ru_len,
                "automatic_captions_en_len": self.ytdlp_automatic_captions_en_len[:1000] if len(self.ytdlp_automatic_captions_en_len) > 1000 else self.ytdlp_automatic_captions_en_len,
                "automatic_captions_ru_count": self.ytdlp_automatic_captions_ru_count,
                "automatic_captions_en_count": self.ytdlp_automatic_captions_en_count,
                "empty_automatic_captions_ru_count": self.ytdlp_empty_automatic_captions_ru_count,
                "empty_automatic_captions_en_count": self.ytdlp_empty_automatic_captions_en_count,
                "chapters_count": self.ytdlp_chapters_count,
                "videos_with_chapters": self.ytdlp_videos_with_chapters,
                "videos_without_chapters": self.ytdlp_videos_without_chapters,
                "formats_count": self.ytdlp_formats_count,
                "videos_with_formats": self.ytdlp_videos_with_formats,
                "videos_without_formats": self.ytdlp_videos_without_formats,
                "resolution_counts": dict(self.ytdlp_resolution_counts),
                "thumbnails_count": self.ytdlp_thumbnails_count,
                "videos_with_thumbnails": self.ytdlp_videos_with_thumbnails,
                "videos_without_thumbnails": self.ytdlp_videos_without_thumbnails,
                "duration_seconds": self.ytdlp_duration_seconds[:1000] if len(self.ytdlp_duration_seconds) > 1000 else self.ytdlp_duration_seconds,
                "extract_info_seconds": self.ytdlp_extract_info_seconds[:1000] if len(self.ytdlp_extract_info_seconds) > 1000 else self.ytdlp_extract_info_seconds,
                "captions_seconds_total": self.ytdlp_captions_seconds_total[:1000] if len(self.ytdlp_captions_seconds_total) > 1000 else self.ytdlp_captions_seconds_total,
                "total_seconds": self.ytdlp_total_seconds[:1000] if len(self.ytdlp_total_seconds) > 1000 else self.ytdlp_total_seconds,
            }
            
            # Сериализуем в JSON
            json_content = json.dumps(metrics_data, ensure_ascii=False, indent=2, default=str)
            json_bytes = json_content.encode('utf-8')
            
            # Формируем имя файла с временной меткой
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            filename = f"fetcher_metrics_{timestamp}.json"
            path_in_repo = f"metrics/{filename}"
            
            # Загружаем в HuggingFace
            api = HfApi(token=self.token)
            api.upload_file(
                path_or_fileobj=json_bytes,
                path_in_repo=path_in_repo,
                repo_id=self.repo_id,
                repo_type=self.repo_type,
                commit_message=f"Upload fetcher metrics: {timestamp}"
            )
            
            print(f"✓ Successfully uploaded fetcher metrics to {self.repo_id}/{path_in_repo}")
            return True
            
        except Exception as e:
            print(f"✗ Error uploading metrics to HuggingFace: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _load_meta_snapshot_data(self) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
        """Загружает все данные из meta_snapshot и возвращает словарь видео и категории."""
        meta_snapshot_dir = os.path.join(self.results_dir, "meta_snapshot")
        if not os.path.isdir(meta_snapshot_dir):
            logger.warning(f"meta_snapshot directory not found: {meta_snapshot_dir}")
            return {}, {}
        
        logger.info(f"Loading meta_snapshot data from: {meta_snapshot_dir}")
        videos_data: Dict[str, Dict[str, Any]] = {}  # video_id -> video_data
        videos_by_category: Dict[str, int] = defaultdict(int)  # category -> count
        
        # Пробуем загрузить data.json (объединенный файл)
        data_json_path = os.path.join(meta_snapshot_dir, "data.json")
        if os.path.exists(data_json_path):
            try:
                with open(data_json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Структура: {category: {interval: {video_id: video_data}, ...}, ...}
                for category, intervals in data.items():
                    if not isinstance(intervals, dict):
                        continue
                    for interval, videos in intervals.items():
                        if interval in ["_used_queries", "completed"]:
                            continue
                        if isinstance(videos, dict):
                            for video_id, video_data in videos.items():
                                if isinstance(video_data, dict):
                                    videos_data[video_id] = video_data
                                    videos_by_category[category] += 1
            except Exception as e:
                print(f"Error loading data.json: {e}")
        
        # Если data.json не существует или пуст, загружаем из отдельных файлов категорий
        if not videos_data:
            for file in os.listdir(meta_snapshot_dir):
                if file.endswith(".json") and file not in ["data.json", "progress.json", "sequence.json"]:
                    category = file[:-5]  # убираем .json
                    category_path = os.path.join(meta_snapshot_dir, file)
                    try:
                        with open(category_path, "r", encoding="utf-8") as f:
                            category_data = json.load(f)
                        # Структура: {interval: {video_id: video_data}, "_used_queries": [...], "completed": bool}
                        if isinstance(category_data, dict):
                            for interval, videos in category_data.items():
                                if interval in ["_used_queries", "completed"]:
                                    continue
                                if isinstance(videos, dict):
                                    for video_id, video_data in videos.items():
                                        if isinstance(video_data, dict):
                                            videos_data[video_id] = video_data
                                            videos_by_category[category] += 1
                    except Exception as e:
                        logger.error(f"Error loading category {category}: {e}")
        
        logger.info(f"Loaded {len(videos_data)} videos from meta_snapshot across {len(videos_by_category)} categories")
        return videos_data, videos_by_category
    
    def _load_snapshot_data(self, snapshot_num: int) -> Dict[str, Dict[str, Any]]:
        """Загружает все данные из snapshot_N."""
        snapshot_dir = os.path.join(self.results_dir, f"snapshot_{snapshot_num}")
        if not os.path.isdir(snapshot_dir):
            logger.warning(f"snapshot_{snapshot_num} directory not found: {snapshot_dir}")
            return {}
        
        logger.info(f"Loading snapshot_{snapshot_num} data from: {snapshot_dir}")
        videos_data: Dict[str, Dict[str, Any]] = {}  # video_id -> video_data
        
        # Загружаем из файлов timestamp'ов
        timestamp_files = []
        for file in os.listdir(snapshot_dir):
            if file.endswith(".json") and file not in ["progress.json", "target2ids.json"]:
                timestamp = file[:-5]  # убираем .json
                timestamp_path = os.path.join(snapshot_dir, file)
                timestamp_files.append(timestamp)
                try:
                    with open(timestamp_path, "r", encoding="utf-8") as f:
                        timestamp_data = json.load(f)
                    # Структура: {video_id: video_data}
                    if isinstance(timestamp_data, dict):
                        count_before = len(videos_data)
                        for video_id, video_data in timestamp_data.items():
                            if isinstance(video_data, dict):
                                videos_data[video_id] = video_data
                        logger.debug(f"Loaded {len(videos_data) - count_before} videos from timestamp {timestamp}")
                except Exception as e:
                    logger.error(f"Error loading timestamp {timestamp} from snapshot_{snapshot_num}: {e}")
        
        logger.info(f"Loaded {len(videos_data)} videos from snapshot_{snapshot_num} across {len(timestamp_files)} timestamps")
        return videos_data
    
    def _get_snapshot_numbers(self) -> List[int]:
        """Возвращает список номеров доступных snapshot'ов."""
        if not os.path.isdir(self.results_dir):
            return []
        
        snapshot_nums = []
        for item in os.listdir(self.results_dir):
            if item.startswith("snapshot_") and os.path.isdir(os.path.join(self.results_dir, item)):
                try:
                    num = int(item.split("_")[1])
                    snapshot_nums.append(num)
                except (ValueError, IndexError):
                    continue
        
        return sorted(snapshot_nums)
    
    def _load_yt_dlp_data(self) -> Dict[str, Dict[str, Any]]:
        """Загружает все данные из yt_dlp файлов data_{date}.json."""
        yt_dlp_dir = YT_DLP_RESULTS_DIR
        if not os.path.isdir(yt_dlp_dir):
            logger.warning(f"yt_dlp directory not found: {yt_dlp_dir}")
            return {}
        
        logger.info(f"Loading yt_dlp data from: {yt_dlp_dir}")
        videos_data: Dict[str, Dict[str, Any]] = {}  # video_id -> video_data
        
        # Загружаем все файлы data_{date}.json
        for file in os.listdir(yt_dlp_dir):
            if file.startswith("data_") and file.endswith(".json") and file != "progress.json":
                file_path = os.path.join(yt_dlp_dir, file)
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    # Структура: {video_id: video_data, "_metadata": {...}}
                    for video_id, video_data in data.items():
                        if video_id != "_metadata" and isinstance(video_data, dict):
                            videos_data[video_id] = video_data
                except Exception as e:
                    logger.error(f"Error loading yt_dlp file {file}: {e}")
        
        logger.info(f"Loaded {len(videos_data)} videos from yt_dlp")
        return videos_data
    
    def _init_yt_dlp_metrics(self):
        """Инициализирует все метрики для yt_dlp."""
        # Video-level metrics
        self.ytdlp_videos_total_count: int = 0
        self.ytdlp_age_limit: List[int] = []
        self.ytdlp_subtitles_ru_len: List[int] = []
        self.ytdlp_subtitles_en_len: List[int] = []
        self.ytdlp_subtitles_ru_count = 0
        self.ytdlp_subtitles_en_count = 0
        self.ytdlp_empty_subtitles_ru_count = 0
        self.ytdlp_empty_subtitles_en_count = 0
        
        self.ytdlp_automatic_captions_ru_len: List[int] = []
        self.ytdlp_automatic_captions_en_len: List[int] = []
        self.ytdlp_automatic_captions_ru_count = 0
        self.ytdlp_automatic_captions_en_count = 0
        self.ytdlp_empty_automatic_captions_ru_count = 0
        self.ytdlp_empty_automatic_captions_en_count = 0
        
        self.ytdlp_chapters_count = 0
        self.ytdlp_videos_with_chapters = 0
        self.ytdlp_videos_without_chapters = 0
        
        self.ytdlp_formats_count = 0
        self.ytdlp_videos_with_formats = 0
        self.ytdlp_videos_without_formats = 0
        self.ytdlp_resolution_counts: Dict[str, int] = {}
        
        self.ytdlp_thumbnails_count = 0
        self.ytdlp_videos_with_thumbnails = 0
        self.ytdlp_videos_without_thumbnails = 0
        
        self.ytdlp_duration_seconds: List[float] = []
        self.ytdlp_extract_info_seconds: List[float] = []
        self.ytdlp_captions_seconds_total: List[float] = []
        self.ytdlp_total_seconds: List[float] = []
    
    def _process_yt_dlp_metrics(self, videos: Dict[str, Dict[str, Any]]):
        """Обрабатывает метрики yt_dlp."""
        for video_id, video_data in videos.items():
            if not isinstance(video_data, dict):
                continue
            
            self.ytdlp_videos_total_count += 1
            
            # Age limit
            if "age_limit" in video_data:
                age_limit = video_data["age_limit"]
                if isinstance(age_limit, (int, float)):
                    self.ytdlp_age_limit.append(int(age_limit))
            
            # Subtitles
            subtitles = video_data.get("subtitles", {})
            if isinstance(subtitles, dict):
                for lang, subtitle_text in subtitles.items():
                    if lang == "ru":
                        self.ytdlp_subtitles_ru_count += 1
                        if subtitle_text:
                            self.ytdlp_subtitles_ru_len.append(len(subtitle_text))
                        else:
                            self.ytdlp_empty_subtitles_ru_count += 1
                    elif lang == "en":
                        self.ytdlp_subtitles_en_count += 1
                        if subtitle_text:
                            self.ytdlp_subtitles_en_len.append(len(subtitle_text))
                        else:
                            self.ytdlp_empty_subtitles_en_count += 1
            
            # Automatic captions
            automatic_captions = video_data.get("automatic_captions", {})
            if isinstance(automatic_captions, dict):
                for lang, caption_text in automatic_captions.items():
                    if lang == "ru":
                        self.ytdlp_automatic_captions_ru_count += 1
                        if caption_text:
                            self.ytdlp_automatic_captions_ru_len.append(len(caption_text))
                        else:
                            self.ytdlp_empty_automatic_captions_ru_count += 1
                    elif lang == "en":
                        self.ytdlp_automatic_captions_en_count += 1
                        if caption_text:
                            self.ytdlp_automatic_captions_en_len.append(len(caption_text))
                        else:
                            self.ytdlp_empty_automatic_captions_en_count += 1
            
            # Chapters
            chapters = video_data.get("chapters")
            if chapters:
                if isinstance(chapters, list):
                    self.ytdlp_chapters_count += len(chapters)
                    self.ytdlp_videos_with_chapters += 1
                else:
                    self.ytdlp_videos_without_chapters += 1
            else:
                self.ytdlp_videos_without_chapters += 1
            
            # Formats
            formats = video_data.get("formats", [])
            if formats and isinstance(formats, list):
                self.ytdlp_formats_count += len(formats)
                self.ytdlp_videos_with_formats += 1
                for fmt in formats:
                    if isinstance(fmt, dict) and "resolution" in fmt:
                        resolution = str(fmt["resolution"])
                        self.ytdlp_resolution_counts[resolution] = self.ytdlp_resolution_counts.get(resolution, 0) + 1
            else:
                self.ytdlp_videos_without_formats += 1
            
            # Thumbnails
            thumbnails = video_data.get("thumbnails_ytdlp", video_data.get("thumbnails", []))
            if thumbnails and isinstance(thumbnails, list):
                self.ytdlp_thumbnails_count += len(thumbnails)
                self.ytdlp_videos_with_thumbnails += 1
            else:
                self.ytdlp_videos_without_thumbnails += 1
            
            # Duration
            if "duration_seconds" in video_data:
                dur_sec = video_data["duration_seconds"]
                if isinstance(dur_sec, (int, float)):
                    self.ytdlp_duration_seconds.append(float(dur_sec))
            
            # Timings
            timings = video_data.get("timings_ytdlp", {})
            if isinstance(timings, dict):
                if "extract_info_seconds" in timings:
                    val = timings["extract_info_seconds"]
                    if isinstance(val, (int, float)):
                        self.ytdlp_extract_info_seconds.append(float(val))
                
                if "captions_seconds_total" in timings:
                    val = timings["captions_seconds_total"]
                    if isinstance(val, (int, float)):
                        self.ytdlp_captions_seconds_total.append(float(val))
                
                if "total_seconds" in timings:
                    val = timings["total_seconds"]
                    if isinstance(val, (int, float)):
                        self.ytdlp_total_seconds.append(float(val))
    
    def _collect_metrics(self):
        """Собирает все метрики из meta_snapshot и snapshot_N."""
        logger.info("Starting metrics collection...")
        
        # Инициализация всех метрик meta_snapshot
        self._init_meta_snapshot_metrics()
        
        # Загружаем данные meta_snapshot
        meta_videos, videos_by_category = self._load_meta_snapshot_data()
        self.meta_videos_by_category = videos_by_category
        
        # Обрабатываем метрики meta_snapshot
        logger.info(f"Processing {len(meta_videos)} meta_snapshot videos...")
        self._process_meta_snapshot_metrics(meta_videos)
        
        # Инициализация метрик snapshot_N
        self._init_snapshot_metrics()
        
        # Загружаем snapshot'ы
        snapshot_nums = self._get_snapshot_numbers()
        logger.info(f"Found {len(snapshot_nums)} snapshots: {snapshot_nums}")
        for snapshot_num in snapshot_nums:
            snapshot_videos = self._load_snapshot_data(snapshot_num)
            if snapshot_videos:
                logger.info(f"Processing snapshot_{snapshot_num} with {len(snapshot_videos)} videos...")
                self._process_snapshot_metrics(snapshot_num, snapshot_videos, meta_videos)
            else:
                logger.warning(f"No videos found in snapshot_{snapshot_num}")
        
        # Инициализация и обработка метрик yt_dlp
        self._init_yt_dlp_metrics()
        yt_dlp_videos = self._load_yt_dlp_data()
        if yt_dlp_videos:
            logger.info(f"Processing {len(yt_dlp_videos)} yt_dlp videos...")
            self._process_yt_dlp_metrics(yt_dlp_videos)
        else:
            logger.warning("No yt_dlp videos found")
        
        logger.info("Metrics collection completed")
    
    def _init_meta_snapshot_metrics(self):
        """Инициализирует все метрики для meta_snapshot."""
        # Общие метрики
        self.meta_videos_total = 0
        self.meta_videos_by_category: Dict[str, int] = defaultdict(int)
        
        # Title метрики
        self.meta_title_lengths: List[float] = []
        
        # Description метрики
        self.meta_description_lengths: List[float] = []
        self.meta_description_empty_count = 0
        self.meta_description_non_empty_count = 0
        
        # Tags метрики
        self.meta_tags_counts: List[float] = []
        self.meta_tags_all: List[str] = []  # все теги для топ-20
        self.meta_tags_per_video_lengths: List[float] = []  # длины отдельных тегов
        self.meta_videos_without_tags = 0
        self.meta_videos_with_tags = 0
        
        # Language метрики
        self.meta_languages: List[str] = []
        self.meta_videos_without_language = 0
        
        # ViewCount метрики
        self.meta_view_counts: List[float] = []
        
        # LikeCount метрики
        self.meta_like_counts: List[float] = []
        
        # CommentCount метрики
        self.meta_comment_counts: List[float] = []
        self.meta_videos_without_comments = 0
        
        # Thumbnails метрики
        self.meta_thumbnails_present = 0
        self.meta_thumbnails_missing = 0
        self.meta_thumbnail_sizes: List[Tuple[int, int]] = []  # (width, height)
        
        # Duration метрики
        self.meta_durations: List[float] = []  # в секундах
        
        # PublishedAt метрики
        self.meta_published_dates: List[datetime] = []
        
        # ChannelTitle метрики
        self.meta_channel_titles: List[str] = []
        
        # SubscriberCount метрики
        self.meta_subscriber_counts: List[float] = []
        
        # VideoCount метрики
        self.meta_video_counts: List[float] = []
        
        # ViewCount_channel метрики
        self.meta_view_count_channels: List[float] = []
        
        # Country метрики
        self.meta_countries: List[str] = []
        self.meta_videos_without_country = 0
        
        # Comments метрики (из массива comments)
        self.meta_comments_counts: List[float] = []  # количество комментариев на видео
        self.meta_comment_text_lengths: List[float] = []
        self.meta_comment_empty_text_count = 0  # количество комментариев с пустым текстом
        self.meta_comment_like_counts: List[float] = []
        self.meta_comment_reply_counts: List[float] = []
        self.meta_comment_authors: List[str] = []
        self.meta_comment_dates: List[datetime] = []  # даты комментариев
        self.meta_video_published_dates_for_comments: Dict[str, datetime] = {}  # для вычисления интервалов
        self.meta_comment_video_ids: List[str] = []  # video_id для каждого комментария (для временных интервалов)
    
    def _init_snapshot_metrics(self):
        """Инициализирует все метрики для snapshot_N."""
        self.snapshot_numbers: List[int] = []
        self.snapshot_timestamps_counts: Dict[int, int] = {}  # snapshot_num -> количество timestamp'ов
        self.snapshot_videos_counts: Dict[int, int] = {}  # snapshot_num -> количество видео
        self.snapshot_timestamp_videos_counts: Dict[int, Dict[str, int]] = {}  # snapshot_num -> {timestamp: count}
        self.snapshot_time_intervals: Dict[int, float] = {}  # snapshot_num -> интервал в часах от meta_snapshot
        
        # Дельты для каждого snapshot
        self.snapshot_deltas_view_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_like_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_comment_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_subscriber_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_video_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_view_count_channel: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_comments_count: Dict[int, List[float]] = defaultdict(list)
        
        # Проценты изменений
        self.snapshot_percent_changes_view_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_percent_changes_like_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_percent_changes_comment_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_percent_changes_subscriber_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_percent_changes_video_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_percent_changes_view_count_channel: Dict[int, List[float]] = defaultdict(list)
        
        # Скорости роста
        self.snapshot_growth_rates_view_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_growth_rates_like_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_growth_rates_comment_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_growth_rates_subscriber_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_growth_rates_video_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_growth_rates_view_count_channel: Dict[int, List[float]] = defaultdict(list)
        
        # Engagement rate дельты
        self.snapshot_deltas_engagement_rate: Dict[int, List[float]] = defaultdict(list)
        
        # Дельты комментариев (детальные)
        self.snapshot_deltas_comment_text_length: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_comment_like_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_deltas_comment_reply_count: Dict[int, List[float]] = defaultdict(list)
        self.snapshot_new_comment_authors: Dict[int, set] = defaultdict(set)  # новые авторы
        self.snapshot_meta_comment_authors: Dict[int, set] = defaultdict(set)  # авторы из meta
        
        # Топ-20 видео/каналов (для хранения video_id/channel_id с дельтами)
        self.snapshot_top_view_deltas: Dict[int, List[Tuple[str, float]]] = defaultdict(list)  # (video_id, delta)
        self.snapshot_top_like_deltas: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
        self.snapshot_top_comment_deltas: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
        self.snapshot_top_subscriber_deltas: Dict[int, List[Tuple[str, float]]] = defaultdict(list)  # (channel_id, delta)
        self.snapshot_top_engagement_deltas: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
        self.snapshot_top_new_comments: Dict[int, List[Tuple[str, float]]] = defaultdict(list)
        
        # Для корреляций и временных метрик
        self.snapshot_video_ages: Dict[int, List[float]] = defaultdict(list)  # возраст видео в днях
        self.snapshot_channel_categories: Dict[int, Dict[str, str]] = defaultdict(dict)  # video_id -> category
        self.snapshot_video_ids_with_deltas: Dict[int, List[str]] = defaultdict(list)  # порядок video_id для сопоставления с дельтами
        self.snapshot_video_published_intervals: Dict[int, Dict[str, str]] = defaultdict(dict)  # video_id -> interval для группировки
    
    def _parse_duration(self, duration_str: Optional[str]) -> Optional[float]:
        """Парсит ISO 8601 duration в секунды."""
        if not duration_str:
            return None
        try:
            # Формат: PT1H2M10S или PT10M30S
            if not duration_str.startswith("PT"):
                return None
            duration_str = duration_str[2:]  # убираем PT
            total_seconds = 0.0
            current_num = ""
            for char in duration_str:
                if char.isdigit():
                    current_num += char
                elif char == "H":
                    total_seconds += float(current_num) * 3600
                    current_num = ""
                elif char == "M":
                    total_seconds += float(current_num) * 60
                    current_num = ""
                elif char == "S":
                    total_seconds += float(current_num)
                    current_num = ""
            return total_seconds
        except Exception:
            return None
    
    def _process_meta_snapshot_metrics(self, videos: Dict[str, Dict[str, Any]]):
        """Обрабатывает метрики meta_snapshot."""
        for video_id, video_data in videos.items():
            if not isinstance(video_data, dict):
                            continue
            
            self.meta_videos_total += 1
            
            # Title (1.1)
            title = video_data.get("title", "")
            if title:
                self.meta_title_lengths.append(float(len(title)))
            
            # Description (1.2)
            description = video_data.get("description", "")
            if description:
                self.meta_description_lengths.append(float(len(description)))
                self.meta_description_non_empty_count += 1
            else:
                self.meta_description_empty_count += 1
            
            # Tags (1.3)
            tags = video_data.get("tags", [])
            if isinstance(tags, list):
                tag_count = len(tags)
                self.meta_tags_counts.append(float(tag_count))
                if tag_count == 0:
                    self.meta_videos_without_tags += 1
                else:
                    self.meta_videos_with_tags += 1
                    for tag in tags:
                        if isinstance(tag, str):
                            self.meta_tags_all.append(tag)
                            self.meta_tags_per_video_lengths.append(float(len(tag)))
            else:
                self.meta_videos_without_tags += 1
            
            # Language (1.4)
            language = video_data.get("language")
            if language:
                self.meta_languages.append(str(language))
            else:
                self.meta_videos_without_language += 1
            
            # ViewCount (1.5)
            view_count = _safe_convert_to_number(video_data.get("viewCount"))
            if view_count is not None:
                self.meta_view_counts.append(view_count)
            
            # LikeCount (1.6)
            like_count = _safe_convert_to_number(video_data.get("likeCount"))
            if like_count is not None:
                self.meta_like_counts.append(like_count)
            
            # CommentCount (1.7)
            comment_count = _safe_convert_to_number(video_data.get("commentCount"))
            if comment_count is not None:
                self.meta_comment_counts.append(comment_count)
            else:
                self.meta_videos_without_comments += 1
            
            # Thumbnails (1.9)
            thumbnails = video_data.get("thumbnails", {})
            if isinstance(thumbnails, dict) and thumbnails:
                self.meta_thumbnails_present += 1
                # Пробуем получить размеры
                for thumb_key, thumb_data in thumbnails.items():
                    if isinstance(thumb_data, dict):
                        width = _safe_convert_to_number(thumb_data.get("width"))
                        height = _safe_convert_to_number(thumb_data.get("height"))
                        if width is not None and height is not None:
                            self.meta_thumbnail_sizes.append((int(width), int(height)))
                            break  # берем первый доступный размер
            else:
                self.meta_thumbnails_missing += 1
            
            # Duration (1.10)
            duration = self._parse_duration(video_data.get("duration"))
            if duration is not None:
                self.meta_durations.append(duration)
            
            # PublishedAt (1.11)
            published_at = _parse_iso_datetime(video_data.get("publishedAt"))
            if published_at:
                self.meta_published_dates.append(published_at)
                self.meta_video_published_dates_for_comments[video_id] = published_at
            
            # ChannelTitle (1.12)
            channel_title = video_data.get("channelTitle")
            if channel_title:
                self.meta_channel_titles.append(str(channel_title))
            
            # SubscriberCount (1.13)
            subscriber_count = _safe_convert_to_number(video_data.get("subscriberCount"))
            if subscriber_count is not None:
                self.meta_subscriber_counts.append(subscriber_count)
            
            # VideoCount (1.14)
            video_count = _safe_convert_to_number(video_data.get("videoCount"))
            if video_count is not None:
                self.meta_video_counts.append(video_count)
            
            # ViewCount_channel (1.15)
            view_count_channel = _safe_convert_to_number(video_data.get("viewCount_channel"))
            if view_count_channel is not None:
                self.meta_view_count_channels.append(view_count_channel)
            
            # Country (1.16)
            country = video_data.get("country")
            if country:
                self.meta_countries.append(str(country))
            else:
                self.meta_videos_without_country += 1
            
            # Comments (1.17) - из массива comments
            comments = video_data.get("comments", [])
            if isinstance(comments, list):
                comment_count = len(comments)
                self.meta_comments_counts.append(float(comment_count))
                for comment in comments:
                    if isinstance(comment, dict):
                        # Длина текста комментария
                        comment_text = comment.get("text", "")
                        if comment_text:
                            self.meta_comment_text_lengths.append(float(len(comment_text)))
                        else:
                            self.meta_comment_empty_text_count += 1
                        
                        # Лайки на комментарий
                        comment_likes = _safe_convert_to_number(comment.get("likeCount"))
                        if comment_likes is not None:
                            self.meta_comment_like_counts.append(comment_likes)
                        
                        # Ответы на комментарий
                        comment_replies = _safe_convert_to_number(comment.get("repliesCount"))
                        if comment_replies is not None:
                            self.meta_comment_reply_counts.append(comment_replies)
                        
                        # Автор комментария
                        author = comment.get("authorDisplayName") or comment.get("author")
                        if author:
                            self.meta_comment_authors.append(str(author))
                        
                        # Дата комментария
                        comment_date = _parse_iso_datetime(comment.get("publishedAt"))
                        if comment_date:
                            self.meta_comment_dates.append(comment_date)
                            self.meta_comment_video_ids.append(video_id)  # связываем комментарий с видео
    
    def _process_snapshot_metrics(self, snapshot_num: int, snapshot_videos: Dict[str, Dict[str, Any]], meta_videos: Dict[str, Dict[str, Any]]):
        """Обрабатывает метрики snapshot_N."""
        logger.debug(f"Processing snapshot_{snapshot_num} metrics...")
        self.snapshot_numbers.append(snapshot_num)
        self.snapshot_videos_counts[snapshot_num] = len(snapshot_videos)
        
        matched_videos = 0
        unmatched_videos = 0
        
        # Подсчитываем количество timestamp'ов
        snapshot_dir = os.path.join(self.results_dir, f"snapshot_{snapshot_num}")
        timestamp_count = 0
        timestamp_videos: Dict[str, int] = {}
        if os.path.isdir(snapshot_dir):
            for file in os.listdir(snapshot_dir):
                if file.endswith(".json") and file not in ["progress.json", "target2ids.json"]:
                    timestamp = file[:-5]
                    timestamp_count += 1
                    timestamp_path = os.path.join(snapshot_dir, file)
                    try:
                        with open(timestamp_path, "r", encoding="utf-8") as f:
                            timestamp_data = json.load(f)
                            if isinstance(timestamp_data, dict):
                                timestamp_videos[timestamp] = len(timestamp_data)
                    except Exception:
                        pass
        self.snapshot_timestamps_counts[snapshot_num] = timestamp_count
        self.snapshot_timestamp_videos_counts[snapshot_num] = timestamp_videos
        
        # Вычисляем временной интервал от meta_snapshot (приблизительно)
        # Берем среднюю дату публикации из meta_videos
        if self.meta_published_dates:
            avg_meta_date = sum([d.timestamp() for d in self.meta_published_dates]) / len(self.meta_published_dates)
            # Для snapshot берем текущее время (или можно использовать timestamp из имени файла)
            snapshot_time = datetime.now().timestamp()
            interval_hours = (snapshot_time - avg_meta_date) / 3600.0
            self.snapshot_time_intervals[snapshot_num] = interval_hours
        
        # Вычисляем дельты для каждого видео
        for video_id, snapshot_video_data in snapshot_videos.items():
            if not isinstance(snapshot_video_data, dict):
                unmatched_videos += 1
                continue
            
            meta_video_data = meta_videos.get(video_id)
            if not isinstance(meta_video_data, dict):
                unmatched_videos += 1
                continue  # Пропускаем видео, которых нет в meta_snapshot
            
            matched_videos += 1
            
            # Дельты viewCount (2.2)
            meta_view = _safe_convert_to_number(meta_video_data.get("viewCount"))
            snap_view = _safe_convert_to_number(snapshot_video_data.get("viewCount"))
            if meta_view is not None and snap_view is not None:
                delta = snap_view - meta_view
                self.snapshot_deltas_view_count[snapshot_num].append(delta)
                self.snapshot_top_view_deltas[snapshot_num].append((video_id, delta))
                if meta_view > 0:
                    percent_change = (delta / meta_view) * 100
                    self.snapshot_percent_changes_view_count[snapshot_num].append(percent_change)
                    if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                        growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                        self.snapshot_growth_rates_view_count[snapshot_num].append(growth_rate)
            
            # Возраст видео для временных метрик
            meta_published = _parse_iso_datetime(meta_video_data.get("publishedAt"))
            if meta_published and self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                # Возраст видео в днях на момент snapshot
                age_days = self.snapshot_time_intervals[snapshot_num] / 24.0
                self.snapshot_video_ages[snapshot_num].append(age_days)
            
            # Временной интервал публикации для группировки (2.12.1-4)
            if meta_published:
                now = datetime.now()
                delta = now - meta_published
                if delta.days < 1:
                    interval = "less-1day"
                elif delta.days < 7:
                    interval = "1day-1week"
                elif delta.days < 30:
                    interval = "1week-1month"
                elif delta.days < 365:
                    interval = "1month-1year"
                else:
                    interval = ">1year"
                self.snapshot_video_published_intervals[snapshot_num][video_id] = interval
            
            # Категория канала для группировки
            meta_sub = _safe_convert_to_number(meta_video_data.get("subscriberCount"))
            if meta_sub is not None:
                if meta_sub < 1000:
                    category = "micro"
                elif meta_sub < 10000:
                    category = "small"
                elif meta_sub < 100000:
                    category = "medium"
                elif meta_sub < 1000000:
                    category = "large"
                else:
                    category = "mega"
                self.snapshot_channel_categories[snapshot_num][video_id] = category
            
            # Сохраняем video_id для правильного сопоставления
            self.snapshot_video_ids_with_deltas[snapshot_num].append(video_id)
            
            # Дельты likeCount (2.3)
            meta_like = _safe_convert_to_number(meta_video_data.get("likeCount"))
            snap_like = _safe_convert_to_number(snapshot_video_data.get("likeCount"))
            if meta_like is not None and snap_like is not None:
                delta = snap_like - meta_like
                self.snapshot_deltas_like_count[snapshot_num].append(delta)
                self.snapshot_top_like_deltas[snapshot_num].append((video_id, delta))
                if meta_like > 0:
                    percent_change = (delta / meta_like) * 100
                    self.snapshot_percent_changes_like_count[snapshot_num].append(percent_change)
                    if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                        growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                        self.snapshot_growth_rates_like_count[snapshot_num].append(growth_rate)
            
            # Дельты commentCount (2.4)
            meta_comment = _safe_convert_to_number(meta_video_data.get("commentCount"))
            snap_comment = _safe_convert_to_number(snapshot_video_data.get("commentCount"))
            if meta_comment is not None and snap_comment is not None:
                delta = snap_comment - meta_comment
                self.snapshot_deltas_comment_count[snapshot_num].append(delta)
                self.snapshot_top_comment_deltas[snapshot_num].append((video_id, delta))
                if meta_comment > 0:
                    percent_change = (delta / meta_comment) * 100
                    self.snapshot_percent_changes_comment_count[snapshot_num].append(percent_change)
                    if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                        growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                        self.snapshot_growth_rates_comment_count[snapshot_num].append(growth_rate)
            
            # Дельты subscriberCount (2.5)
            meta_sub = _safe_convert_to_number(meta_video_data.get("subscriberCount"))
            snap_sub = _safe_convert_to_number(snapshot_video_data.get("subscriberCount"))
            if meta_sub is not None and snap_sub is not None:
                delta = snap_sub - meta_sub
                self.snapshot_deltas_subscriber_count[snapshot_num].append(delta)
                if meta_sub > 0:
                    percent_change = (delta / meta_sub) * 100
                    self.snapshot_percent_changes_subscriber_count[snapshot_num].append(percent_change)
                if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                    growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                    self.snapshot_growth_rates_subscriber_count[snapshot_num].append(growth_rate)
                # Для топ-20 каналов используем channelTitle как идентификатор
                channel_title = meta_video_data.get("channelTitle") or video_id
                self.snapshot_top_subscriber_deltas[snapshot_num].append((channel_title, delta))
            
            # Дельты videoCount (2.6)
            meta_vid_count = _safe_convert_to_number(meta_video_data.get("videoCount"))
            snap_vid_count = _safe_convert_to_number(snapshot_video_data.get("videoCount"))
            if meta_vid_count is not None and snap_vid_count is not None:
                delta = snap_vid_count - meta_vid_count
                self.snapshot_deltas_video_count[snapshot_num].append(delta)
                if meta_vid_count > 0:
                    percent_change = (delta / meta_vid_count) * 100
                    self.snapshot_percent_changes_video_count[snapshot_num].append(percent_change)
                if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                    growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                    self.snapshot_growth_rates_video_count[snapshot_num].append(growth_rate)
            
            # Дельты viewCount_channel (2.7)
            meta_view_ch = _safe_convert_to_number(meta_video_data.get("viewCount_channel"))
            snap_view_ch = _safe_convert_to_number(snapshot_video_data.get("viewCount_channel"))
            if meta_view_ch is not None and snap_view_ch is not None:
                delta = snap_view_ch - meta_view_ch
                self.snapshot_deltas_view_count_channel[snapshot_num].append(delta)
                if meta_view_ch > 0:
                    percent_change = (delta / meta_view_ch) * 100
                    self.snapshot_percent_changes_view_count_channel[snapshot_num].append(percent_change)
                if self.snapshot_time_intervals.get(snapshot_num, 0) > 0:
                    growth_rate = delta / self.snapshot_time_intervals[snapshot_num]
                    self.snapshot_growth_rates_view_count_channel[snapshot_num].append(growth_rate)
            
            # Дельты comments (2.8) - из массива comments
            meta_comments = meta_video_data.get("comments", [])
            snap_comments = snapshot_video_data.get("comments", [])
            if isinstance(meta_comments, list) and isinstance(snap_comments, list):
                delta = len(snap_comments) - len(meta_comments)
                self.snapshot_deltas_comments_count[snapshot_num].append(float(delta))
                self.snapshot_top_new_comments[snapshot_num].append((video_id, float(delta)))
                
                # Собираем авторов из meta и snapshot
                meta_authors = set()
                for comment in meta_comments:
                    if isinstance(comment, dict):
                        author = comment.get("authorDisplayName") or comment.get("author")
                        if author:
                            meta_authors.add(str(author))
                            self.snapshot_meta_comment_authors[snapshot_num].add(str(author))
                
                snap_authors = set()
                for comment in snap_comments:
                    if isinstance(comment, dict):
                        author = comment.get("authorDisplayName") or comment.get("author")
                        if author:
                            snap_authors.add(str(author))
                
                # Новые авторы - те, кто есть в snapshot, но нет в meta
                new_authors = snap_authors - meta_authors
                self.snapshot_new_comment_authors[snapshot_num].update(new_authors)
                
                # Дельты для текста, лайков и ответов комментариев
                meta_text_lengths = [len(c.get("text", "")) for c in meta_comments if isinstance(c, dict)]
                snap_text_lengths = [len(c.get("text", "")) for c in snap_comments if isinstance(c, dict)]
                if meta_text_lengths and snap_text_lengths:
                    avg_meta_text = sum(meta_text_lengths) / len(meta_text_lengths)
                    avg_snap_text = sum(snap_text_lengths) / len(snap_text_lengths)
                    self.snapshot_deltas_comment_text_length[snapshot_num].append(avg_snap_text - avg_meta_text)
                
                meta_likes = [_safe_convert_to_number(c.get("likeCount")) or 0 for c in meta_comments if isinstance(c, dict)]
                snap_likes = [_safe_convert_to_number(c.get("likeCount")) or 0 for c in snap_comments if isinstance(c, dict)]
                if meta_likes and snap_likes:
                    avg_meta_likes = sum(meta_likes) / len(meta_likes)
                    avg_snap_likes = sum(snap_likes) / len(snap_likes)
                    self.snapshot_deltas_comment_like_count[snapshot_num].append(avg_snap_likes - avg_meta_likes)
                
                meta_replies = [_safe_convert_to_number(c.get("repliesCount")) or 0 for c in meta_comments if isinstance(c, dict)]
                snap_replies = [_safe_convert_to_number(c.get("repliesCount")) or 0 for c in snap_comments if isinstance(c, dict)]
                if meta_replies and snap_replies:
                    avg_meta_replies = sum(meta_replies) / len(meta_replies)
                    avg_snap_replies = sum(snap_replies) / len(snap_replies)
                    self.snapshot_deltas_comment_reply_count[snapshot_num].append(avg_snap_replies - avg_meta_replies)
            
            # Engagement rate дельты (2.10)
            meta_view = _safe_convert_to_number(meta_video_data.get("viewCount"))
            snap_view = _safe_convert_to_number(snapshot_video_data.get("viewCount"))
            meta_like = _safe_convert_to_number(meta_video_data.get("likeCount"))
            snap_like = _safe_convert_to_number(snapshot_video_data.get("likeCount"))
            meta_comm = _safe_convert_to_number(meta_video_data.get("commentCount"))
            snap_comm = _safe_convert_to_number(snapshot_video_data.get("commentCount"))
            
            if meta_view and meta_view > 0 and snap_view and snap_view > 0:
                meta_engagement = ((meta_like or 0) + (meta_comm or 0)) / meta_view
                snap_engagement = ((snap_like or 0) + (snap_comm or 0)) / snap_view
                delta_engagement = snap_engagement - meta_engagement
                self.snapshot_deltas_engagement_rate[snapshot_num].append(delta_engagement)
                self.snapshot_top_engagement_deltas[snapshot_num].append((video_id, delta_engagement))
        
        logger.info(f"snapshot_{snapshot_num}: matched {matched_videos} videos, unmatched {unmatched_videos} videos")
        logger.debug(f"snapshot_{snapshot_num} metrics: "
                    f"view_deltas={len(self.snapshot_deltas_view_count.get(snapshot_num, []))}, "
                    f"top_view_deltas={len(self.snapshot_top_view_deltas.get(snapshot_num, []))}, "
                    f"percent_changes_view={len(self.snapshot_percent_changes_view_count.get(snapshot_num, []))}, "
                    f"growth_rates_view={len(self.snapshot_growth_rates_view_count.get(snapshot_num, []))}, "
                    f"top_subscriber_deltas={len(self.snapshot_top_subscriber_deltas.get(snapshot_num, []))}, "
                    f"new_comment_authors={len(self.snapshot_new_comment_authors.get(snapshot_num, set()))}")

    def collect(self):
        """Generate Prometheus metrics from collected data."""
        # Re-collect metrics on each scrape to get fresh data
        self._collect_metrics()

        # Helper functions
        def emit_stats(metric_base: str, desc: str, values: List[float], include_median: bool = False):
            """Emit min/max/mean/count stats, optionally with median."""
            if not values:
                return
            vmin = min(values)
            vmax = max(values)
            vmean = sum(values) / len(values)
            stats = GaugeMetricFamily(
                f"{metric_base}",
                f"{desc} (мин/макс/среднее)",
                labels=["stat"]
            )
            stats.add_metric(["min"], vmin)
            stats.add_metric(["max"], vmax)
            stats.add_metric(["mean"], vmean)
            if include_median:
                vmedian = statistics.median(values)
                stats.add_metric(["median"], vmedian)
            yield stats
            yield GaugeMetricFamily(f"{metric_base}_count", f"Количество значений {desc}", len(values))

        def emit_distribution(metric_base: str, desc: str, values: List[float], bins: List[float]):
            """Emit distribution metrics by bins."""
            if not values or not bins:
                logger.debug(f"emit_distribution: Skipping {metric_base} - values={len(values) if values else 0}, bins={len(bins) if bins else 0}")
                return
            logger.debug(f"emit_distribution: Generating {metric_base}_distribution with {len(values)} values, {len(bins)} bins")
            dist = CounterMetricFamily(
                f"{metric_base}_distribution",
                f"Распределение {desc}",
                labels=["range"]
            )
            # Sort bins
            sorted_bins = sorted(bins)
            # Count values in each bin
            bin_counts = defaultdict(int)
            for val in values:
                for i, bin_val in enumerate(sorted_bins):
                    if val <= bin_val:
                        bin_counts[f"<={bin_val}"] += 1
                        break
                else:
                    bin_counts[f">{sorted_bins[-1]}"] += 1
            for range_label, count in bin_counts.items():
                dist.add_metric([range_label], count)
            logger.debug(f"emit_distribution: Generated {metric_base}_distribution with {len(bin_counts)} bins")
            yield dist

        # ========== META_SNAPSHOT METRICS ==========
        
        # 1.18.1 Общее количество видео
        yield GaugeMetricFamily(
            "fetcher_meta_videos_total",
            "Общее количество видео в meta_snapshot",
            self.meta_videos_total
        )
        
        # 1.1 Title метрики
        if self.meta_title_lengths:
            yield from emit_stats("fetcher_meta_title_length", "Длина заголовка (символов)", self.meta_title_lengths, include_median=True)
            # Распределение длин title по диапазонам (0-100, 100-200, и т.д.)
            title_bins = [10, 20, 30, 50, 70, 100, 150, 200, 300, 500, 1000]
            yield from emit_distribution("fetcher_meta_title_length", "Длина заголовка", self.meta_title_lengths, title_bins)
        
        # 1.2 Description метрики
        if self.meta_description_lengths:
            yield from emit_stats("fetcher_meta_description_length", "Длина описания (символов)", self.meta_description_lengths, include_median=True)
            # Распределение длин description
            desc_bins = [50, 100, 200, 500, 1000, 2000, 3000, 5000, 10000, 20000]
            yield from emit_distribution("fetcher_meta_description_length", "Длина описания", self.meta_description_lengths, desc_bins)
        
        desc_presence = CounterMetricFamily(
            "fetcher_meta_description_presence_total",
            "Количество видео по наличию описания",
            labels=["presence"]
        )
        desc_presence.add_metric(["empty"], self.meta_description_empty_count)
        desc_presence.add_metric(["non_empty"], self.meta_description_non_empty_count)
        yield desc_presence
        
        # 1.3 Tags метрики
        if self.meta_tags_counts:
            yield from emit_stats("fetcher_meta_tags_count", "Количество тегов на видео", self.meta_tags_counts, include_median=True)
            # Распределение количества тегов
            tags_bins = [0, 2, 5, 10, 15, 20, 30, 40, 50, 100]
            yield from emit_distribution("fetcher_meta_tags_count", "Количество тегов", self.meta_tags_counts, tags_bins)
        
        tags_presence = CounterMetricFamily(
            "fetcher_meta_tags_presence_total",
            "Количество видео по наличию тегов",
            labels=["presence"]
        )
        tags_presence.add_metric(["with_tags"], self.meta_videos_with_tags)
        tags_presence.add_metric(["without_tags"], self.meta_videos_without_tags)
        yield tags_presence
        
        # Топ-20 самых частых тегов
        if self.meta_tags_all:
            tag_counter = Counter(self.meta_tags_all)
            top_tags = tag_counter.most_common(20)
            top_tags_metric = GaugeMetricFamily(
                "fetcher_meta_tags_top20",
                "Топ-20 самых частых тегов",
                labels=["tag"]
            )
            for tag, count in top_tags:
                top_tags_metric.add_metric([tag], count)
            yield top_tags_metric
        
        # Средняя длина одного тега
        if self.meta_tags_per_video_lengths:
            yield from emit_stats("fetcher_meta_tag_length", "Длина отдельного тега (символов)", self.meta_tags_per_video_lengths)
        
        # 1.4 Language метрики
        if self.meta_languages:
            lang_counter = Counter(self.meta_languages)
            lang_dist = CounterMetricFamily(
                "fetcher_meta_language_distribution_total",
                "Распределение языков",
                labels=["language"]
            )
            for lang, count in lang_counter.items():
                lang_dist.add_metric([lang], count)
            yield lang_dist
        
            yield GaugeMetricFamily(
            "fetcher_meta_language_missing_total",
            "Количество видео без указания языка",
            self.meta_videos_without_language
        )
        
        # 1.5 ViewCount метрики
        if self.meta_view_counts:
            yield from emit_stats("fetcher_meta_view_count", "Количество просмотров видео", self.meta_view_counts, include_median=True)
            # Распределение просмотров (логарифмическая шкала)
            view_bins = [0, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 10000000]
            yield from emit_distribution("fetcher_meta_view_count", "Количество просмотров", self.meta_view_counts, view_bins)
            # 1.5.5 Количество видео с просмотрами выше/ниже медианы
            if len(self.meta_view_counts) > 0:
                median_views = statistics.median(self.meta_view_counts)
                above_median = sum(1 for v in self.meta_view_counts if v > median_views)
                below_median = sum(1 for v in self.meta_view_counts if v < median_views)
                equal_median = sum(1 for v in self.meta_view_counts if v == median_views)
                view_median_dist = CounterMetricFamily(
                    "fetcher_meta_view_count_median_distribution_total",
                    "Количество видео по отношению просмотров к медиане",
                    labels=["position"]
                )
                view_median_dist.add_metric(["above"], above_median)
                view_median_dist.add_metric(["below"], below_median)
                view_median_dist.add_metric(["equal"], equal_median)
                yield view_median_dist
        
        # 1.6 LikeCount метрики
        if self.meta_like_counts:
            yield from emit_stats("fetcher_meta_like_count", "Количество лайков видео", self.meta_like_counts, include_median=True)
            # Распределение лайков
            like_bins = [0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 1000000]
            yield from emit_distribution("fetcher_meta_like_count", "Количество лайков", self.meta_like_counts, like_bins)
            # Соотношение лайков к просмотрам
            if self.meta_view_counts and len(self.meta_like_counts) == len(self.meta_view_counts):
                like_view_ratios = []
                for i in range(min(len(self.meta_like_counts), len(self.meta_view_counts))):
                    if self.meta_view_counts[i] > 0:
                        like_view_ratios.append(self.meta_like_counts[i] / self.meta_view_counts[i])
                if like_view_ratios:
                    yield from emit_stats("fetcher_meta_like_view_ratio", "Соотношение лайков к просмотрам", like_view_ratios, include_median=True)
        
        # 1.7 CommentCount метрики
        if self.meta_comment_counts:
            yield from emit_stats("fetcher_meta_comment_count", "Количество комментариев видео", self.meta_comment_counts, include_median=True)
            # Распределение комментариев
            comment_bins = [0, 1, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]
            yield from emit_distribution("fetcher_meta_comment_count", "Количество комментариев", self.meta_comment_counts, comment_bins)
            # Соотношение комментариев к просмотрам
            if self.meta_view_counts and len(self.meta_comment_counts) == len(self.meta_view_counts):
                comment_view_ratios = []
                for i in range(min(len(self.meta_comment_counts), len(self.meta_view_counts))):
                    if self.meta_view_counts[i] > 0:
                        comment_view_ratios.append(self.meta_comment_counts[i] / self.meta_view_counts[i])
                if comment_view_ratios:
                    yield from emit_stats("fetcher_meta_comment_view_ratio", "Соотношение комментариев к просмотрам", comment_view_ratios, include_median=True)
        
            yield GaugeMetricFamily(
            "fetcher_meta_comment_count_missing_total",
            "Количество видео без комментариев",
            self.meta_videos_without_comments
        )
        
        # 1.9 Thumbnails метрики
        thumbs_presence = CounterMetricFamily(
            "fetcher_meta_thumbnails_presence_total",
            "Количество видео по наличию превью",
            labels=["presence"]
        )
        thumbs_presence.add_metric(["present"], self.meta_thumbnails_present)
        thumbs_presence.add_metric(["missing"], self.meta_thumbnails_missing)
        yield thumbs_presence
        
        # Распределение размеров thumbnails
        if self.meta_thumbnail_sizes:
            thumb_size_counter = Counter(self.meta_thumbnail_sizes)
            thumb_size_metric = GaugeMetricFamily(
                "fetcher_meta_thumbnail_size_distribution_total",
                "Распределение размеров превью",
                labels=["size"]
            )
            for size, count in thumb_size_counter.items():
                thumb_size_metric.add_metric([f"{size[0]}x{size[1]}"], count)
            yield thumb_size_metric
        
        # 1.10 Duration метрики
        if self.meta_durations:
            yield from emit_stats("fetcher_meta_duration_seconds", "Длительность видео (секунды)", self.meta_durations, include_median=True)
            # Распределение по диапазонам: 0-60с, 1-5мин, 5-15мин, 15-60мин, >60мин
            duration_range_counts = defaultdict(int)
            for duration in self.meta_durations:
                if duration <= 60:
                    duration_range_counts["0-60s"] += 1
                elif duration <= 300:  # 5 минут
                    duration_range_counts["1-5min"] += 1
                elif duration <= 900:  # 15 минут
                    duration_range_counts["5-15min"] += 1
                elif duration <= 3600:  # 60 минут
                    duration_range_counts["15-60min"] += 1
                else:
                    duration_range_counts[">60min"] += 1
            duration_ranges = CounterMetricFamily(
                "fetcher_meta_duration_range_distribution_total",
                "Распределение длительностей видео по диапазонам",
                labels=["range"]
            )
            for range_label, count in duration_range_counts.items():
                duration_ranges.add_metric([range_label], count)
            yield duration_ranges
        
        # 1.11 PublishedAt метрики
        if self.meta_published_dates:
            # Распределение по временным интервалам
            now = datetime.now()
            time_interval_counts = defaultdict(int)
            for pub_date in self.meta_published_dates:
                delta = now - pub_date
                if delta.days < 1:
                    time_interval_counts["less-1day"] += 1
                elif delta.days < 7:
                    time_interval_counts["1day-1week"] += 1
                elif delta.days < 30:
                    time_interval_counts["1week-1month"] += 1
                elif delta.days < 365:
                    time_interval_counts["1month-1year"] += 1
                else:
                    time_interval_counts[">1year"] += 1
            time_intervals = CounterMetricFamily(
                "fetcher_meta_published_time_interval_total",
                "Распределение видео по времени с момента публикации",
                labels=["interval"]
            )
            for interval_label, count in time_interval_counts.items():
                time_intervals.add_metric([interval_label], count)
            yield time_intervals
            
            # Распределение по дням недели
            weekday_counter = Counter([d.weekday() for d in self.meta_published_dates])
            weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            weekday_metric = CounterMetricFamily(
                "fetcher_meta_published_weekday_total",
                "Распределение видео по дню недели публикации",
                labels=["weekday"]
            )
            for weekday_num, count in weekday_counter.items():
                weekday_metric.add_metric([weekday_names[weekday_num]], count)
            yield weekday_metric
            
            # Распределение по часам
            hour_counter = Counter([d.hour for d in self.meta_published_dates])
            hour_metric = CounterMetricFamily(
                "fetcher_meta_published_hour_total",
                "Распределение видео по часу публикации",
                labels=["hour"]
            )
            for hour, count in hour_counter.items():
                hour_metric.add_metric([str(hour)], count)
            yield hour_metric
            
            # Распределение по месяцам
            month_counter = Counter([d.month for d in self.meta_published_dates])
            month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            month_metric = CounterMetricFamily(
                "fetcher_meta_published_month_total",
                "Распределение видео по месяцу публикации",
                labels=["month"]
            )
            for month_num, count in month_counter.items():
                month_metric.add_metric([month_names[month_num - 1]], count)
            yield month_metric
        
        # 1.12 ChannelTitle метрики
        if self.meta_channel_titles:
            channel_counter = Counter(self.meta_channel_titles)
            yield GaugeMetricFamily(
                "fetcher_meta_unique_channels_total",
                "Количество уникальных каналов",
                len(channel_counter)
            )
            # Топ-20 каналов
            top_channels_list = channel_counter.most_common(20)
            top_channels_metric = GaugeMetricFamily(
                "fetcher_meta_channels_top20",
                "Топ-20 каналов по количеству видео",
                labels=["channel"]
            )
            for channel, count in top_channels_list:
                top_channels_metric.add_metric([channel], count)
            yield top_channels_metric
            # Среднее количество видео на канал
            if channel_counter:
                avg_videos_per_channel = sum(channel_counter.values()) / len(channel_counter)
                yield GaugeMetricFamily(
                    "fetcher_meta_avg_videos_per_channel",
                    "Среднее количество видео на канал",
                    avg_videos_per_channel
                )
        
        # 1.13 SubscriberCount метрики
        if self.meta_subscriber_counts:
            yield from emit_stats("fetcher_meta_subscriber_count", "Количество подписчиков канала", self.meta_subscriber_counts, include_median=True)
            # Распределение подписчиков (логарифмическая шкала)
            sub_bins = [0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000]
            yield from emit_distribution("fetcher_meta_subscriber_count", "Количество подписчиков", self.meta_subscriber_counts, sub_bins)
            # Категории размера канала
            channel_size_category_counts = defaultdict(int)
            for sub_count in self.meta_subscriber_counts:
                if sub_count < 1000:
                    channel_size_category_counts["micro"] += 1
                elif sub_count < 10000:
                    channel_size_category_counts["small"] += 1
                elif sub_count < 100000:
                    channel_size_category_counts["medium"] += 1
                elif sub_count < 1000000:
                    channel_size_category_counts["large"] += 1
                else:
                    channel_size_category_counts["mega"] += 1
            channel_size_categories = CounterMetricFamily(
                "fetcher_meta_channel_size_category_total",
                "Распределение каналов по категориям размера",
                labels=["category"]
            )
            for category_label, count in channel_size_category_counts.items():
                channel_size_categories.add_metric([category_label], count)
            yield channel_size_categories
        
        # 1.14 VideoCount метрики
        if self.meta_video_counts:
            yield from emit_stats("fetcher_meta_channel_video_count", "Количество видео канала", self.meta_video_counts, include_median=True)
            # Распределение количества видео
            vid_count_bins = [0, 5, 10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000]
            yield from emit_distribution("fetcher_meta_channel_video_count", "Количество видео", self.meta_video_counts, vid_count_bins)
        
        # 1.15 ViewCount_channel метрики
        if self.meta_view_count_channels:
            yield from emit_stats("fetcher_meta_channel_view_count", "Количество просмотров канала", self.meta_view_count_channels, include_median=True)
            # Распределение просмотров канала (логарифмическая шкала)
            ch_view_bins = [0, 1000, 10000, 50000, 100000, 500000, 1000000, 5000000, 10000000, 50000000, 100000000, 1000000000]
            yield from emit_distribution("fetcher_meta_channel_view_count", "Количество просмотров канала", self.meta_view_count_channels, ch_view_bins)
        
        # 1.16 Country метрики
        if self.meta_countries:
            country_counter = Counter(self.meta_countries)
            # Топ-20 стран
            top_countries = country_counter.most_common(20)
            top_countries_metric = GaugeMetricFamily(
                "fetcher_meta_country_top20",
                "Топ-20 стран по количеству видео",
                labels=["country"]
            )
            for country, count in top_countries:
                top_countries_metric.add_metric([country], count)
            yield top_countries_metric
            yield GaugeMetricFamily(
                "fetcher_meta_unique_countries_total",
                "Количество уникальных стран",
                len(country_counter)
            )
        
        yield GaugeMetricFamily(
            "fetcher_meta_country_missing_total",
            "Количество видео без указания страны",
            self.meta_videos_without_country
        )
        
        # 1.17 Comments метрики (из массива comments)
        if self.meta_comments_counts:
            yield from emit_stats("fetcher_meta_comments_array_count", "Количество комментариев из массива comments", self.meta_comments_counts, include_median=True)
            # Распределение количества комментариев
            comments_array_bins = [0, 1, 5, 10, 20, 50, 100, 200, 500, 1000]
            yield from emit_distribution("fetcher_meta_comments_array_count", "Количество комментариев из массива", self.meta_comments_counts, comments_array_bins)
        
        if self.meta_comment_text_lengths:
            yield from emit_stats("fetcher_meta_comment_text_length", "Длина текста комментария (символов)", self.meta_comment_text_lengths, include_median=True)
            # 1.17.7 Распределение длин текстов комментариев по диапазонам
            comment_text_bins = [0, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000, 10000]
            yield from emit_distribution("fetcher_meta_comment_text_length", "Длина текста комментария", self.meta_comment_text_lengths, comment_text_bins)
        
        # 1.17.8 Количество комментариев с пустым текстом
        yield GaugeMetricFamily(
            "fetcher_meta_comment_empty_text_total",
            "Количество комментариев с пустым текстом",
            self.meta_comment_empty_text_count
        )
        
        if self.meta_comment_like_counts:
            yield from emit_stats("fetcher_meta_comment_like_count", "Количество лайков комментария", self.meta_comment_like_counts, include_median=True)
            # 1.17.11 Распределение лайков на комментарии по диапазонам
            comment_like_bins = [0, 1, 5, 10, 25, 50, 100, 500, 1000, 5000, 10000]
            yield from emit_distribution("fetcher_meta_comment_like_count", "Количество лайков комментария", self.meta_comment_like_counts, comment_like_bins)
        
        if self.meta_comment_reply_counts:
            yield from emit_stats("fetcher_meta_comment_reply_count", "Количество ответов на комментарий", self.meta_comment_reply_counts, include_median=True)
            # 1.17.14 Распределение ответов по диапазонам
            comment_reply_bins = [0, 1, 2, 5, 10, 20, 50, 100, 200, 500]
            yield from emit_distribution("fetcher_meta_comment_reply_count", "Количество ответов на комментарий", self.meta_comment_reply_counts, comment_reply_bins)
        
        # 1.17.16 Распределение комментариев по временным интервалам от публикации видео
        if self.meta_comment_dates and self.meta_comment_video_ids:
            comment_time_intervals = CounterMetricFamily(
                "fetcher_meta_comment_time_interval_total",
                "Распределение комментариев по временному интервалу от публикации видео",
                labels=["interval"]
            )
            comment_interval_counts = defaultdict(int)
            for i, comment_date in enumerate(self.meta_comment_dates):
                video_id = self.meta_comment_video_ids[i] if i < len(self.meta_comment_video_ids) else None
                if video_id and video_id in self.meta_video_published_dates_for_comments:
                    video_pub_date = self.meta_video_published_dates_for_comments[video_id]
                    delta = comment_date - video_pub_date
                    if delta.days < 1:
                        comment_interval_counts["less-1day"] += 1
                    elif delta.days < 7:
                        comment_interval_counts["1day-1week"] += 1
                    elif delta.days < 30:
                        comment_interval_counts["1week-1month"] += 1
                    elif delta.days < 365:
                        comment_interval_counts["1month-1year"] += 1
                    else:
                        comment_interval_counts[">1year"] += 1
            for interval_label, count in comment_interval_counts.items():
                comment_time_intervals.add_metric([interval_label], count)
            if comment_interval_counts:
                yield comment_time_intervals
        
        # Топ-20 авторов комментариев
        if self.meta_comment_authors:
            author_counter = Counter(self.meta_comment_authors)
            top_authors = author_counter.most_common(20)
            top_authors_metric = GaugeMetricFamily(
                "fetcher_meta_comment_author_top20",
                "Топ-20 авторов комментариев по количеству комментариев",
                labels=["author"]
            )
            for author, count in top_authors:
                top_authors_metric.add_metric([author], count)
            yield top_authors_metric
        
        # 1.18 Общие метрики
        # 1.18.1 Общее количество видео - уже реализовано выше
        
        # 1.18.2 Количество видео по категориям
        if self.meta_videos_by_category:
            category_metric = GaugeMetricFamily(
                "fetcher_meta_videos_by_category_total",
                "Количество видео по категориям",
                labels=["category"]
            )
            for category, count in self.meta_videos_by_category.items():
                category_metric.add_metric([category], count)
            yield category_metric
        
        # 1.18.3 Количество видео по временным интервалам - уже реализовано в 1.11.1
        
        # 1.18.4-1.18.6 Корреляции (коэффициенты корреляции)
        if self.meta_view_counts and self.meta_like_counts and len(self.meta_view_counts) == len(self.meta_like_counts):
            try:
                correlation = statistics.correlation(self.meta_view_counts, self.meta_like_counts)
                yield GaugeMetricFamily(
                    "fetcher_meta_correlation_views_likes",
                    "Коэффициент корреляции между просмотрами и лайками",
                    correlation
                )
            except Exception:
                pass
        
        if self.meta_view_counts and self.meta_comment_counts and len(self.meta_view_counts) == len(self.meta_comment_counts):
            try:
                correlation = statistics.correlation(self.meta_view_counts, self.meta_comment_counts)
                yield GaugeMetricFamily(
                    "fetcher_meta_correlation_views_comments",
                    "Коэффициент корреляции между просмотрами и комментариями",
                    correlation
                )
            except Exception:
                pass
        
        if self.meta_view_counts and self.meta_subscriber_counts and len(self.meta_view_counts) == len(self.meta_subscriber_counts):
            try:
                correlation = statistics.correlation(self.meta_view_counts, self.meta_subscriber_counts)
                yield GaugeMetricFamily(
                    "fetcher_meta_correlation_views_subscribers",
                    "Коэффициент корреляции между просмотрами и подписчиками канала",
                    correlation
                )
            except Exception:
                pass
        
        # 1.18.7 Engagement rate
        if self.meta_view_counts and self.meta_like_counts and self.meta_comment_counts:
            engagement_rates = []
            for i in range(min(len(self.meta_view_counts), len(self.meta_like_counts), len(self.meta_comment_counts))):
                if self.meta_view_counts[i] > 0:
                    engagement = ((self.meta_like_counts[i] if i < len(self.meta_like_counts) else 0) + 
                                 (self.meta_comment_counts[i] if i < len(self.meta_comment_counts) else 0)) / self.meta_view_counts[i]
                    engagement_rates.append(engagement)
            if engagement_rates:
                yield from emit_stats("fetcher_meta_engagement_rate", "Уровень вовлеченности (лайки + комментарии) / просмотры", engagement_rates, include_median=True)
        
        # ========== SNAPSHOT_N METRICS ==========
        
        # 2.1 Общие метрики по snapshot
        yield GaugeMetricFamily(
            "fetcher_snapshot_count_total",
            "Количество снапшотов",
            len(self.snapshot_numbers)
        )
        
        for snapshot_num in self.snapshot_numbers:
            snapshot_label = str(snapshot_num)
            
            # Количество timestamp'ов
            if snapshot_num in self.snapshot_timestamps_counts:
                metric = GaugeMetricFamily(
                    "fetcher_snapshot_timestamps_count",
                    "Количество временных меток в снапшоте",
                    labels=["snapshot"]
                )
                metric.add_metric([snapshot_label], self.snapshot_timestamps_counts[snapshot_num])
                yield metric
            
            # 2.1.4 Количество видео по timestamp'ам
            if snapshot_num in self.snapshot_timestamp_videos_counts:
                timestamp_metric = GaugeMetricFamily(
                    "fetcher_snapshot_timestamp_videos_count",
                    "Количество видео по временным меткам в снапшоте",
                    labels=["snapshot", "timestamp"]
                )
                for timestamp, count in self.snapshot_timestamp_videos_counts[snapshot_num].items():
                    timestamp_metric.add_metric([snapshot_label, timestamp], count)
                yield timestamp_metric
            
            # Количество видео
            if snapshot_num in self.snapshot_videos_counts:
                metric = GaugeMetricFamily(
                    "fetcher_snapshot_videos_count",
                    "Количество видео в снапшоте",
                    labels=["snapshot"]
                )
                metric.add_metric([snapshot_label], self.snapshot_videos_counts[snapshot_num])
                yield metric
            
            # Временной интервал
            if snapshot_num in self.snapshot_time_intervals:
                metric = GaugeMetricFamily(
                    "fetcher_snapshot_time_interval_hours",
                    "Временной интервал от meta_snapshot до снапшота (часы)",
                    labels=["snapshot"]
                )
                metric.add_metric([snapshot_label], self.snapshot_time_intervals[snapshot_num])
                yield metric
            
            # 2.2 Дельты viewCount
            if snapshot_num in self.snapshot_deltas_view_count and self.snapshot_deltas_view_count[snapshot_num]:
                deltas = self.snapshot_deltas_view_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_delta", "Дельта количества просмотров", deltas, include_median=True)
                # 2.2.3 Распределение дельт просмотров по диапазонам
                view_delta_bins = [-100000, -10000, -1000, -100, 0, 100, 1000, 10000, 100000, 1000000]
                logger.debug(f"snapshot_{snapshot_num}: Generating view_count_delta distribution ({len(deltas)} values)")
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_view_count_delta", "Дельта количества просмотров", deltas, view_delta_bins)
                # Количество видео с положительной/отрицательной дельтой
                positive_count = sum(1 for d in deltas if d > 0)
                negative_count = sum(1 for d in deltas if d < 0)
                zero_count = sum(1 for d in deltas if d == 0)
                delta_direction = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_view_count_delta_direction_total",
                    "Направление дельты количества просмотров",
                    labels=["direction", "snapshot"]
                )
                delta_direction.add_metric(["positive", snapshot_label], positive_count)
                delta_direction.add_metric(["negative", snapshot_label], negative_count)
                delta_direction.add_metric(["zero", snapshot_label], zero_count)
                yield delta_direction
                
                # Проценты изменения
                if snapshot_num in self.snapshot_percent_changes_view_count:
                    percents = self.snapshot_percent_changes_view_count[snapshot_num]
                    if percents:
                        logger.debug(f"snapshot_{snapshot_num}: Generating view_count percent_change metrics ({len(percents)} values)")
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_percent_change", "Процент изменения количества просмотров", percents, include_median=True)
                        # 2.2.10 Распределение процентов изменения просмотров по диапазонам
                        percent_bins = [-100, -50, -10, -1, 0, 1, 10, 50, 100, 500, 1000]
                        yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_view_count_percent_change", "Процент изменения количества просмотров", percents, percent_bins)
                    else:
                        logger.warning(f"snapshot_{snapshot_num}: percent_changes_view_count is empty")
                else:
                    logger.warning(f"snapshot_{snapshot_num}: snapshot_percent_changes_view_count not found")
                
                # Скорость роста
                if snapshot_num in self.snapshot_growth_rates_view_count:
                    rates = self.snapshot_growth_rates_view_count[snapshot_num]
                    if rates:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_growth_rate", "Скорость роста количества просмотров (в час)", rates, include_median=True)
                
                # 2.2.11-12 Топ-20 видео с наибольшим ростом/падением просмотров
                if snapshot_num in self.snapshot_top_view_deltas:
                    top_list = sorted(self.snapshot_top_view_deltas[snapshot_num], key=lambda x: x[1], reverse=True)
                    top_growth = top_list[:20]
                    top_decline = sorted(top_list, key=lambda x: x[1])[:20]
                    
                    logger.debug(f"snapshot_{snapshot_num}: Generating top20 view deltas - growth: {len(top_growth)}, decline: {len(top_decline)}")
                    
                    top_growth_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_view_count_top20_growth",
                        "Топ-20 видео с наибольшим ростом просмотров",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_growth:
                        top_growth_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_growth_metric
                    
                    top_decline_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_view_count_top20_decline",
                        "Топ-20 видео с наибольшим падением просмотров",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_decline:
                        top_decline_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_decline_metric
                else:
                    logger.warning(f"snapshot_{snapshot_num}: snapshot_top_view_deltas not found")
            
            # 2.3 Дельты likeCount
            if snapshot_num in self.snapshot_deltas_like_count and self.snapshot_deltas_like_count[snapshot_num]:
                deltas = self.snapshot_deltas_like_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_like_count_delta", "Дельта количества лайков", deltas, include_median=True)
                # Распределение дельт
                like_delta_bins = [-10000, -1000, -500, -100, -10, 0, 10, 100, 500, 1000, 5000, 10000]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_like_count_delta", "Дельта количества лайков", deltas, like_delta_bins)
                # Направление дельты
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                zero = sum(1 for d in deltas if d == 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_like_count_delta_direction_total",
                    "Направление дельты количества лайков",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["positive", snapshot_label], positive)
                delta_dir.add_metric(["negative", snapshot_label], negative)
                delta_dir.add_metric(["zero", snapshot_label], zero)
                yield delta_dir
                # Проценты и скорость роста
                if snapshot_num in self.snapshot_percent_changes_like_count:
                    percents = self.snapshot_percent_changes_like_count[snapshot_num]
                    if percents:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_like_count_percent_change", "Процент изменения количества лайков", percents, include_median=True)
                        # 2.3.10 Распределение процентов изменения лайков по диапазонам
                        percent_bins = [-100, -50, -10, -1, 0, 1, 10, 50, 100, 500, 1000]
                        yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_like_count_percent_change", "Процент изменения количества лайков", percents, percent_bins)
                if snapshot_num in self.snapshot_growth_rates_like_count:
                    rates = self.snapshot_growth_rates_like_count[snapshot_num]
                    if rates:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_like_count_growth_rate", "Скорость роста количества лайков (в час)", rates, include_median=True)
                
                # 2.3.11 Топ-20 видео с наибольшим ростом лайков
                if snapshot_num in self.snapshot_top_like_deltas:
                    top_list = sorted(self.snapshot_top_like_deltas[snapshot_num], key=lambda x: x[1], reverse=True)[:20]
                    top_like_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_like_count_top20_growth",
                        "Топ-20 видео с наибольшим ростом лайков",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_list:
                        top_like_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_like_metric
            
            # 2.4 Дельты commentCount
            if snapshot_num in self.snapshot_deltas_comment_count and self.snapshot_deltas_comment_count[snapshot_num]:
                deltas = self.snapshot_deltas_comment_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_count_delta", "Дельта количества комментариев", deltas, include_median=True)
                # Распределение и направление
                comment_delta_bins = [-1000, -100, -50, -10, -1, 0, 1, 10, 50, 100, 500, 1000]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_comment_count_delta", "Дельта количества комментариев", deltas, comment_delta_bins)
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                zero = sum(1 for d in deltas if d == 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_comment_count_delta_direction_total",
                    "Направление дельты количества комментариев",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["positive", snapshot_label], positive)
                delta_dir.add_metric(["negative", snapshot_label], negative)
                delta_dir.add_metric(["zero", snapshot_label], zero)
                yield delta_dir
                # Проценты и скорость роста
                if snapshot_num in self.snapshot_percent_changes_comment_count:
                    percents = self.snapshot_percent_changes_comment_count[snapshot_num]
                    if percents:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_count_percent_change", "Процент изменения количества комментариев", percents, include_median=True)
                        # 2.4.10 Распределение процентов изменения комментариев по диапазонам
                        percent_bins = [-100, -50, -10, -1, 0, 1, 10, 50, 100, 500, 1000]
                        yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_comment_count_percent_change", "Процент изменения количества комментариев", percents, percent_bins)
                if snapshot_num in self.snapshot_growth_rates_comment_count:
                    rates = self.snapshot_growth_rates_comment_count[snapshot_num]
                    if rates:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_count_growth_rate", "Скорость роста количества комментариев (в час)", rates, include_median=True)
                
                # 2.4.11 Топ-20 видео с наибольшим ростом комментариев
                if snapshot_num in self.snapshot_top_comment_deltas:
                    top_list = sorted(self.snapshot_top_comment_deltas[snapshot_num], key=lambda x: x[1], reverse=True)[:20]
                    top_comment_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_comment_count_top20_growth",
                        "Топ-20 видео с наибольшим ростом комментариев",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_list:
                        top_comment_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_comment_metric
            
            # 2.5 Дельты subscriberCount
            if snapshot_num in self.snapshot_deltas_subscriber_count and self.snapshot_deltas_subscriber_count[snapshot_num]:
                deltas = self.snapshot_deltas_subscriber_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_subscriber_count_delta", "Дельта количества подписчиков", deltas, include_median=True)
                sub_delta_bins = [-100000, -10000, -5000, -1000, -100, 0, 100, 1000, 5000, 10000, 50000, 100000]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_subscriber_count_delta", "Дельта количества подписчиков", deltas, sub_delta_bins)
                # 2.5.5-7 Направления дельт
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                zero = sum(1 for d in deltas if d == 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_subscriber_count_delta_direction_total",
                    "Направление дельты количества подписчиков",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["positive", snapshot_label], positive)
                delta_dir.add_metric(["negative", snapshot_label], negative)
                delta_dir.add_metric(["zero", snapshot_label], zero)
                yield delta_dir
                # 2.5.8-12 Проценты, распределения, топ-20, скорость роста
                if snapshot_num in self.snapshot_percent_changes_subscriber_count:
                    percents = self.snapshot_percent_changes_subscriber_count[snapshot_num]
                    if percents:
                        logger.debug(f"snapshot_{snapshot_num}: Generating subscriber_count percent_change metrics ({len(percents)} values)")
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_subscriber_count_percent_change", "Процент изменения количества подписчиков", percents, include_median=True)
                        percent_bins = [-100, -50, -20, -10, -5, -1, 0, 1, 5, 10, 20, 50, 100, 500]
                        yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_subscriber_count_percent_change", "Процент изменения количества подписчиков", percents, percent_bins)
                    else:
                        logger.warning(f"snapshot_{snapshot_num}: percent_changes_subscriber_count is empty")
                else:
                    logger.warning(f"snapshot_{snapshot_num}: snapshot_percent_changes_subscriber_count not found")
                if snapshot_num in self.snapshot_growth_rates_subscriber_count:
                    rates = self.snapshot_growth_rates_subscriber_count[snapshot_num]
                    if rates:
                        logger.debug(f"snapshot_{snapshot_num}: Generating subscriber_count growth_rate metrics ({len(rates)} values)")
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_subscriber_count_growth_rate", "Скорость роста количества подписчиков (в час)", rates, include_median=True)
                    else:
                        logger.warning(f"snapshot_{snapshot_num}: growth_rates_subscriber_count is empty")
                else:
                    logger.warning(f"snapshot_{snapshot_num}: snapshot_growth_rates_subscriber_count not found")
                # 2.5.11 Топ-20 каналов
                if snapshot_num in self.snapshot_top_subscriber_deltas:
                    top_list = sorted(self.snapshot_top_subscriber_deltas[snapshot_num], key=lambda x: x[1], reverse=True)[:20]
                    top_sub_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_subscriber_count_top20_growth",
                        "Топ-20 каналов с наибольшим ростом подписчиков",
                        labels=["channel", "snapshot"]
                    )
                    for channel, delta in top_list:
                        top_sub_metric.add_metric([channel, snapshot_label], delta)
                    yield top_sub_metric
            
            # 2.6 Дельты videoCount
            if snapshot_num in self.snapshot_deltas_video_count and self.snapshot_deltas_video_count[snapshot_num]:
                deltas = self.snapshot_deltas_video_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_video_count_delta", "Дельта количества видео", deltas, include_median=True)
                vid_delta_bins = [-1000, -100, -50, -10, -1, 0, 1, 10, 50, 100, 500, 1000]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_video_count_delta", "Дельта количества видео", deltas, vid_delta_bins)
                # 2.6.5-7 Направления дельт
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                zero = sum(1 for d in deltas if d == 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_video_count_delta_direction_total",
                    "Направление дельты количества видео",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["positive", snapshot_label], positive)
                delta_dir.add_metric(["negative", snapshot_label], negative)
                delta_dir.add_metric(["zero", snapshot_label], zero)
                yield delta_dir
                # 2.6.8-9 Проценты и скорость роста
                if snapshot_num in self.snapshot_percent_changes_video_count:
                    percents = self.snapshot_percent_changes_video_count[snapshot_num]
                    if percents:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_video_count_percent_change", "Процент изменения количества видео", percents, include_median=True)
                if snapshot_num in self.snapshot_growth_rates_video_count:
                    rates = self.snapshot_growth_rates_video_count[snapshot_num]
                    if rates:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_video_count_growth_rate", "Скорость роста количества видео (в час)", rates, include_median=True)
            
            # 2.7 Дельты viewCount_channel
            if snapshot_num in self.snapshot_deltas_view_count_channel and self.snapshot_deltas_view_count_channel[snapshot_num]:
                deltas = self.snapshot_deltas_view_count_channel[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_channel_delta", "Дельта количества просмотров канала", deltas, include_median=True)
                ch_view_delta_bins = [-10000000, -1000000, -500000, -100000, -10000, 0, 10000, 100000, 500000, 1000000, 5000000, 10000000]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_view_count_channel_delta", "Дельта количества просмотров канала", deltas, ch_view_delta_bins)
                # 2.7.5-6 Направления дельт
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_view_count_channel_delta_direction_total",
                    "Направление дельты количества просмотров канала",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["positive", snapshot_label], positive)
                delta_dir.add_metric(["negative", snapshot_label], negative)
                yield delta_dir
                # 2.7.7-9 Проценты и скорость роста
                if snapshot_num in self.snapshot_percent_changes_view_count_channel:
                    percents = self.snapshot_percent_changes_view_count_channel[snapshot_num]
                    if percents:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_channel_percent_change", "Процент изменения количества просмотров канала", percents, include_median=True)
                if snapshot_num in self.snapshot_growth_rates_view_count_channel:
                    rates = self.snapshot_growth_rates_view_count_channel[snapshot_num]
                    if rates:
                        yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_view_count_channel_growth_rate", "Скорость роста количества просмотров канала (в час)", rates, include_median=True)
            
            # 2.8 Дельты comments (из массива)
            if snapshot_num in self.snapshot_deltas_comments_count and self.snapshot_deltas_comments_count[snapshot_num]:
                deltas = self.snapshot_deltas_comments_count[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comments_array_delta", "Дельта количества комментариев из массива", deltas, include_median=True)
                comments_delta_bins = [-100, -50, -20, -10, -1, 0, 1, 10, 20, 50, 100, 500]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_comments_array_delta", "Дельта количества комментариев из массива", deltas, comments_delta_bins)
                positive = sum(1 for d in deltas if d > 0)
                zero = sum(1 for d in deltas if d == 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_comments_array_delta_direction_total",
                    "Направление дельты количества комментариев из массива",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["new_comments", snapshot_label], positive)
                delta_dir.add_metric(["no_new", snapshot_label], zero)
                yield delta_dir
                
                # 2.8.6-8 Дельты текста, лайков и ответов комментариев
                if snapshot_num in self.snapshot_deltas_comment_text_length and self.snapshot_deltas_comment_text_length[snapshot_num]:
                    yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_text_length_delta", "Дельта длины текста комментария", self.snapshot_deltas_comment_text_length[snapshot_num], include_median=True)
                if snapshot_num in self.snapshot_deltas_comment_like_count and self.snapshot_deltas_comment_like_count[snapshot_num]:
                    yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_like_count_delta", "Дельта количества лайков комментария", self.snapshot_deltas_comment_like_count[snapshot_num], include_median=True)
                if snapshot_num in self.snapshot_deltas_comment_reply_count and self.snapshot_deltas_comment_reply_count[snapshot_num]:
                    yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_comment_reply_count_delta", "Дельта количества ответов на комментарий", self.snapshot_deltas_comment_reply_count[snapshot_num], include_median=True)
                
                # 2.8.9 Количество новых уникальных авторов комментариев
                if snapshot_num in self.snapshot_new_comment_authors:
                    authors_count = len(self.snapshot_new_comment_authors[snapshot_num])
                    logger.debug(f"snapshot_{snapshot_num}: Generating new_comment_authors metric ({authors_count} authors)")
                    yield GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_new_comment_authors_total",
                        "Количество новых уникальных авторов комментариев",
                        authors_count
                    )
                else:
                    logger.warning(f"snapshot_{snapshot_num}: snapshot_new_comment_authors not found")
                
                # 2.8.10 Топ-20 видео с наибольшим количеством новых комментариев
                if snapshot_num in self.snapshot_top_new_comments:
                    top_list = sorted(self.snapshot_top_new_comments[snapshot_num], key=lambda x: x[1], reverse=True)[:20]
                    top_new_comments_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_new_comments_top20",
                        "Топ-20 видео с наибольшим количеством новых комментариев",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_list:
                        top_new_comments_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_new_comments_metric
            
            # 2.10 Engagement rate дельты
            if snapshot_num in self.snapshot_deltas_engagement_rate and self.snapshot_deltas_engagement_rate[snapshot_num]:
                deltas = self.snapshot_deltas_engagement_rate[snapshot_num]
                yield from emit_stats(f"fetcher_snapshot_{snapshot_num}_engagement_rate_delta", "Дельта уровня вовлеченности", deltas, include_median=True)
                engagement_delta_bins = [-0.1, -0.01, -0.001, 0, 0.001, 0.01, 0.1, 1.0]
                yield from emit_distribution(f"fetcher_snapshot_{snapshot_num}_engagement_rate_delta", "Дельта уровня вовлеченности", deltas, engagement_delta_bins)
                positive = sum(1 for d in deltas if d > 0)
                negative = sum(1 for d in deltas if d < 0)
                delta_dir = CounterMetricFamily(
                    f"fetcher_snapshot_{snapshot_num}_engagement_rate_delta_direction_total",
                    "Направление дельты уровня вовлеченности",
                    labels=["direction", "snapshot"]
                )
                delta_dir.add_metric(["increase", snapshot_label], positive)
                delta_dir.add_metric(["decrease", snapshot_label], negative)
                yield delta_dir
                
                # 2.10.6 Топ-20 видео с наибольшим ростом engagement rate
                if snapshot_num in self.snapshot_top_engagement_deltas:
                    top_list = sorted(self.snapshot_top_engagement_deltas[snapshot_num], key=lambda x: x[1], reverse=True)[:20]
                    top_engagement_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_engagement_rate_top20_growth",
                        "Топ-20 видео с наибольшим ростом уровня вовлеченности",
                        labels=["video_id", "snapshot"]
                    )
                    for video_id, delta in top_list:
                        top_engagement_metric.add_metric([video_id, snapshot_label], delta)
                    yield top_engagement_metric
            
            # 2.9 Корреляции дельт
            if snapshot_num in self.snapshot_deltas_view_count and snapshot_num in self.snapshot_deltas_like_count:
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                like_deltas = self.snapshot_deltas_like_count[snapshot_num]
                if len(view_deltas) == len(like_deltas) and len(view_deltas) > 1:
                    try:
                        correlation = statistics.correlation(view_deltas, like_deltas)
                        corr_metric = GaugeMetricFamily(
                            f"fetcher_snapshot_{snapshot_num}_correlation_view_like_delta",
                            "Корреляция между дельтой просмотров и дельтой лайков",
                            labels=["snapshot"]
                        )
                        corr_metric.add_metric([snapshot_label], correlation)
                        yield corr_metric
                    except Exception:
                        pass
            
            if snapshot_num in self.snapshot_deltas_view_count and snapshot_num in self.snapshot_deltas_comment_count:
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                comment_deltas = self.snapshot_deltas_comment_count[snapshot_num]
                if len(view_deltas) == len(comment_deltas) and len(view_deltas) > 1:
                    try:
                        correlation = statistics.correlation(view_deltas, comment_deltas)
                        corr_metric = GaugeMetricFamily(
                            f"fetcher_snapshot_{snapshot_num}_correlation_view_comment_delta",
                            "Корреляция между дельтой просмотров и дельтой комментариев",
                            labels=["snapshot"]
                        )
                        corr_metric.add_metric([snapshot_label], correlation)
                        yield corr_metric
                    except Exception:
                        pass
            
            if snapshot_num in self.snapshot_deltas_subscriber_count and snapshot_num in self.snapshot_deltas_view_count:
                sub_deltas = self.snapshot_deltas_subscriber_count[snapshot_num]
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                if len(sub_deltas) == len(view_deltas) and len(sub_deltas) > 1:
                    try:
                        correlation = statistics.correlation(sub_deltas, view_deltas)
                        corr_metric = GaugeMetricFamily(
                            f"fetcher_snapshot_{snapshot_num}_correlation_subscriber_view_delta",
                            "Корреляция между дельтой подписчиков и дельтой просмотров",
                            labels=["snapshot"]
                        )
                        corr_metric.add_metric([snapshot_label], correlation)
                        yield corr_metric
                    except Exception:
                        pass
            
            if snapshot_num in self.snapshot_deltas_view_count and snapshot_num in self.snapshot_time_intervals:
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                time_interval = self.snapshot_time_intervals[snapshot_num]
                if len(view_deltas) > 1 and time_interval > 0:
                    # 2.9.4 Корреляция между дельтой просмотров и временем между снапшотами
                    avg_delta = sum(view_deltas) / len(view_deltas) if view_deltas else 0
                    avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_avg_view_delta_per_hour",
                        "Средняя дельта просмотров в час",
                        labels=["snapshot"]
                    )
                    avg_metric.add_metric([snapshot_label], avg_delta / time_interval if time_interval > 0 else 0)
                    yield avg_metric
            
            # 2.12 Временные метрики
            # 2.12.1-4 Распределение дельт по временным интервалам публикации
            if snapshot_num in self.snapshot_video_published_intervals and snapshot_num in self.snapshot_deltas_view_count:
                intervals = self.snapshot_video_published_intervals[snapshot_num]
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                # Группируем дельты по интервалам
                interval_view_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(view_deltas)]):
                    if video_id in intervals and i < len(view_deltas):
                        interval = intervals[video_id]
                        interval_view_deltas[interval].append(view_deltas[i])
                
                # 2.12.1 Распределение дельт по временным интервалам
                if interval_view_deltas:
                    interval_dist = CounterMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_view_delta_by_publish_interval_total",
                        "Распределение дельт просмотров по временному интервалу публикации видео",
                        labels=["interval", "snapshot"]
                    )
                    for interval, deltas in interval_view_deltas.items():
                        interval_dist.add_metric([interval, snapshot_label], len(deltas))
                    yield interval_dist
                    
                    # 2.12.2 Средняя дельта просмотров по временным интервалам
                    interval_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_avg_view_delta_by_publish_interval",
                        "Средняя дельта просмотров по временному интервалу публикации видео",
                        labels=["interval", "snapshot"]
                    )
                    for interval, deltas in interval_view_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            interval_avg_metric.add_metric([interval, snapshot_label], avg_delta)
                    yield interval_avg_metric
            
            # 2.12.3 Средняя дельта лайков по временным интервалам
            if snapshot_num in self.snapshot_video_published_intervals and snapshot_num in self.snapshot_deltas_like_count:
                intervals = self.snapshot_video_published_intervals[snapshot_num]
                like_deltas = self.snapshot_deltas_like_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                interval_like_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(like_deltas)]):
                    if video_id in intervals and i < len(like_deltas):
                        interval = intervals[video_id]
                        interval_like_deltas[interval].append(like_deltas[i])
                
                if interval_like_deltas:
                    interval_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_avg_like_delta_by_publish_interval",
                        "Средняя дельта лайков по временному интервалу публикации видео",
                        labels=["interval", "snapshot"]
                    )
                    for interval, deltas in interval_like_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            interval_avg_metric.add_metric([interval, snapshot_label], avg_delta)
                    yield interval_avg_metric
            
            # 2.12.4 Средняя дельта комментариев по временным интервалам
            if snapshot_num in self.snapshot_video_published_intervals and snapshot_num in self.snapshot_deltas_comment_count:
                intervals = self.snapshot_video_published_intervals[snapshot_num]
                comment_deltas = self.snapshot_deltas_comment_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                interval_comment_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(comment_deltas)]):
                    if video_id in intervals and i < len(comment_deltas):
                        interval = intervals[video_id]
                        interval_comment_deltas[interval].append(comment_deltas[i])
                
                if interval_comment_deltas:
                    interval_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_avg_comment_delta_by_publish_interval",
                        "Средняя дельта комментариев по временному интервалу публикации видео",
                        labels=["interval", "snapshot"]
                    )
                    for interval, deltas in interval_comment_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            interval_avg_metric.add_metric([interval, snapshot_label], avg_delta)
                    yield interval_avg_metric
            
            # 2.12.5 Корреляция между возрастом видео и дельтой просмотров
            if snapshot_num in self.snapshot_video_ages and snapshot_num in self.snapshot_deltas_view_count:
                ages = self.snapshot_video_ages[snapshot_num]
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                if len(ages) == len(view_deltas) and len(ages) > 1:
                    try:
                        correlation = statistics.correlation(ages, view_deltas)
                        corr_metric = GaugeMetricFamily(
                            f"fetcher_snapshot_{snapshot_num}_correlation_age_view_delta",
                            "Корреляция между возрастом видео и дельтой просмотров",
                            labels=["snapshot"]
                        )
                        corr_metric.add_metric([snapshot_label], correlation)
                        yield corr_metric
                    except Exception:
                        pass
            
            # 2.13 Категории каналов по дельтам
            # Используем сохраненный порядок video_id для правильного сопоставления
            if snapshot_num in self.snapshot_channel_categories and snapshot_num in self.snapshot_deltas_view_count:
                categories = self.snapshot_channel_categories[snapshot_num]
                view_deltas = self.snapshot_deltas_view_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                # Группируем дельты по категориям
                category_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(view_deltas)]):
                    if video_id in categories and i < len(view_deltas):
                        category = categories[video_id]
                        category_deltas[category].append(view_deltas[i])
                
                # 2.13.1 Средняя дельта просмотров по категориям
                if category_deltas:
                    category_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_view_delta_by_category",
                        "Средняя дельта просмотров по категории канала",
                        labels=["category", "snapshot"]
                    )
                    for category, deltas in category_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            category_avg_metric.add_metric([category, snapshot_label], avg_delta)
                    yield category_avg_metric
            
            # 2.13.2 Средняя дельта лайков по категориям
            if snapshot_num in self.snapshot_channel_categories and snapshot_num in self.snapshot_deltas_like_count:
                categories = self.snapshot_channel_categories[snapshot_num]
                like_deltas = self.snapshot_deltas_like_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                category_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(like_deltas)]):
                    if video_id in categories and i < len(like_deltas):
                        category = categories[video_id]
                        category_deltas[category].append(like_deltas[i])
                
                if category_deltas:
                    category_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_like_delta_by_category",
                        "Средняя дельта лайков по категории канала",
                        labels=["category", "snapshot"]
                    )
                    for category, deltas in category_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            category_avg_metric.add_metric([category, snapshot_label], avg_delta)
                    yield category_avg_metric
            
            # 2.13.3 Средняя дельта комментариев по категориям
            if snapshot_num in self.snapshot_channel_categories and snapshot_num in self.snapshot_deltas_comment_count:
                categories = self.snapshot_channel_categories[snapshot_num]
                comment_deltas = self.snapshot_deltas_comment_count[snapshot_num]
                video_ids = self.snapshot_video_ids_with_deltas[snapshot_num]
                
                category_deltas: Dict[str, List[float]] = defaultdict(list)
                for i, video_id in enumerate(video_ids[:len(comment_deltas)]):
                    if video_id in categories and i < len(comment_deltas):
                        category = categories[video_id]
                        category_deltas[category].append(comment_deltas[i])
                
                if category_deltas:
                    category_avg_metric = GaugeMetricFamily(
                        f"fetcher_snapshot_{snapshot_num}_comment_delta_by_category",
                        "Средняя дельта комментариев по категории канала",
                        labels=["category", "snapshot"]
                    )
                    for category, deltas in category_deltas.items():
                        if deltas:
                            avg_delta = sum(deltas) / len(deltas)
                            category_avg_metric.add_metric([category, snapshot_label], avg_delta)
                    yield category_avg_metric
        
        # ========== YT_DLP METRICS ==========
        
        # Helper to emit basic stats (min/max/mean/count) as gauges
        def emit_ytdlp_stats(metric_base: str, desc: str, values: List[float]):
            if not values:
                return
            vmin = min(values)
            vmax = max(values)
            vmean = sum(values) / len(values)
            stats = GaugeMetricFamily(
                f"{metric_base}",
                f"{desc} (min/max/mean)",
                labels=["stat"]
            )
            stats.add_metric(["min"], vmin)
            stats.add_metric(["max"], vmax)
            stats.add_metric(["mean"], vmean)
            yield stats
            yield GaugeMetricFamily(f"{metric_base}_count", f"Count of {desc}", len(values))
        
        # Video counts
        yield GaugeMetricFamily(
            "ytdlp_videos_total",
            "Total number of processed video entries",
            self.ytdlp_videos_total_count
        )
        
        # Age limit stats
        if self.ytdlp_age_limit:
            yield from emit_ytdlp_stats("ytdlp_video_age_limit", "Video age_limit values", [float(v) for v in self.ytdlp_age_limit])
        
        # Subtitles metrics
        subtitles_total = CounterMetricFamily(
            "ytdlp_subtitles_total",
            "Total number of subtitle entries",
            labels=["language"]
        )
        subtitles_total.add_metric(["ru"], self.ytdlp_subtitles_ru_count)
        subtitles_total.add_metric(["en"], self.ytdlp_subtitles_en_count)
        yield subtitles_total

        subtitles_empty_total = CounterMetricFamily(
            "ytdlp_subtitles_empty_total",
            "Number of empty subtitle entries",
            labels=["language"]
        )
        subtitles_empty_total.add_metric(["ru"], self.ytdlp_empty_subtitles_ru_count)
        subtitles_empty_total.add_metric(["en"], self.ytdlp_empty_subtitles_en_count)
        yield subtitles_empty_total
        
        # Subtitles length stats
        if self.ytdlp_subtitles_ru_len or self.ytdlp_subtitles_en_len:
            subtitles_stats = GaugeMetricFamily(
                "ytdlp_subtitles_length_characters",
                "Length of subtitle text in characters (min/max/mean)",
                labels=["language", "stat"]
            )
            subtitles_count = GaugeMetricFamily(
                "ytdlp_subtitles_length_characters_count",
                "Count of subtitles entries with text",
                labels=["language"]
            )
            if self.ytdlp_subtitles_ru_len:
                v = self.ytdlp_subtitles_ru_len
                subtitles_stats.add_metric(["ru", "min"], min(v))
                subtitles_stats.add_metric(["ru", "max"], max(v))
                subtitles_stats.add_metric(["ru", "mean"], sum(v)/len(v))
                subtitles_count.add_metric(["ru"], len(v))
            if self.ytdlp_subtitles_en_len:
                v = self.ytdlp_subtitles_en_len
                subtitles_stats.add_metric(["en", "min"], min(v))
                subtitles_stats.add_metric(["en", "max"], max(v))
                subtitles_stats.add_metric(["en", "mean"], sum(v)/len(v))
                subtitles_count.add_metric(["en"], len(v))
            yield subtitles_stats
            yield subtitles_count
        
        # Automatic captions metrics
        auto_caps_total = CounterMetricFamily(
            "ytdlp_automatic_captions_total",
            "Total number of automatic caption entries",
            labels=["language"]
        )
        auto_caps_total.add_metric(["ru"], self.ytdlp_automatic_captions_ru_count)
        auto_caps_total.add_metric(["en"], self.ytdlp_automatic_captions_en_count)
        yield auto_caps_total

        auto_caps_empty_total = CounterMetricFamily(
            "ytdlp_automatic_captions_empty_total",
            "Number of empty automatic caption entries",
            labels=["language"]
        )
        auto_caps_empty_total.add_metric(["ru"], self.ytdlp_empty_automatic_captions_ru_count)
        auto_caps_empty_total.add_metric(["en"], self.ytdlp_empty_automatic_captions_en_count)
        yield auto_caps_empty_total
        
        # Automatic captions length stats
        if self.ytdlp_automatic_captions_ru_len or self.ytdlp_automatic_captions_en_len:
            auto_stats = GaugeMetricFamily(
                "ytdlp_automatic_captions_length_characters",
                "Length of automatic caption text in characters (min/max/mean)",
                labels=["language", "stat"]
            )
            auto_count = GaugeMetricFamily(
                "ytdlp_automatic_captions_length_characters_count",
                "Count of automatic captions entries with text",
                labels=["language"]
            )
            if self.ytdlp_automatic_captions_ru_len:
                v = self.ytdlp_automatic_captions_ru_len
                auto_stats.add_metric(["ru", "min"], min(v))
                auto_stats.add_metric(["ru", "max"], max(v))
                auto_stats.add_metric(["ru", "mean"], sum(v)/len(v))
                auto_count.add_metric(["ru"], len(v))
            if self.ytdlp_automatic_captions_en_len:
                v = self.ytdlp_automatic_captions_en_len
                auto_stats.add_metric(["en", "min"], min(v))
                auto_stats.add_metric(["en", "max"], max(v))
                auto_stats.add_metric(["en", "mean"], sum(v)/len(v))
                auto_count.add_metric(["en"], len(v))
            yield auto_stats
            yield auto_count
        
        # Chapters metrics
        chapters_total = CounterMetricFamily(
            "ytdlp_chapters_total",
            "Total number of chapters across all videos"
        )
        chapters_total.add_metric([], self.ytdlp_chapters_count)
        yield chapters_total

        videos_with_chapters = CounterMetricFamily(
            "ytdlp_videos_with_chapters_total",
            "Number of videos with chapters"
        )
        videos_with_chapters.add_metric([], self.ytdlp_videos_with_chapters)
        yield videos_with_chapters

        videos_without_chapters = CounterMetricFamily(
            "ytdlp_videos_without_chapters_total",
            "Number of videos without chapters"
        )
        videos_without_chapters.add_metric([], self.ytdlp_videos_without_chapters)
        yield videos_without_chapters
        
        # Formats metrics
        formats_total = CounterMetricFamily(
            "ytdlp_formats_total",
            "Total number of format entries across all videos"
        )
        formats_total.add_metric([], self.ytdlp_formats_count)
        yield formats_total

        videos_with_formats = CounterMetricFamily(
            "ytdlp_videos_with_formats_total",
            "Number of videos with formats"
        )
        videos_with_formats.add_metric([], self.ytdlp_videos_with_formats)
        yield videos_with_formats

        videos_without_formats = CounterMetricFamily(
            "ytdlp_videos_without_formats_total",
            "Number of videos without formats"
        )
        videos_without_formats.add_metric([], self.ytdlp_videos_without_formats)
        yield videos_without_formats
        
        # Resolution distribution
        if self.ytdlp_resolution_counts:
            resolution_gauge = GaugeMetricFamily(
                "ytdlp_resolution_count",
                "Number of formats with specific resolution",
                labels=["resolution"]
            )
            for resolution, count in self.ytdlp_resolution_counts.items():
                resolution_gauge.add_metric([resolution], count)
            yield resolution_gauge
        
        # Thumbnails metrics
        thumbnails_total = CounterMetricFamily(
            "ytdlp_thumbnails_total",
            "Total number of thumbnail entries"
        )
        thumbnails_total.add_metric([], self.ytdlp_thumbnails_count)
        yield thumbnails_total

        videos_with_thumbnails = CounterMetricFamily(
            "ytdlp_videos_with_thumbnails_total",
            "Number of videos with thumbnails"
        )
        videos_with_thumbnails.add_metric([], self.ytdlp_videos_with_thumbnails)
        yield videos_with_thumbnails

        videos_without_thumbnails = CounterMetricFamily(
            "ytdlp_videos_without_thumbnails_total",
            "Number of videos without thumbnails"
        )
        videos_without_thumbnails.add_metric([], self.ytdlp_videos_without_thumbnails)
        yield videos_without_thumbnails
        
        # Video duration stats
        if self.ytdlp_duration_seconds:
            yield from emit_ytdlp_stats("ytdlp_video_duration_seconds", "Video duration (seconds)", self.ytdlp_duration_seconds)
        
        # Timing stats
        if self.ytdlp_extract_info_seconds:
            yield from emit_ytdlp_stats("ytdlp_extract_info_seconds", "Time spent extracting video info (seconds)", self.ytdlp_extract_info_seconds)
        if self.ytdlp_captions_seconds_total:
            yield from emit_ytdlp_stats("ytdlp_captions_seconds_total", "Total time spent fetching captions (seconds)", self.ytdlp_captions_seconds_total)
        if self.ytdlp_total_seconds:
            yield from emit_ytdlp_stats("ytdlp_total_processing_seconds", "Total processing time per video (seconds)", self.ytdlp_total_seconds)


def get_metrics_registry(results_dir: Optional[str] = None, token = None) -> CollectorRegistry:
    """Create a Prometheus registry with fetcher metrics."""
    registry = CollectorRegistry()
    collector = FetcherMetricsCollector(results_dir, token)
    registry.register(collector)
    return registry


def generate_metrics_text(results_dir: Optional[str] = None) -> str:
    """Generate Prometheus metrics text format."""
    registry = get_metrics_registry(results_dir)
    return generate_latest(registry).decode('utf-8')


if __name__ == "__main__":
    # Can be used as standalone HTTP server
    import argparse

    default_dir = _resolve_fetcher_results_dir()
    parser = argparse.ArgumentParser(description="Prometheus metrics exporter for fetcher results")
    parser.add_argument("--port", type=int, default=8002, help="Port to listen on (default: 8002)")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host to bind to (default: 0.0.0.0)")
    parser.add_argument("--results-dir", type=str, default=default_dir,
                       help=f"Directory containing .results/fetcher files (default: {default_dir})")
    parser.add_argument("--token", type=str, help="HuggingFace token for uploading metrics")
    args = parser.parse_args()

    registry = get_metrics_registry(args.results_dir, args.token)

    print(f"Starting Prometheus metrics server for yt_api results...")
    print(f"  Host: {args.host}")
    print(f"  Port: {args.port}")
    print(f"  Results directory: {args.results_dir}")
    print(f"  Metrics endpoint: http://{args.host}:{args.port}/metrics")
    print(f"\nServer running. Press Ctrl+C to stop.")

    from prometheus_client import make_wsgi_app
    from wsgiref.simple_server import make_server

    app = make_wsgi_app(registry=registry)
    httpd = make_server(args.host, args.port, app)
    httpd.serve_forever()



"AIzaSyBnDx9tmUlnXVp53YP4qLnFDRIQyxWWZso","AIzaSyBJRqU4WmhRiLWiwVksGY3cko1LK2FRMk0","AIzaSyDFG9Ekykd86a0Zu8DjUFikPd3J9G5HOJo","AIzaSyBL4KXVptpZtobUYL198WSVKaYUn9GoZGg","AIzaSyCCCR0gw2QmcG2EB2q9g1IjM95Woj7Sin8","AIzaSyD3cFsn19TfMSmt_BDauhlyWZWLtEfH_Uc","AIzaSyBYKGEA8Vs6xfLdFS2BES8ZhRR0xkfQXnM","AIzaSyC3Q1uEb7bFAD5W-EE4sU2SOaXRd8WHYzQ","AIzaSyCUpKd89t3P3h0-3Hu5mS2wUBLNwnFHGvs","AIzaSyDW3NppaTLjRuJcs6hHKsV_WfMK78M5WlY","AIzaSyDoPQek_Tjivmknj4u9WuPzJtQrhNyTOHI","AIzaSyAGTVbaRYs0UQAt6WwDDW9ZC6OENirqW94","AIzaSyABLcNEXOlcwZaWCJIFqY8ll2zFdxq9OeM","AIzaSyB9uuC1jiqM8Qzvfk-m2wg2__FiL0ZBjzI","AIzaSyAdZlD-M0jLnhnhxrqrCps4b8pVMy3VVQQ","AIzaSyAo_h1TFfSstq5jiyMZwsj31bNFqgMi2yU","AIzaSyCNwSvOxM7wPnKFx-4O4Wv5hQM4XqGDARw","AIzaSyAqw2GdZowaSHRtQZy8uG9qXIRzYf5PK28","AIzaSyBxIqEBLdXb-NG7O6jPUXXKzgxrOSKFzEQ","AIzaSyDW12dITGsF0PoZ3z31ygUVIZF5wFqj0MQ","AIzaSyDa4jZu1EwKpeXHfE5wcN4YlhOvkTPXQWo","AIzaSyAk8tXJqDkEfp6PIWmUDs2eIeWyKLlNJhM","AIzaSyBLJORxeKsCeU5OsX52MDkW8QYI0MOPtJY","AIzaSyCPl6vSxstcYYKP7HR-b0KQdgpMqtBWZkY","AIzaSyBaABTE58mF5RK9rkl5mCslWQRTHk-l0Fc","AIzaSyDyzOYsD4SUBiVjdBrQfHxArquZrpTTxZ8","AIzaSyCsuAhHWlqEmPr63-IL24jGkfSxW6w3UDU","AIzaSyD5v6Q2mpD_tqEmIXapImPJvI1tWNp9oi8","AIzaSyD9nQpCwIGtz3-KI-sRPxlTi_j6lzHplTU","AIzaSyBzRcjv6FW7IrrONHBUN8nJt4uz0GxTG7Y","AIzaSyDRvRs-7ARMP9F_LDS9iAckO1QM-qtjsxg","AIzaSyCA_grTOF80YE7Wl01eKAAvxDHsCzM6SaI","AIzaSyBe7PhfSXCwkCsd22ckL0kLhoGKdfTFu4Q","AIzaSyDoIkab5NOucUHOpulyGpZHMIhmERMtmb4","AIzaSyB-PHatagRwTjFHC6s4UbXpADDehN0-PeU","AIzaSyCaXAIgVt1muG0DVN7ZTBEQ2Dw88QDZtXY"