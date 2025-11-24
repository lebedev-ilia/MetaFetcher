import os
import json
import subprocess
import time
import shutil
import logging
import tempfile
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Set, List, Optional, Any
from pathlib import Path

# Add project root to path for importing cookie_manager
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from _yt_dlp.core.cookie_manager import CookieRotationManager
    COOKIE_MANAGER_AVAILABLE = True
except ImportError:
    COOKIE_MANAGER_AVAILABLE = False
    print("Warning: CookieRotationManager not found. Cookie rotation will be disabled.")

try:
    from huggingface_hub import HfApi, upload_large_folder, login
    HF_HUB_AVAILABLE = True
except ImportError:
    HF_HUB_AVAILABLE = False
    print("Warning: huggingface_hub is not installed. Install it with: pip install huggingface_hub")

# Настройка логгера
logger = logging.getLogger('downloader')
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.FileHandler('downloader.log', encoding='utf-8')
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    # Также выводим в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# Константы
YT_DLP_RESULTS_DIR = ".results/fetcher/yt_dlp"
TEMP_DOWNLOAD_DIR = ".tmp"
CHECK_INTERVAL = 120  # 2 минуты
BATCH_SIZE = 8  # Размер батча для обработки видео


class VideoDownloader:
    """
    Класс для скачивания видео и загрузки их в HuggingFace датасет.
    """
    
    def __init__(
        self,
        repo_id: str,
        token: Optional[str] = None,
        repo_type: str = "dataset"
    ):
        """
        Инициализация загрузчика.
        
        Args:
            repo_id: ID репозитория в формате "username/repo_name"
            token: Hugging Face токен
            repo_type: Тип репозитория ("dataset" или "model")
        """
        if not HF_HUB_AVAILABLE:
            raise ImportError("huggingface_hub is required. Install it with: pip install huggingface_hub")
        
        self.repo_id = repo_id
        self.repo_type = repo_type
        self.token = token
        self.api = HfApi(token=token)
        self.cookie_lock = threading.Lock()
        
        # Инициализируем менеджер ротации cookies
        if COOKIE_MANAGER_AVAILABLE:
            self.cookie_manager = CookieRotationManager()
            logger.info(f"Инициализирован CookieRotationManager с {len(self.cookie_manager.cookie_files)} cookies")
        else:
            self.cookie_manager = None
            logger.warning("CookieRotationManager недоступен. Ротация cookies отключена.")
        
        # Создаем временную директорию для скачивания
        os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
        
        logger.info(f"Инициализирован VideoDownloader для репозитория: {repo_id}")
    
    def load_progress_from_hf(self) -> Set[str]:
        """
        Загружает progress.json из HF репозитория.
        Возвращает множество video_id уже скачанных видео.
        """
        try:
            progress_path = "progress.json"
            
            # Пытаемся скачать файл
            try:
                progress_file = self.api.hf_hub_download(
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                    filename=progress_path,
                    token=self.token
                )
                
                with open(progress_file, "r", encoding="utf-8") as f:
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
                logger.info(f"Progress файл не найден или ошибка загрузки: {e}. Начинаем с пустого списка.")
                return set()
        except Exception as e:
            logger.error(f"Ошибка при загрузке progress: {e}")
            return set()
    
    def save_progress_to_hf(self, processed_video_ids: Set[str], temp_dir: str) -> bool:
        """
        Сохраняет progress.json во временную директорию для последующей загрузки в HF.
        
        Args:
            processed_video_ids: Множество video_id обработанных видео
            temp_dir: Временная директория для подготовки файлов
        """
        try:
            progress_path = os.path.join(temp_dir, "progress.json")
            
            progress_data = {
                "processed_video_ids": sorted(list(processed_video_ids)),
                "count": len(processed_video_ids),
                "last_updated": datetime.now().isoformat()
            }
            
            with open(progress_path, "w", encoding="utf-8") as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Подготовлен progress.json ({len(processed_video_ids)} видео)")
            return True
        except Exception as e:
            logger.error(f"Ошибка подготовки progress.json: {e}")
            return False
    
    def collect_videos_from_yt_dlp(self) -> Dict[str, Dict[str, Any]]:
        """
        Собирает видео из .results/fetcher/yt_dlp файлов data_*.json.
        Возвращает словарь {video_id: video_data}
        """
        videos = {}
        
        if not os.path.isdir(YT_DLP_RESULTS_DIR):
            logger.warning(f"Директория {YT_DLP_RESULTS_DIR} не существует")
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
                            # Проверяем наличие необходимых полей
                            if "webpage_url" in video_data and "formats" in video_data:
                                videos[video_id] = video_data
                except Exception as e:
                    logger.error(f"Ошибка загрузки файла {file}: {e}")
        
        logger.info(f"Собрано {len(videos)} видео из yt_dlp файлов")
        return videos
    
    def _is_blocked_error(self, error_output: str) -> bool:
        """
        Определяет, является ли ошибка блокировкой YouTube.
        
        Args:
            error_output: Вывод stderr от yt-dlp
            
        Returns:
            True если ошибка указывает на блокировку
        """
        error_str = error_output.lower()
        
        # Проверяем специфичные признаки блокировки
        block_indicators = [
            '429',  # Too Many Requests
            '403',  # Forbidden
            'blocked',
            'rate limit',
            'too many requests',
            'unable to extract',
            'sign in to confirm',
            'sign in to confirm you\'re not a bot',
            'http error',
            'unable to download',
            'extractor error',
            'failed to resolve',
            'nodename nor servname provided'
        ]
        
        for indicator in block_indicators:
            if indicator in error_str:
                return True
        
        return False
    
    def download_video(
        self,
        video_id: str,
        video_data: Dict[str, Any],
        output_dir: str
    ) -> Optional[str]:
        """
        Скачивает видео через yt-dlp с ротацией cookies при ошибках.
        Сначала пытается скачать лучший формат (последний в списке formats),
        если не получилось - второй формат.
        Для каждого формата пробует все доступные cookies при блокировках.
        
        Args:
            video_id: ID видео
            video_data: Данные видео из yt_dlp
            output_dir: Директория для сохранения
            
        Returns:
            Путь к скачанному файлу или None при ошибке
        """
        webpage_url = video_data.get("webpage_url")
        formats = video_data.get("formats", [])
        
        if not webpage_url:
            logger.error(f"Нет webpage_url для видео {video_id}")
            return None
        
        if not formats:
            logger.error(f"Нет formats для видео {video_id}")
            return None
        
        # Лучший формат - последний в списке
        # Пытаемся сначала лучший, потом второй (предпоследний)
        format_indices = [len(formats) - 1]  # Лучший формат
        if len(formats) > 1:
            format_indices.append(len(formats) - 2)  # Второй формат
        
        # Определяем максимальное количество попыток с cookies
        max_cookie_attempts = 1
        if self.cookie_manager and self.cookie_manager.cookie_files:
            max_cookie_attempts = max(1, len(self.cookie_manager.cookie_files))
        
        for format_idx in format_indices:
            format_data = formats[format_idx]
            format_id = format_data.get("format_id")
            
            if not format_id:
                continue
            
            # Формируем имя файла
            output_template = os.path.join(output_dir, f"{video_id}.%(ext)s")
            
            # Пробуем каждый cookie для этого формата
            for cookie_attempt in range(max_cookie_attempts):
                # Получаем текущий cookie
                current_cookie = None
                if self.cookie_manager:
                    with self.cookie_lock:
                        current_cookie = self.cookie_manager.get_current_cookie()
                
                # Команда yt-dlp
                cmd = [
                    ".venv/bin/python",
                    "-m"
                    "yt_dlp",
                    "-f", format_id,
                    "-o", output_template,
                    webpage_url
                ]
                
                # Добавляем cookies если есть
                if current_cookie:
                    cmd.extend(["--cookies", current_cookie])
                
                logger.info(f"Скачивание видео {video_id} с форматом {format_id}...")
                
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=600  # 10 минут таймаут
                    )
                    
                    if result.returncode == 0:
                        # Ищем скачанный файл
                        # yt-dlp может изменить расширение, поэтому ищем по video_id
                        for file in os.listdir(output_dir):
                            if file.startswith(video_id + '.'):
                                file_path = os.path.join(output_dir, file)
                                logger.info(f"✓ Видео {video_id} успешно скачано: {file_path}")
                                return file_path
                            # Также проверяем файлы, которые могут быть без расширения в начале
                            file_without_ext = os.path.splitext(file)[0]
                            if file_without_ext == video_id:
                                file_path = os.path.join(output_dir, file)
                                logger.info(f"✓ Видео {video_id} успешно скачано: {file_path}")
                                return file_path
                        
                        logger.warning(f"Видео {video_id} скачано, но файл не найден в {output_dir}")
                        # Если файл не найден, но код возврата 0, считаем успешным и выходим
                        return None
                    
                    # Проверяем ошибку
                    error_output = result.stderr or result.stdout or ""
                    
                    # Проверяем, является ли ошибка блокировкой
                    is_blocked = self._is_blocked_error(error_output)
                    
                    # Проверяем таймаут по сообщению об ошибке
                    error_str = error_output.lower()
                    is_timeout = any(indicator in error_str for indicator in [
                        'timeout', 'timed out', 'connection timed out', 'socket timeout'
                    ])
                    
                    # Если блокировка или таймаут, и есть еще попытки - ротируем cookie
                    if (is_blocked or is_timeout) and cookie_attempt < max_cookie_attempts - 1:
                        if self.cookie_manager:
                            with self.cookie_lock:
                                next_cookie = self.cookie_manager.rotate_to_next()
                            error_type = "Таймаут" if is_timeout else "Блокировка"
                            logger.warning(
                                f"Ошибка скачивания видео {video_id} с форматом {format_id}: {error_type}. "
                                f"Повторяю с новым cookie..."
                            )
                            continue
                    
                    # Если не блокировка или это последняя попытка - логируем и переходим к следующему формату
                    logger.warning(
                        f"Ошибка скачивания видео {video_id} с форматом {format_id}: "
                        f"{error_output[:500]}"  # Ограничиваем длину вывода
                    )
                    break  # Переходим к следующему формату
                        
                except subprocess.TimeoutExpired:
                    # При таймауте пробуем следующий cookie, если есть
                    if cookie_attempt < max_cookie_attempts - 1:
                        if self.cookie_manager:
                            with self.cookie_lock:
                                self.cookie_manager.rotate_to_next()
                            logger.warning(
                                f"Таймаут при скачивании видео {video_id} с форматом {format_id}. "
                                f"Повторяю с новым cookie..."
                            )
                            continue
                    
                    logger.error(f"Таймаут при скачивании видео {video_id} с форматом {format_id}")
                    break  # Переходим к следующему формату
                except Exception as e:
                    logger.error(f"Ошибка при скачивании видео {video_id}: {e}")
                    break  # Переходим к следующему формату
        
        logger.error(f"Не удалось скачать видео {video_id} ни с одним форматом")
        return None
    
    def upload_videos_to_hf(
        self,
        video_files: List[tuple],  # [(video_id, local_path, hf_path), ...]
        temp_dir: str
    ) -> bool:
        """
        Загружает батч видео в HF.
        
        Args:
            video_files: Список кортежей (video_id, local_path, hf_path)
            temp_dir: Временная директория с подготовленными файлами
        """
        if not video_files:
            return True
        
        try:
            logger.info(f"Загрузка {len(video_files)} видео в HF...")
            
            upload_large_folder(
                folder_path=temp_dir,
                repo_id=self.repo_id,
                repo_type=self.repo_type,
            )
            
            logger.info(f"✓ Успешно загружено {len(video_files)} видео")
            return True
            
        except Exception as e:
            error_str = str(e)
            # Проверяем, является ли это ошибкой rate limit
            if "429" in error_str or "rate limit" in error_str.lower() or "Too Many Requests" in error_str:
                logger.warning(f"Rate limit reached. Error: {e}")
                logger.info("Waiting 60 seconds before retry...")
                time.sleep(60)
                # Пробуем еще раз
                try:
                    upload_large_folder(
                        folder_path=temp_dir,
                        repo_id=self.repo_id,
                        repo_type=self.repo_type,
                    )
                    logger.info(f"✓ Успешно загружено {len(video_files)} видео (retry)")
                    return True
                except Exception as e2:
                    logger.error(f"Retry failed: {e2}")
                    return False
            else:
                logger.error(f"Error uploading batch: {e}")
                return False
    
    def process_single_batch(
        self,
        batch_videos: Dict[str, Dict[str, Any]],
        processed_video_ids: Set[str]
    ) -> Set[str]:
        """
        Обрабатывает один батч видео: скачивает, загружает в HF, очищает tmp.
        
        Args:
            batch_videos: Словарь {video_id: video_data} для обработки в этом батче
            processed_video_ids: Множество уже обработанных video_id (для обновления прогресса)
        
        Returns:
            Множество успешно обработанных video_id
        """
        batch_num = len(processed_video_ids) // BATCH_SIZE + 1
        logger.info(f"Обработка батча #{batch_num} ({len(batch_videos)} видео)...")
        
        # Создаем временные директории для этого батча
        temp_upload_dir = tempfile.mkdtemp()
        temp_download_dir = os.path.join(TEMP_DOWNLOAD_DIR, f"batch_{int(time.time())}_{batch_num}")
        os.makedirs(temp_download_dir, exist_ok=True)
        
        successfully_processed = set()
        video_files_to_upload = []
        
        try:
            def process_video(video_id: str, video_data: Dict[str, Any]) -> Optional[tuple]:
                try:
                    downloaded_file = self.download_video(video_id, video_data, temp_download_dir)
                    if not downloaded_file:
                        return None
                    
                    file_name = os.path.basename(downloaded_file)
                    hf_path = file_name
                    upload_file_path = os.path.join(temp_upload_dir, hf_path)
                    shutil.copy2(downloaded_file, upload_file_path)
                    
                    logger.info(f"Подготовлено для загрузки: {video_id} -> {hf_path}")
                    return (video_id, upload_file_path, hf_path)
                except Exception as e:
                    logger.error(f"Ошибка при обработке видео {video_id}: {e}")
                    return None

            with ThreadPoolExecutor(max_workers=2) as executor:
                future_to_video = {
                    executor.submit(process_video, video_id, video_data): video_id
                    for video_id, video_data in batch_videos.items()
                }
                
                for future in as_completed(future_to_video):
                    result = future.result()
                    if not result:
                        continue
                    video_id, upload_file_path, hf_path = result
                    video_files_to_upload.append((video_id, upload_file_path, hf_path))
                    successfully_processed.add(video_id)
            
            # Загружаем батч в HF
            if video_files_to_upload:
                # Обновляем прогресс (включая уже обработанные + новые из этого батча)
                all_processed = processed_video_ids | successfully_processed
                self.save_progress_to_hf(all_processed, temp_upload_dir)
                
                # Загружаем все файлы одним коммитом
                if self.upload_videos_to_hf(video_files_to_upload, temp_upload_dir):
                    logger.info(f"✓ Батч #{batch_num}: успешно обработано {len(successfully_processed)} видео")
                else:
                    logger.error(f"Ошибка при загрузке батча #{batch_num} в HF")
                    # Не добавляем в processed, если загрузка не удалась
                    successfully_processed = set()
            else:
                logger.warning(f"Батч #{batch_num}: нет видео для загрузки")
        
        finally:
            # Очищаем временные директории после обработки батча
            try:
                shutil.rmtree(temp_upload_dir, ignore_errors=True)
                shutil.rmtree(temp_download_dir, ignore_errors=True)
                logger.info(f"Батч #{batch_num}: временные директории очищены")
            except Exception as e:
                logger.warning(f"Ошибка при очистке временных директорий батча #{batch_num}: {e}")
        
        return successfully_processed
    
    def process_batch(self) -> int:
        """
        Обрабатывает все новые видео, разбивая их на батчи по BATCH_SIZE.
        Каждый батч обрабатывается отдельно: скачивание -> загрузка в HF -> очистка tmp.
        
        Returns:
            Количество успешно обработанных видео
        """
        logger.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Начало обработки...")
        
        # Загружаем прогресс из HF
        processed_video_ids = self.load_progress_from_hf()
        logger.info(f"Уже обработано видео: {len(processed_video_ids)}")
        
        # Собираем видео из yt_dlp
        all_videos = self.collect_videos_from_yt_dlp()
        
        # Фильтруем уже обработанные
        new_videos = {
            video_id: video_data
            for video_id, video_data in all_videos.items()
            if video_id not in processed_video_ids
        }
        
        if not new_videos:
            logger.info("Нет новых видео для обработки")
            return 0
        
        logger.info(f"Найдено {len(new_videos)} новых видео для обработки")
        
        # Разбиваем на батчи по BATCH_SIZE
        video_items = list(new_videos.items())
        total_batches = (len(video_items) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Видео будут обработаны в {total_batches} батчах по {BATCH_SIZE} видео")
        
        total_successfully_processed = 0
        current_processed = processed_video_ids.copy()
        
        # Обрабатываем каждый батч отдельно
        for batch_idx in range(0, len(video_items), BATCH_SIZE):
            batch_items = video_items[batch_idx:batch_idx + BATCH_SIZE]
            batch_videos = dict(batch_items)
            
            # Обрабатываем батч: скачивание -> загрузка в HF -> очистка tmp
            successfully_processed = self.process_single_batch(batch_videos, current_processed)
            
            # Обновляем текущий прогресс
            current_processed.update(successfully_processed)
            total_successfully_processed += len(successfully_processed)
            
            logger.info(f"Прогресс: обработано {len(current_processed)} видео из {len(all_videos)}")
        
        logger.info(f"✓ Всего успешно обработано в этом цикле: {total_successfully_processed} видео")
        return total_successfully_processed
    
    def run(self):
        """
        Запускает основной цикл обработки каждые 2 минуты.
        """
        logger.info("Запуск загрузчика видео")
        logger.info(f"Проверка каждые {CHECK_INTERVAL} секунд")
        logger.info(f"Репозиторий: {self.repo_id}")
        
        while True:
            try:
                processed_count = self.process_batch()
                logger.info(f"Обработано видео в этом цикле: {processed_count}")
                logger.info(f"Ожидание {CHECK_INTERVAL} секунд до следующей проверки...")
                time.sleep(CHECK_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("\nОстановка...")
                break
            except Exception as e:
                logger.error(f"Ошибка в main loop: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(60)  # Ждем минуту перед повтором при ошибке


def main():
    """
    Главная функция для запуска загрузчика.
    """
    # Настройки (можно вынести в конфиг или env переменные)
    token = os.getenv("HF_HUB_TOKEN")
    repo_id = os.getenv("HF_VIDEO_REPO_ID", "Ilialebedev/videos")  # Отдельный репозиторий для видео
    
    # Логинимся в HF
    login(token=token)
    
    # Создаем загрузчик
    downloader = VideoDownloader(
        repo_id=repo_id,
        token=token,
        repo_type="dataset"
    )
    
    # Запускаем
    downloader.run()


if __name__ == "__main__":
    main()
    
    