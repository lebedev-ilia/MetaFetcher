"""
Модуль для загрузки метаданных видео в Hugging Face репозиторий.

Управляет загрузкой JSON файлов в структуру:
- category/video_id/snapshot_date/metadata.json

Проверяет существующие снапшоты и предотвращает дублирование.
"""

import json
import os
import re
import pickle
import httpx
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from transliterate import translit
from utils._static import CATEGORIES2TRANSLITERATE

try:
    from huggingface_hub import HfApi, upload_large_folder
    HF_HUB_AVAILABLE = True
except ImportError as e:
    HF_HUB_AVAILABLE = False
    print("Warning: huggingface_hub is not installed. Install it with: pip install huggingface_hub", e)

class HuggingFaceUploader:
    """
    Класс для загрузки метаданных видео в Hugging Face репозиторий.
    """
    
    def __init__(
        self,
        repo_id: str,
        token: Optional[str] = None,
        repo_type: str = "dataset",
        cache_dir: str = None,
        cat2ids_path: str = ".input/cat2ids.json"
    ):
        """
        Инициализация.
        
        Args:
            repo_id: ID репозитория в формате "username/repo_name"
            token: Hugging Face токен (можно получить из HF_HUB_TOKEN env var)
            repo_type: Тип репозитория ("dataset" или "model")
        """
        if not HF_HUB_AVAILABLE:
            raise ImportError("huggingface_hub is required. Install it with: pip install huggingface_hub")

        self.cache_dir = cache_dir
        self.repo_id = repo_id
        self.repo_type = repo_type
        self.token = token
        self.api_url = f"https://huggingface.co/api/{repo_type}s/{repo_id}"
        # Загружаем cat2ids только если файл существует (опционально)
        if os.path.exists(cat2ids_path):
            try:
                self.cat2ids = json.load(open(cat2ids_path, "r"))
            except Exception as e:
                print(f"Warning: Failed to load cat2ids from {cat2ids_path}: {e}. Using empty dict.")
                self.cat2ids = {}
        else:
            # Файл не существует - используем пустой словарь
            self.cat2ids = {}
        self.headers = {"Authorization": f"Bearer {token}"}

        if os.path.exists(cache_dir):
            if os.path.exists(os.path.join(cache_dir, "repo_files.pkl")):
                self.current_repo_files = self.get_repo_files_cache()
            else:
                self.current_repo_files = self.get_current_repo_files()
        else:
            os.mkdir(cache_dir)
            self.current_repo_files = self.get_current_repo_files()
        
        print(f"Инициализирован HuggingFaceUploader для репозитория: {repo_id}")
        
        
    def get_files_from_commits(self, batch_size=100, timeout=120.0):
        files_state = 0
        cursor = 0
        with httpx.Client(timeout=timeout) as client:
            while True:
                url = f"{self.api_url}/commits/main?limit={batch_size}&cursor={cursor}"
                r = client.get(url, headers=self.headers)
                if r.status_code == 404:
                    raise ValueError("Repo not found or private (check token or repo_type)")
                r.raise_for_status()

                commits = r.json()
                if not commits:
                    break

                for commit in commits:
                    title = commit.get("title")
                    if "Batch upload" in title:
                        files_state += int(title.split(" ")[2])
                    else:
                        files_state += 1

                print(f"📦 Обработано коммитов: {cursor + len(commits)}")
                if len(commits) < batch_size:
                    break
                cursor += batch_size

        print(f"✅ Всего уникальных файлов: {files_state}")
        return files_state

    def get_repo_files_cache(self):
        files = pickle.load(open(f"{self.cache_dir}/repo_files.pkl", 'rb'))
        print(f"(get_repo_files_cache) Получено {files} файлов из кэша")
        return files

    def load_repo_files_cache(self, files):
        pickle.dump(files, open(f"{self.cache_dir}/repo_files.pkl", 'wb'))
        print(f"(load_repo_files_cache) Загружено {files} файлов в кэш")

    def update_repo_files_cache(self, new_files_cnt):
        old_len = self.current_repo_files
        self.current_repo_files += new_files_cnt
        self.load_repo_files_cache(self.current_repo_files)

        print(f"(update_repo_files_cache) Было: {old_len} | Получено: {new_files_cnt} | Кол-во после обновления: {self.current_repo_files}")


    def get_current_repo_files(self) -> List[str]:
        """
        Получает список файлов в текущем репозитории.
        """

        cache_path = os.path.join(self.cache_dir, "repo_files.pkl")

        try:
            if not os.path.exists(cache_path):
                files = self.get_files_from_commits(timeout=300, batch_size=1000)

                with open(cache_path, "wb") as f:
                    pickle.dump(files, f)
            else:
                with open(cache_path, 'rb') as f:
                    files = pickle.load(f)

        except Exception as e:
            print(f"Get repo file error: {e}")
            files = 0

        # current_repo_files = []
        
        # for file in files:
        #     sp = file.split("/")
        #     category = sp[0]
        #     if category not in list(CATEGORIES2TRANSLITERATE.values()):
        #         continue
        #     if sp[1] not in self.cat2ids[category]:
        #         continue
        #     if "meta_" not in sp[2]:
        #         continue
        #     current_repo_files.append(file)
            
        return files
        
        
    def check_for_new_batches(self, success_batchs: List[int], yt_dlp_results_dir: str) -> None:
        
        new_batchs = []
        
        for batch_name in os.listdir(yt_dlp_results_dir):
            if batch_name.startswith("batch_") and batch_name.endswith(".json"):
                if batch_name not in success_batchs:
                    new_batchs.append(batch_name)
        
        return new_batchs
    
    
    def list_existing_snapshots(self, category: str, video_id: str) -> List[Tuple[str, bool]]:
        """
        Получает список существующих снапшотов для видео в HF.

        Returns:
            Список кортежей (snapshot_date, is_meta_snapshot), отсортированный по дате
        """
        try:
            # Путь папки видео в репозитории (учитываем ту же трансформацию категории, что и при загрузке)
            category_key = translit(category, "en", reversed=True).replace(' ', '_')
            video_path = f"{category_key}/{video_id}"

            try:
                files = self.api.list_repo_files(repo_id=self.repo_id, repo_type=self.repo_type)
            except Exception as e:
                print(f"Предупреждение: не удалось получить список файлов для {video_path}: {e}")
                return []

            snapshots: List[Tuple[str, bool]] = []
            # Ищем соответствующие JSON файлы внутри подпапок видео
            pattern = re.compile(rf"{re.escape(video_path)}/([^/]+)/.*\\.json")

            for file in files:
                match = pattern.match(file)
                if not match:
                    continue
                folder_name = match.group(1)
                is_meta = folder_name.startswith('meta_')
                snapshot_date = folder_name[5:] if is_meta else folder_name
                snapshot_tuple = (snapshot_date, is_meta)
                if snapshot_tuple not in snapshots:
                    snapshots.append(snapshot_tuple)

            return sorted(snapshots, key=lambda x: x[0])
        except Exception as e:
            print(f"Ошибка при получении списка снапшотов для {category}/{video_id}: {e}")
            return []
        

    def get_video_snapshot_status(self, category: str, video_id: str) -> Dict[str, Any]:
        """
        Возвращает статус снапшотов для конкретного видео.

        Returns:
            {
              'meta_snapshot_date': Optional[str],
              'regular_snapshots': List[str],
            }
        """
        snapshots = self.list_existing_snapshots(category, video_id)
        meta_date: Optional[str] = None
        regular_dates: List[str] = []

        for snapshot_date, is_meta in snapshots:
            if is_meta:
                meta_date = snapshot_date
            else:
                regular_dates.append(snapshot_date)

        regular_dates = sorted(regular_dates)
        return {
            'meta_snapshot_date': meta_date,
            'regular_snapshots': regular_dates,
        }
        

    def determine_current_snapshot_number(self, video_list: List[Tuple[str, str]]) -> Tuple[int, int]:
        """
        Определяет текущий незавершенный номер снапшота на основе проверки списка видео.

        video_list: список кортежей (category, video_url)

        Returns: (snapshot_number, checked_count)
        """
        if not video_list:
            print("  Список видео пуст, начинаем с первого снапшота")
            return 1, 0

        max_completed: Optional[int] = None

        for idx, (category, video_url) in enumerate(video_list, start=1):
            try:
                vid = self.extract_video_id(video_url) or video_url.split('?v=')[-1]
                status = self.get_video_snapshot_status(category, vid)

                if status['meta_snapshot_date'] is None:
                    # Нет meta snapshot — следующий к выполнению: 1
                    return 1, idx - 1 if idx > 1 else 0
                else:
                    # Есть meta snapshot — снапшот 1 завершен; остальные начинаются со 2
                    completed_snapshot = 1 + len(status['regular_snapshots'])

                    if max_completed is None:
                        max_completed = completed_snapshot
                    else:
                        max_completed = max(max_completed, completed_snapshot)

                    if completed_snapshot < max_completed:
                        next_snapshot = completed_snapshot + 1
                        print(f"  {vid}: завершено до {completed_snapshot}, максимум {max_completed}, возвращаем {next_snapshot}")
                        return next_snapshot, idx - 1

            except Exception as e:
                print(f"  Ошибка при проверке видео {idx}: {e}")
                return 1, idx

        if max_completed is None:
            return 1, 0
        return max_completed + 1, 0
    
    
    def extract_video_id(self, video_url: str) -> Optional[str]:
        """
        Извлекает video ID из YouTube URL.
        
        Args:
            video_url: URL видео YouTube
            
        Returns:
            video_id или None
        """
        patterns = [
            r'(?:v=|\/)([0-9A-Za-z_-]{11}).*',
            r'youtube\.com\/watch\?v=([0-9A-Za-z_-]{11})',
            r'youtu\.be\/([0-9A-Za-z_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, video_url)
            if match:
                return match.group(1)
        
        return None

    
    def upload_metadata(
        self, 
        metadata: Dict[str, Any], 
        category: str, 
        video_id: str, 
        folder_name: Optional[str] = None, 
    ) -> bool:
        """
        Загружает метаданные видео в HF.
        
        Args:
            metadata: Словарь с метаданными видео
            category: Название категории (русское)
            video_id: ID видео
            meta_folder_name: Имя папки снапшота (если None, используется текущее время)
            
        Returns:
            True если загрузка успешна, False иначе
        """
        hf_path = f"{category}/{video_id}/{folder_name}"
        
        try:
            # Сериализуем метаданные в JSON
            json_content = json.dumps(metadata, ensure_ascii=False, indent=2, default=str)
            json_bytes = json_content.encode('utf-8')
            
            try:
                self.api.upload_file(
                    path_or_fileobj=json_bytes,
                    path_in_repo=hf_path,
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                    token=self.token
                )
            except Exception as e:
                print(f"Error uploading metadata to {hf_path}: {e}")
                return False
            
            print(f"✓ Загружено: {hf_path}")
            return True
            
        except Exception as e:
            print(f"✗ Ошибка при загрузке {hf_path}: {e}")
            return False
    
    def upload_metadata_batch(
        self,
        files_to_upload: List[Tuple[Dict[str, Any], str, str, str]],
        folder_name: str
    ) -> Tuple[int, int]:
        """
        Загружает несколько файлов метаданных одним коммитом.
        
        Args:
            files_to_upload: Список кортежей (metadata, category, video_id, hf_path)
            folder_name: Имя папки снапшота
            
        Returns:
            Кортеж (успешно загружено, всего попыток)
        """
        if not files_to_upload:
            return 0, 0
        
        import tempfile
        import shutil
        
        # Создаем временную папку для всех файлов
        temp_dir = tempfile.mkdtemp()
        uploaded_count = 0
        
        try:
            # Создаем структуру папок и файлов во временной директории
            for metadata, category, video_id, hf_path in files_to_upload:
                # Добавляем имя файла к пути (если его еще нет)
                # Структура должна быть: category/video_id/folder_name/metadata.json
                if not hf_path.endswith('.json'):
                    # Если путь не заканчивается на .json, добавляем имя файла
                    # Используем video_id как имя файла, чтобы сохранить уникальность
                    hf_file_path = f"{hf_path}/{video_id}.json"
                else:
                    hf_file_path = hf_path
                
                # Создаем полный путь во временной директории
                local_path = os.path.join(temp_dir, hf_file_path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                
                # Записываем JSON файл
                json_content = json.dumps(metadata, ensure_ascii=False, indent=2, default=str)
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(json_content)
            
            # Загружаем всю папку одним коммитом
            try:
                from huggingface_hub import upload_large_folder
                upload_large_folder(
                    folder_path=temp_dir,
                    repo_id=self.repo_id,
                    repo_type=self.repo_type,
                )
                uploaded_count = len(files_to_upload)
                print(f"✓ Загружено всего: {uploaded_count}")
            except Exception as e:
                error_str = str(e)
                # Проверяем, является ли это ошибкой rate limit
                if "429" in error_str or "rate limit" in error_str.lower() or "Too Many Requests" in error_str:
                    print(f"Rate limit reached during batch upload. Error: {e}")
                    print("Waiting 60 seconds before retry...")
                    time.sleep(60)
                    # Пробуем еще раз
                    try:
                        from huggingface_hub import upload_large_folder
                        upload_large_folder(
                            folder_path=temp_dir,
                            repo_id=self.repo_id,
                            repo_type=self.repo_type,
                        )
                        uploaded_count = len(files_to_upload)
                        print(f"✓ Загружено всего: {uploaded_count}")
                    except Exception as e2:
                        print(f"Retry failed: {e2}")
                        print("Please wait for rate limit to reset (about 1 hour)")
                        raise
                else:
                    print(f"Error uploading batch: {e}")
                    # Если batch upload не сработал, пробуем загрузить по одному
                    # (fallback на старый метод)
                    print("Falling back to individual uploads...")
                    for metadata, category, video_id, hf_path in files_to_upload:
                        if self.upload_metadata(metadata, category, video_id, folder_name):
                            uploaded_count += 1
                        time.sleep(0.5)  # Небольшая задержка между загрузками
        finally:
            # Удаляем временную папку
            shutil.rmtree(temp_dir, ignore_errors=True)
        
        return uploaded_count, len(files_to_upload)
    
    
    def upload_from_file(self, json_file: str, category: str, video_url: str,
                        snapshot_date: Optional[str] = None, overwrite: bool = False) -> bool:
        """
        Загружает метаданные из JSON файла в HF.
        
        Args:
            json_file: Путь к JSON файлу с метаданными
            category: Название категории (русское)
            video_url: URL видео (для извлечения video_id)
            snapshot_date: Дата снапшота (если None, используется текущее время)
            overwrite: Перезаписать существующий файл если True
            
        Returns:
            True если загрузка успешна, False иначе
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            return self.upload_metadata(metadata, category, video_url, snapshot_date, overwrite)
            
        except Exception as e:
            print(f"Ошибка при чтении файла {json_file}: {e}")
            return False


# Утилиты для работы с urls.json (если требуется вне класса)
def load_urls_data(urls_file: str) -> Dict[str, Any]:
    with open(urls_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_flat_video_list(urls_data: Dict[str, Any]) -> List[Tuple[str, str]]:
    """
    Преобразует структуру urls.json в плоский список (category, video_url)
    с сохранением порядка обхода исходного файла.
    """
    video_list: List[Tuple[str, str]] = []
    for category, intervals in urls_data.items():
        if not isinstance(intervals, dict):
            continue
        for _interval, videos in intervals.items():
            if not isinstance(videos, dict):
                continue
            for video_url, video_data in videos.items():
                if isinstance(video_data, dict):
                    video_list.append((category, video_url))
    return video_list
