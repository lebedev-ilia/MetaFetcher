# DataFetcher - Сбор метаданных YouTube видео

Проект для сбора всех метаданных YouTube видео из различных источников.

## Установка

```bash
pip install -r requirements.txt
```

## Требования

- Python 3.7+
- YouTube Data API v3 ключ (опционально, для получения данных через API)
- yt-dlp (устанавливается через requirements.txt)
- youtube-transcript-api (для получения транскриптов)

## Использование

### Базовое использование

```python
from MetaFetcher import MetaFetcher

# Инициализация (можно без API ключа, но некоторые фичи будут недоступны)
fetcher = MetaFetcher(youtube_api_key='YOUR_API_KEY_HERE')

# Сбор всех фичей для видео
video_url = 'https://www.youtube.com/watch?v=dQw4w9WgXcQ'
features = fetcher.fetch_all_features(video_url)

# Сохранение в файл
fetcher.save_to_json(features, 'video_features.json')
```

### С транскриптом и комментариями

```python
features = fetcher.fetch_all_features(
    video_url='https://www.youtube.com/watch?v=VIDEO_ID',
    include_transcript=True,  # Получить и проанализировать транскрипт
    include_comments=True,    # Получить комментарии
    max_comments=100          # Максимальное количество комментариев
)
```

### Установка дополнительных фичей

```python
# После получения базовых фичей можно добавить информацию об эмбеддингах
features = fetcher.set_embedding_features(
    features,
    embedding_dim=1024,
    embedding_source_id='vector_store_id_123',
    vector_store_uri='postgresql://localhost/vectors',
    model_version='1.0.0'
)

# Установить статус загрузки
features = fetcher.set_download_status(features, downloaded=True)
```

## Собираемые фичи

Класс собирает более 100 различных фичей из:

1. **YouTube Data API** - базовая информация о видео, канале, статистика
2. **yt-dlp** - расширенные метаданные, форматы, плейлисты
3. **Выводные фичи** - вычисляются на основе собранных данных:
   - Длительность в секундах
   - Флаг короткого видео (<60 сек)
   - Clickbait score заголовка
   - Анализ транскрипта (слова в минуту, длина предложений и т.д.)

Полный список фичей см. в файле `features.md`.

## Структура проекта

```
DataFetcher/
├── MetaFetcher/
│   ├── __init__.py
│   └── meta_fetcher.py    # Основной класс
├── features.md            # Список всех собираемых фичей
├── requirements.txt       # Зависимости
└── README.md             # Этот файл
```

## Примечания

- Некоторые фичи требуют API ключ YouTube Data API v3
- Получение транскрипта работает только если видео имеет субтитры
- Комментарии могут быть ограничены настройками приватности видео
- Все данные возвращаются в виде словаря Python и могут быть сохранены в JSON





python _yt_dlp/_yt_dlp_metrics.py --port 8001 --host 0.0.0.0 --results-dir _yt_dlp/.results

./prometheus --config.file="prometheus.yml" --web.listen-address=":9090"

cloudflared tunnel --url http://localhost:9090