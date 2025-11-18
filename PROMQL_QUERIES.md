# PromQL Запросы для метрик MetaFetcher

Этот документ содержит основные PromQL запросы для всех метрик, экспортируемых через `metrics.py`.

## META_SNAPSHOT Метрики

### Общие метрики

```promql
# Общее количество видео
fetcher_meta_videos_total
```

### Title (Заголовок)

```promql
# Статистика длины заголовка (min, max, mean, median)
fetcher_meta_title_length{stat="min"}
fetcher_meta_title_length{stat="max"}
fetcher_meta_title_length{stat="mean"}
fetcher_meta_title_length{stat="median"}

# Количество значений
fetcher_meta_title_length_count

# Распределение длин заголовков по диапазонам
fetcher_meta_title_length_distribution{range="<=10"}
fetcher_meta_title_length_distribution{range="<=20"}
# ... и т.д.
```

### Description (Описание)

```promql
# Статистика длины описания
fetcher_meta_description_length{stat="min"}
fetcher_meta_description_length{stat="max"}
fetcher_meta_description_length{stat="mean"}
fetcher_meta_description_length{stat="median"}

# Количество значений
fetcher_meta_description_length_count

# Распределение длин описаний
fetcher_meta_description_length_distribution{range="<=50"}
fetcher_meta_description_length_distribution{range="<=100"}
# ... и т.д.

# Наличие описания
fetcher_meta_description_presence_total{presence="empty"}
fetcher_meta_description_presence_total{presence="non_empty"}
```

### Tags (Теги)

```promql
# Статистика количества тегов
fetcher_meta_tags_count{stat="min"}
fetcher_meta_tags_count{stat="max"}
fetcher_meta_tags_count{stat="mean"}
fetcher_meta_tags_count{stat="median"}

# Количество значений
fetcher_meta_tags_count_count

# Распределение количества тегов
fetcher_meta_tags_count_distribution{range="<=0"}
fetcher_meta_tags_count_distribution{range="<=2"}
# ... и т.д.

# Наличие тегов
fetcher_meta_tags_presence_total{presence="with_tags"}
fetcher_meta_tags_presence_total{presence="without_tags"}

# Топ-20 самых частых тегов
fetcher_meta_tags_top20{tag="название_тега"}
```

### Language (Язык)

```promql
# Распределение языков
fetcher_meta_language_distribution_total{language="ru"}
fetcher_meta_language_distribution_total{language="en"}
# ... и т.д.

# Количество видео без указания языка
fetcher_meta_language_missing_total
```

### View Count (Просмотры)

```promql
# Статистика просмотров
fetcher_meta_view_count{stat="min"}
fetcher_meta_view_count{stat="max"}
fetcher_meta_view_count{stat="mean"}
fetcher_meta_view_count{stat="median"}

# Количество значений
fetcher_meta_view_count_count

# Распределение просмотров
fetcher_meta_view_count_distribution{range="<=0"}
fetcher_meta_view_count_distribution{range="<=10"}
# ... и т.д.

# Распределение относительно медианы
fetcher_meta_view_count_median_distribution_total{position="above"}
fetcher_meta_view_count_median_distribution_total{position="below"}
fetcher_meta_view_count_median_distribution_total{position="equal"}
```

### Like Count (Лайки)

```promql
# Статистика лайков
fetcher_meta_like_count{stat="min"}
fetcher_meta_like_count{stat="max"}
fetcher_meta_like_count{stat="mean"}
fetcher_meta_like_count{stat="median"}

# Количество значений
fetcher_meta_like_count_count

# Распределение лайков
fetcher_meta_like_count_distribution{range="<=0"}
fetcher_meta_like_count_distribution{range="<=1"}
# ... и т.д.

# Соотношение лайков к просмотрам
fetcher_meta_like_view_ratio{stat="min"}
fetcher_meta_like_view_ratio{stat="max"}
fetcher_meta_like_view_ratio{stat="mean"}
fetcher_meta_like_view_ratio{stat="median"}
```

### Comment Count (Комментарии)

```promql
# Статистика комментариев
fetcher_meta_comment_count{stat="min"}
fetcher_meta_comment_count{stat="max"}
fetcher_meta_comment_count{stat="mean"}
fetcher_meta_comment_count{stat="median"}

# Количество значений
fetcher_meta_comment_count_count

# Распределение комментариев
fetcher_meta_comment_count_distribution{range="<=0"}
fetcher_meta_comment_count_distribution{range="<=1"}
# ... и т.д.

# Соотношение комментариев к просмотрам
fetcher_meta_comment_view_ratio{stat="min"}
fetcher_meta_comment_view_ratio{stat="max"}
fetcher_meta_comment_view_ratio{stat="mean"}
fetcher_meta_comment_view_ratio{stat="median"}

# Количество видео без комментариев
fetcher_meta_comment_count_missing_total
```

### Thumbnails (Превью)

```promql
# Наличие превью
fetcher_meta_thumbnails_presence_total{presence="present"}
fetcher_meta_thumbnails_presence_total{presence="missing"}

# Распределение размеров превью
fetcher_meta_thumbnail_size_distribution_total{size="1280x720"}
fetcher_meta_thumbnail_size_distribution_total{size="640x480"}
# ... и т.д.
```

### Duration (Длительность)

```promql
# Статистика длительности видео (в секундах)
fetcher_meta_duration_seconds{stat="min"}
fetcher_meta_duration_seconds{stat="max"}
fetcher_meta_duration_seconds{stat="mean"}
fetcher_meta_duration_seconds{stat="median"}

# Количество значений
fetcher_meta_duration_seconds_count

# Распределение по диапазонам
fetcher_meta_duration_range_distribution_total{range="0-60s"}
fetcher_meta_duration_range_distribution_total{range="1-5min"}
fetcher_meta_duration_range_distribution_total{range="5-15min"}
fetcher_meta_duration_range_distribution_total{range="15-60min"}
fetcher_meta_duration_range_distribution_total{range=">60min"}
```

### Published Date (Дата публикации)

```promql
# Распределение по временным интервалам
fetcher_meta_published_time_interval_total{interval="less-1day"}
fetcher_meta_published_time_interval_total{interval="1day-1week"}
fetcher_meta_published_time_interval_total{interval="1week-1month"}
fetcher_meta_published_time_interval_total{interval="1month-1year"}
fetcher_meta_published_time_interval_total{interval=">1year"}

# Распределение по дням недели
fetcher_meta_published_weekday_total{weekday="Monday"}
fetcher_meta_published_weekday_total{weekday="Tuesday"}
# ... и т.д.

# Распределение по часам
fetcher_meta_published_hour_total{hour="0"}
fetcher_meta_published_hour_total{hour="1"}
# ... и т.д. (0-23)

# Распределение по месяцам
fetcher_meta_published_month_total{month="Jan"}
fetcher_meta_published_month_total{month="Feb"}
# ... и т.д.
```

### Channel (Канал)

```promql
# Количество уникальных каналов
fetcher_meta_unique_channels_total

# Топ-20 каналов по количеству видео
fetcher_meta_channels_top20{channel="название_канала"}

# Среднее количество видео на канал
fetcher_meta_avg_videos_per_channel
```

### Subscriber Count (Подписчики)

```promql
# Статистика подписчиков
fetcher_meta_subscriber_count{stat="min"}
fetcher_meta_subscriber_count{stat="max"}
fetcher_meta_subscriber_count{stat="mean"}
fetcher_meta_subscriber_count{stat="median"}

# Количество значений
fetcher_meta_subscriber_count_count

# Распределение подписчиков
fetcher_meta_subscriber_count_distribution{range="<=0"}
fetcher_meta_subscriber_count_distribution{range="<=100"}
# ... и т.д.

# Категории размера канала
fetcher_meta_channel_size_category_total{category="micro"}
fetcher_meta_channel_size_category_total{category="small"}
fetcher_meta_channel_size_category_total{category="medium"}
fetcher_meta_channel_size_category_total{category="large"}
fetcher_meta_channel_size_category_total{category="mega"}
```

### Video Count (Количество видео канала)

```promql
# Статистика количества видео канала
fetcher_meta_channel_video_count{stat="min"}
fetcher_meta_channel_video_count{stat="max"}
fetcher_meta_channel_video_count{stat="mean"}
fetcher_meta_channel_video_count{stat="median"}

# Количество значений
fetcher_meta_channel_video_count_count

# Распределение
fetcher_meta_channel_video_count_distribution{range="<=0"}
# ... и т.д.
```

### Channel View Count (Просмотры канала)

```promql
# Статистика просмотров канала
fetcher_meta_channel_view_count{stat="min"}
fetcher_meta_channel_view_count{stat="max"}
fetcher_meta_channel_view_count{stat="mean"}
fetcher_meta_channel_view_count{stat="median"}

# Количество значений
fetcher_meta_channel_view_count_count

# Распределение
fetcher_meta_channel_view_count_distribution{range="<=0"}
# ... и т.д.
```

### Country (Страна)

```promql
# Топ-20 стран
fetcher_meta_country_top20{country="RU"}
fetcher_meta_country_top20{country="US"}
# ... и т.д.

# Количество уникальных стран
fetcher_meta_unique_countries_total

# Количество видео без указания страны
fetcher_meta_country_missing_total
```

### Comments (Комментарии из массива)

```promql
# Количество комментариев из массива
fetcher_meta_comments_array_count{stat="min"}
fetcher_meta_comments_array_count{stat="max"}
fetcher_meta_comments_array_count{stat="mean"}
fetcher_meta_comments_array_count{stat="median"}

# Количество значений
fetcher_meta_comments_array_count_count

# Распределение
fetcher_meta_comments_array_count_distribution{range="<=0"}
# ... и т.д.

# Длина текста комментария
fetcher_meta_comment_text_length{stat="min"}
fetcher_meta_comment_text_length{stat="max"}
fetcher_meta_comment_text_length{stat="mean"}
fetcher_meta_comment_text_length{stat="median"}

# Количество комментариев с пустым текстом
fetcher_meta_comment_empty_text_total

# Количество лайков комментария
fetcher_meta_comment_like_count{stat="min"}
fetcher_meta_comment_like_count{stat="max"}
fetcher_meta_comment_like_count{stat="mean"}
fetcher_meta_comment_like_count{stat="median"}

# Количество ответов на комментарий
fetcher_meta_comment_reply_count{stat="min"}
fetcher_meta_comment_reply_count{stat="max"}
fetcher_meta_comment_reply_count{stat="mean"}
fetcher_meta_comment_reply_count{stat="median"}

# Распределение комментариев по временным интервалам
fetcher_meta_comment_time_interval_total{interval="less-1day"}
fetcher_meta_comment_time_interval_total{interval="1day-1week"}
# ... и т.д.

# Топ-20 авторов комментариев
fetcher_meta_comment_author_top20{author="имя_автора"}
```

### Категории и корреляции

```promql
# Количество видео по категориям
fetcher_meta_videos_by_category_total{category="название_категории"}

# Корреляции
fetcher_meta_correlation_views_likes
fetcher_meta_correlation_views_comments
fetcher_meta_correlation_views_subscribers

# Engagement rate (уровень вовлеченности)
fetcher_meta_engagement_rate{stat="min"}
fetcher_meta_engagement_rate{stat="max"}
fetcher_meta_engagement_rate{stat="mean"}
fetcher_meta_engagement_rate{stat="median"}
```

## SNAPSHOT_N Метрики

### Общие метрики snapshot

```promql
# Количество снапшотов
fetcher_snapshot_count_total

# Количество временных меток в снапшоте
fetcher_snapshot_timestamps_count{snapshot="1"}
fetcher_snapshot_timestamps_count{snapshot="2"}
# ... и т.д.

# Количество видео по временным меткам
fetcher_snapshot_timestamp_videos_count{snapshot="1", timestamp="2024-01-01T00:00:00"}

# Количество видео в снапшоте
fetcher_snapshot_videos_count{snapshot="1"}

# Временной интервал от meta_snapshot до снапшота (часы)
fetcher_snapshot_time_interval_hours{snapshot="1"}
```

### View Count Deltas (Дельты просмотров)

```promql
# Статистика дельт просмотров для snapshot N
fetcher_snapshot_1_view_count_delta{stat="min"}
fetcher_snapshot_1_view_count_delta{stat="max"}
fetcher_snapshot_1_view_count_delta{stat="mean"}
fetcher_snapshot_1_view_count_delta{stat="median"}

# Количество значений
fetcher_snapshot_1_view_count_delta_count

# Распределение дельт
fetcher_snapshot_1_view_count_delta_distribution{range="<=-100000"}
# ... и т.д.

# Направление дельты
fetcher_snapshot_1_view_count_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_view_count_delta_direction_total{direction="negative", snapshot="1"}
fetcher_snapshot_1_view_count_delta_direction_total{direction="zero", snapshot="1"}

# Процент изменения просмотров
fetcher_snapshot_1_view_count_percent_change{stat="min"}
fetcher_snapshot_1_view_count_percent_change{stat="max"}
fetcher_snapshot_1_view_count_percent_change{stat="mean"}
fetcher_snapshot_1_view_count_percent_change{stat="median"}

# Распределение процентов изменения
fetcher_snapshot_1_view_count_percent_change_distribution{range="<=-100"}
# ... и т.д.

# Скорость роста просмотров (в час)
fetcher_snapshot_1_view_count_growth_rate{stat="min"}
fetcher_snapshot_1_view_count_growth_rate{stat="max"}
fetcher_snapshot_1_view_count_growth_rate{stat="mean"}
fetcher_snapshot_1_view_count_growth_rate{stat="median"}

# Топ-20 видео с наибольшим ростом/падением просмотров
fetcher_snapshot_1_view_count_top20_growth{video_id="VIDEO_ID", snapshot="1"}
fetcher_snapshot_1_view_count_top20_decline{video_id="VIDEO_ID", snapshot="1"}
```

### Like Count Deltas (Дельты лайков)

```promql
# Статистика дельт лайков
fetcher_snapshot_1_like_count_delta{stat="min"}
fetcher_snapshot_1_like_count_delta{stat="max"}
fetcher_snapshot_1_like_count_delta{stat="mean"}
fetcher_snapshot_1_like_count_delta{stat="median"}

# Распределение дельт
fetcher_snapshot_1_like_count_delta_distribution{range="<=-10000"}
# ... и т.д.

# Направление дельты
fetcher_snapshot_1_like_count_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_like_count_delta_direction_total{direction="negative", snapshot="1"}
fetcher_snapshot_1_like_count_delta_direction_total{direction="zero", snapshot="1"}

# Процент изменения лайков
fetcher_snapshot_1_like_count_percent_change{stat="min"}
fetcher_snapshot_1_like_count_percent_change{stat="max"}
fetcher_snapshot_1_like_count_percent_change{stat="mean"}
fetcher_snapshot_1_like_count_percent_change{stat="median"}

# Скорость роста лайков
fetcher_snapshot_1_like_count_growth_rate{stat="min"}
fetcher_snapshot_1_like_count_growth_rate{stat="max"}
fetcher_snapshot_1_like_count_growth_rate{stat="mean"}
fetcher_snapshot_1_like_count_growth_rate{stat="median"}

# Топ-20 видео с наибольшим ростом лайков
fetcher_snapshot_1_like_count_top20_growth{video_id="VIDEO_ID", snapshot="1"}
```

### Comment Count Deltas (Дельты комментариев)

```promql
# Статистика дельт комментариев
fetcher_snapshot_1_comment_count_delta{stat="min"}
fetcher_snapshot_1_comment_count_delta{stat="max"}
fetcher_snapshot_1_comment_count_delta{stat="mean"}
fetcher_snapshot_1_comment_count_delta{stat="median"}

# Распределение дельт
fetcher_snapshot_1_comment_count_delta_distribution{range="<=-1000"}
# ... и т.д.

# Направление дельты
fetcher_snapshot_1_comment_count_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_comment_count_delta_direction_total{direction="negative", snapshot="1"}
fetcher_snapshot_1_comment_count_delta_direction_total{direction="zero", snapshot="1"}

# Процент изменения комментариев
fetcher_snapshot_1_comment_count_percent_change{stat="min"}
fetcher_snapshot_1_comment_count_percent_change{stat="max"}
fetcher_snapshot_1_comment_count_percent_change{stat="mean"}
fetcher_snapshot_1_comment_count_percent_change{stat="median"}

# Скорость роста комментариев
fetcher_snapshot_1_comment_count_growth_rate{stat="min"}
fetcher_snapshot_1_comment_count_growth_rate{stat="max"}
fetcher_snapshot_1_comment_count_growth_rate{stat="mean"}
fetcher_snapshot_1_comment_count_growth_rate{stat="median"}

# Топ-20 видео с наибольшим ростом комментариев
fetcher_snapshot_1_comment_count_top20_growth{video_id="VIDEO_ID", snapshot="1"}
```

### Subscriber Count Deltas (Дельты подписчиков)

```promql
# Статистика дельт подписчиков
fetcher_snapshot_1_subscriber_count_delta{stat="min"}
fetcher_snapshot_1_subscriber_count_delta{stat="max"}
fetcher_snapshot_1_subscriber_count_delta{stat="mean"}
fetcher_snapshot_1_subscriber_count_delta{stat="median"}

# Распределение дельт
fetcher_snapshot_1_subscriber_count_delta_distribution{range="<=-100000"}
# ... и т.д.

# Направление дельты
fetcher_snapshot_1_subscriber_count_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_subscriber_count_delta_direction_total{direction="negative", snapshot="1"}
fetcher_snapshot_1_subscriber_count_delta_direction_total{direction="zero", snapshot="1"}

# Процент изменения подписчиков
fetcher_snapshot_1_subscriber_count_percent_change{stat="min"}
fetcher_snapshot_1_subscriber_count_percent_change{stat="max"}
fetcher_snapshot_1_subscriber_count_percent_change{stat="mean"}
fetcher_snapshot_1_subscriber_count_percent_change{stat="median"}

# Скорость роста подписчиков
fetcher_snapshot_1_subscriber_count_growth_rate{stat="min"}
fetcher_snapshot_1_subscriber_count_growth_rate{stat="max"}
fetcher_snapshot_1_subscriber_count_growth_rate{stat="mean"}
fetcher_snapshot_1_subscriber_count_growth_rate{stat="median"}

# Топ-20 каналов с наибольшим ростом подписчиков
fetcher_snapshot_1_subscriber_count_top20_growth{channel="CHANNEL_NAME", snapshot="1"}
```

### Video Count Deltas (Дельты количества видео)

```promql
# Статистика дельт количества видео
fetcher_snapshot_1_video_count_delta{stat="min"}
fetcher_snapshot_1_video_count_delta{stat="max"}
fetcher_snapshot_1_video_count_delta{stat="mean"}
fetcher_snapshot_1_video_count_delta{stat="median"}

# Направление дельты
fetcher_snapshot_1_video_count_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_video_count_delta_direction_total{direction="negative", snapshot="1"}
fetcher_snapshot_1_video_count_delta_direction_total{direction="zero", snapshot="1"}

# Процент изменения
fetcher_snapshot_1_video_count_percent_change{stat="min"}
fetcher_snapshot_1_video_count_percent_change{stat="max"}
fetcher_snapshot_1_video_count_percent_change{stat="mean"}
fetcher_snapshot_1_video_count_percent_change{stat="median"}

# Скорость роста
fetcher_snapshot_1_video_count_growth_rate{stat="min"}
fetcher_snapshot_1_video_count_growth_rate{stat="max"}
fetcher_snapshot_1_video_count_growth_rate{stat="mean"}
fetcher_snapshot_1_video_count_growth_rate{stat="median"}
```

### Channel View Count Deltas (Дельты просмотров канала)

```promql
# Статистика дельт просмотров канала
fetcher_snapshot_1_view_count_channel_delta{stat="min"}
fetcher_snapshot_1_view_count_channel_delta{stat="max"}
fetcher_snapshot_1_view_count_channel_delta{stat="mean"}
fetcher_snapshot_1_view_count_channel_delta{stat="median"}

# Направление дельты
fetcher_snapshot_1_view_count_channel_delta_direction_total{direction="positive", snapshot="1"}
fetcher_snapshot_1_view_count_channel_delta_direction_total{direction="negative", snapshot="1"}

# Процент изменения
fetcher_snapshot_1_view_count_channel_percent_change{stat="min"}
fetcher_snapshot_1_view_count_channel_percent_change{stat="max"}
fetcher_snapshot_1_view_count_channel_percent_change{stat="mean"}
fetcher_snapshot_1_view_count_channel_percent_change{stat="median"}

# Скорость роста
fetcher_snapshot_1_view_count_channel_growth_rate{stat="min"}
fetcher_snapshot_1_view_count_channel_growth_rate{stat="max"}
fetcher_snapshot_1_view_count_channel_growth_rate{stat="mean"}
fetcher_snapshot_1_view_count_channel_growth_rate{stat="median"}
```

### Comments Array Deltas (Дельты комментариев из массива)

```promql
# Статистика дельт комментариев из массива
fetcher_snapshot_1_comments_array_delta{stat="min"}
fetcher_snapshot_1_comments_array_delta{stat="max"}
fetcher_snapshot_1_comments_array_delta{stat="mean"}
fetcher_snapshot_1_comments_array_delta{stat="median"}

# Направление дельты
fetcher_snapshot_1_comments_array_delta_direction_total{direction="new_comments", snapshot="1"}
fetcher_snapshot_1_comments_array_delta_direction_total{direction="no_new", snapshot="1"}

# Дельты текста, лайков и ответов комментариев
fetcher_snapshot_1_comment_text_length_delta{stat="min"}
fetcher_snapshot_1_comment_like_count_delta{stat="min"}
fetcher_snapshot_1_comment_reply_count_delta{stat="min"}

# Количество новых уникальных авторов комментариев
fetcher_snapshot_1_new_comment_authors_total

# Топ-20 видео с наибольшим количеством новых комментариев
fetcher_snapshot_1_new_comments_top20{video_id="VIDEO_ID", snapshot="1"}
```

### Engagement Rate Deltas (Дельты уровня вовлеченности)

```promql
# Статистика дельт engagement rate
fetcher_snapshot_1_engagement_rate_delta{stat="min"}
fetcher_snapshot_1_engagement_rate_delta{stat="max"}
fetcher_snapshot_1_engagement_rate_delta{stat="mean"}
fetcher_snapshot_1_engagement_rate_delta{stat="median"}

# Распределение дельт
fetcher_snapshot_1_engagement_rate_delta_distribution{range="<=-0.1"}
# ... и т.д.

# Направление дельты
fetcher_snapshot_1_engagement_rate_delta_direction_total{direction="increase", snapshot="1"}
fetcher_snapshot_1_engagement_rate_delta_direction_total{direction="decrease", snapshot="1"}

# Топ-20 видео с наибольшим ростом engagement rate
fetcher_snapshot_1_engagement_rate_top20_growth{video_id="VIDEO_ID", snapshot="1"}
```

### Корреляции дельт

```promql
# Корреляция между дельтой просмотров и дельтой лайков
fetcher_snapshot_1_correlation_view_like_delta{snapshot="1"}

# Корреляция между дельтой просмотров и дельтой комментариев
fetcher_snapshot_1_correlation_view_comment_delta{snapshot="1"}

# Корреляция между дельтой подписчиков и дельтой просмотров
fetcher_snapshot_1_correlation_subscriber_view_delta{snapshot="1"}

# Средняя дельта просмотров в час
fetcher_snapshot_1_avg_view_delta_per_hour{snapshot="1"}

# Корреляция между возрастом видео и дельтой просмотров
fetcher_snapshot_1_correlation_age_view_delta{snapshot="1"}
```

### Дельты по временным интервалам публикации

```promql
# Распределение дельт просмотров по временному интервалу публикации
fetcher_snapshot_1_view_delta_by_publish_interval_total{interval="less-1day", snapshot="1"}
fetcher_snapshot_1_view_delta_by_publish_interval_total{interval="1day-1week", snapshot="1"}
# ... и т.д.

# Средняя дельта просмотров по временным интервалам
fetcher_snapshot_1_avg_view_delta_by_publish_interval{interval="less-1day", snapshot="1"}
fetcher_snapshot_1_avg_like_delta_by_publish_interval{interval="less-1day", snapshot="1"}
fetcher_snapshot_1_avg_comment_delta_by_publish_interval{interval="less-1day", snapshot="1"}
```

### Дельты по категориям каналов

```promql
# Средняя дельта просмотров по категории канала
fetcher_snapshot_1_view_delta_by_category{category="название_категории", snapshot="1"}

# Средняя дельта лайков по категории канала
fetcher_snapshot_1_like_delta_by_category{category="название_категории", snapshot="1"}

# Средняя дельта комментариев по категории канала
fetcher_snapshot_1_comment_delta_by_category{category="название_категории", snapshot="1"}
```

## YT_DLP Метрики

### Общие метрики

```promql
# Общее количество обработанных видео
ytdlp_videos_total
```

### Age Limit

```promql
# Статистика age_limit
ytdlp_video_age_limit{stat="min"}
ytdlp_video_age_limit{stat="max"}
ytdlp_video_age_limit{stat="mean"}

# Количество значений
ytdlp_video_age_limit_count
```

### Subtitles (Субтитры)

```promql
# Общее количество субтитров
ytdlp_subtitles_total{language="ru"}
ytdlp_subtitles_total{language="en"}

# Количество пустых субтитров
ytdlp_subtitles_empty_total{language="ru"}
ytdlp_subtitles_empty_total{language="en"}

# Длина субтитров (символов)
ytdlp_subtitles_length_characters{language="ru", stat="min"}
ytdlp_subtitles_length_characters{language="ru", stat="max"}
ytdlp_subtitles_length_characters{language="ru", stat="mean"}

# Количество записей с текстом
ytdlp_subtitles_length_characters_count{language="ru"}
ytdlp_subtitles_length_characters_count{language="en"}
```

### Automatic Captions (Автоматические субтитры)

```promql
# Общее количество автоматических субтитров
ytdlp_automatic_captions_total{language="ru"}
ytdlp_automatic_captions_total{language="en"}

# Количество пустых автоматических субтитров
ytdlp_automatic_captions_empty_total{language="ru"}
ytdlp_automatic_captions_empty_total{language="en"}

# Длина автоматических субтитров
ytdlp_automatic_captions_length_characters{language="ru", stat="min"}
ytdlp_automatic_captions_length_characters{language="ru", stat="max"}
ytdlp_automatic_captions_length_characters{language="ru", stat="mean"}

# Количество записей
ytdlp_automatic_captions_length_characters_count{language="ru"}
ytdlp_automatic_captions_length_characters_count{language="en"}
```

### Chapters (Главы)

```promql
# Общее количество глав
ytdlp_chapters_total

# Количество видео с главами
ytdlp_videos_with_chapters_total

# Количество видео без глав
ytdlp_videos_without_chapters_total
```

### Formats (Форматы)

```promql
# Общее количество форматов
ytdlp_formats_total

# Количество видео с форматами
ytdlp_videos_with_formats_total

# Количество видео без форматов
ytdlp_videos_without_formats_total

# Распределение по разрешениям
ytdlp_resolution_count{resolution="1920x1080"}
ytdlp_resolution_count{resolution="1280x720"}
# ... и т.д.
```

### Thumbnails (Превью)

```promql
# Общее количество превью
ytdlp_thumbnails_total

# Количество видео с превью
ytdlp_videos_with_thumbnails_total

# Количество видео без превью
ytdlp_videos_without_thumbnails_total
```

### Video Duration (Длительность видео)

```promql
# Статистика длительности видео (секунды)
ytdlp_video_duration_seconds{stat="min"}
ytdlp_video_duration_seconds{stat="max"}
ytdlp_video_duration_seconds{stat="mean"}

# Количество значений
ytdlp_video_duration_seconds_count
```

### Timing Stats (Статистика времени обработки)

```promql
# Время извлечения информации о видео (секунды)
ytdlp_extract_info_seconds{stat="min"}
ytdlp_extract_info_seconds{stat="max"}
ytdlp_extract_info_seconds{stat="mean"}

# Количество значений
ytdlp_extract_info_seconds_count

# Общее время получения субтитров (секунды)
ytdlp_captions_seconds_total{stat="min"}
ytdlp_captions_seconds_total{stat="max"}
ytdlp_captions_seconds_total{stat="mean"}

# Количество значений
ytdlp_captions_seconds_total_count

# Общее время обработки видео (секунды)
ytdlp_total_processing_seconds{stat="min"}
ytdlp_total_processing_seconds{stat="max"}
ytdlp_total_processing_seconds{stat="mean"}

# Количество значений
ytdlp_total_processing_seconds_count
```

## Примеры полезных запросов

### Средние значения

```promql
# Средняя длина заголовка
fetcher_meta_title_length{stat="mean"}

# Среднее количество просмотров
fetcher_meta_view_count{stat="mean"}

# Средний engagement rate
fetcher_meta_engagement_rate{stat="mean"}
```

### Процентили (используя функции PromQL)

```promql
# 95-й процентиль просмотров (приблизительно через max)
topk(5, fetcher_meta_view_count{stat="max"})

# Среднее значение по всем snapshot'ам
avg(fetcher_snapshot_1_view_count_delta{stat="mean"})
```

### Фильтрация по лейблам

```promql
# Все метрики для snapshot 1
{fetcher_snapshot_1_*}

# Все метрики с русским языком
{fetcher_meta_language_distribution_total{language="ru"}}

# Топ-10 каналов
topk(10, fetcher_meta_channels_top20)
```

### Агрегации

```promql
# Сумма всех просмотров
sum(fetcher_meta_view_count{stat="mean"}) * fetcher_meta_view_count_count

# Количество видео с просмотрами выше среднего
count(fetcher_meta_view_count{stat="mean"} > avg(fetcher_meta_view_count{stat="mean"}))
```

---

**Примечание:** Замените `snapshot="1"` на нужный номер snapshot'а (1, 2, 3, и т.д.) в зависимости от ваших данных.

