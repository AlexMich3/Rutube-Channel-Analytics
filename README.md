# Rutube Channel Analytics + Dashboard

Личный пет‑проект по аналитике видеоканалов на Rutube.

Python‑скрипт собирает статистику по всем видео выбранного канала, сохраняет снапшоты в PostgreSQL (Neon) и визуализирует метрики просмотров и вовлечённости в дашборде. Дашборд параметризован: достаточно поменять `channel_id`, чтобы использовать его для любого Rutube‑канала.

## 1. Business problem

Владельцам каналов и SMM‑менеджерам нужно быстро видеть динамику просмотров и подписчиков, понимать, какие видео дают максимальную вовлечённость, и подбирать оптимальное время и формат публикаций.

Стандартный интерфейс Rutube ограничен для глубокой аналитики, поэтому был собран отдельный пайплайн и дашборд.

## 2. Data pipeline (ETL)

**Extract**

- Публичные API Rutube:
  - `api/video/person/{channel_id}/` — список всех видео канала
  - `api/video/{video_hash}/` — метаданные видео (название, длительность, дата, автор, подписчики)
  - `api/numerator/video/{video_hash}/vote` — лайки и дизлайки
  - `api/v2/comments/video/{video_hash}/` — количество комментариев
- HTML‑страница видео — парсинг общего числа просмотров (`userInteractionCount`).

**Transform**

- Разбор даты публикации → `published_date`, `published_hour`, `weekday`.
- Категоризация длительности: `0–120`, `120–600`, `600–1800`, `1800+` секунд.
- Расчёт метрик:
  - `like_rate = likes / views`
  - `comment_rate = comments / views`
  - `engagement_rate = (likes + comments) / views`
  - `net_likes = likes - dislikes`
  - `likes_per_1k_views`, `comments_per_1k_views`
-  обработка ошибок API и 404.

**Load**

- Батч‑вставка данных в PostgreSQL (Neon) через `psycopg2.extras.execute_values`.
- Дополнительная выгрузка в CSV и JSON (`data/<имя_канала>/rutubedata_<имя_канала>.csv`) для офлайн‑аналитики.

## 3. Data model

Таблица `rutube_video_stats` содержит:

- `video_id`, `hash`, `url`, `channel_id`, `channel_name`
- `snapshot_ts`, `published_at`, `published_date`, `published_hour`, `weekday`
- `title`, `description`, `duration`, `duration_bucket`
- `views`, `likes`, `dislikes`, `comments_count`, `channel_subscribers`
- `like_rate`, `comment_rate`, `engagement_rate`,
  `net_likes`, `likes_per_1k_views`, `comments_per_1k_views`, `is_available`

## 4. Dashboard

Интерактивные дашборды собраны в **Yandex DataLens** и подключены к таблице `rutube_video_stats` в PostgreSQL (Neon).  
Каждый дашборд параметризован по `channel_id`, поэтому шаблон можно переиспользовать для любых Rutube‑каналов.

**Публичные дашборды:**

- [RUTUBE канал Кино-Театр.Ру](https://datalens.yandex/soz75zb2wt0sc)
- [RUTUBE канал Men Today](https://datalens.yandex/okvqgs1xq5ts8)
- [RUTUBE канал Дмитрий Никотин](https://datalens.yandex/c8iu3hfitkeww)

Основные элементы дашбордов:

- KPI‑карточки: подписчики, суммарные просмотры, лайки/комментарии на 100 просмотров, средний engagement rate.
- Распределение видео по длительности и средним просмотрам.
- Графики вовлечённости по дням недели и часам публикации.
- Таблицы самых просматриваемых, самых лайкаемых и самых комментируемых видео.


## 5. Tech stack

- Python: `requests`, `json`, `re`, `csv`, `datetime`, `time`
- PostgreSQL (Neon), `psycopg2` - Аналитические запросы для дашборда (KPI, топ‑5 видео, лайки/комментарии на 100 просмотров)
- BI: DataLens
- Git, GitHub
