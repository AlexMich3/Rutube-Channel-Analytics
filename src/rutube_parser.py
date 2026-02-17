import requests
import json
import re
import os
import csv
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values


# ==== НАСТРОЙКИ ДЛЯ КАНАЛА ====
CHANNEL_ID = 8420540  # здесь подставляешь id нужного канала


# ==== НАСТРОЙКИ NEON POSTGRES ====
NEON_DSN = os.getenv("NEON_DSN")

# --- вспомогательное: получить HTML страницы ---
def get_html(url: str) -> str:
    resp = requests.get(url, timeout=5)
    resp.raise_for_status()
    return resp.text


# --- просмотры из HTML (userInteractionCount) ---
def get_views_from_html(html: str):
    m = re.search(r'"userInteractionCount"\s*:\s*"([0-9]+)"', html)
    if not m:
        return None
    return int(m.group(1))


# --- маленькие вспомогательные функции для метрик ---
def bucket_duration(seconds):
    if seconds is None:
        return "unknown"
    if seconds < 120:
        return "0-120"
    if seconds < 600:
        return "120-600"
    if seconds < 1800:
        return "600-1800"
    return "1800+"


def safe_div(num, den):
    if not den or den <= 0:
        return 0.0
    if num is None:
        return 0.0
    return float(num) / float(den)


# --- сохранение батчем в Postgres (Neon) ---
def save_to_postgres(all_stats: list[dict]):
    if not all_stats:
        print("Postgres: all_stats is empty, nothing to insert")
        return

    rows = []
    for r in all_stats:
        rows.append(
            (
                r["snapshot_ts"],
                r["url"],
                r["hash"],
                r["video_id"],
                r["channel_id"],
                r["channel_name"],
                r["channel_subscribers"],
                r["title"],
                r["description"],
                r["published_at"],
                r["published_date"],
                r["published_hour"],
                r["weekday"],
                r["duration"],
                r["duration_bucket"],
                r["views"],
                r["likes"],
                r["dislikes"],
                r["comments_count"],
                r["like_rate"],
                r["comment_rate"],
                r["engagement_rate"],
                r["net_likes"],
                r["likes_per_1k_views"],
                r["comments_per_1k_views"],
                r["tags"],
                r["category"],
                bool(r["is_available"]),
            )
        )

    insert_sql = """
        INSERT INTO rutube_video_stats (
            snapshot_ts,
            url,
            hash,
            video_id,
            channel_id,
            channel_name,
            channel_subscribers,
            title,
            description,
            published_at,
            published_date,
            published_hour,
            weekday,
            duration,
            duration_bucket,
            views,
            likes,
            dislikes,
            comments_count,
            like_rate,
            comment_rate,
            engagement_rate,
            net_likes,
            likes_per_1k_views,
            comments_per_1k_views,
            tags,
            category,
            is_available
        ) VALUES %s
    """

    with psycopg2.connect(NEON_DSN) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)
        conn.commit()

    print(f"Inserted {len(all_stats)} rows into rutube_video_stats")


# --- базовые данные видео + дата публикации + инфо о канале ---
def get_video_core_stats(video_hash: str) -> dict:
    url = f"https://rutube.ru/api/video/{video_hash}/"
    resp = requests.get(url, timeout=3)
    resp.raise_for_status()
    data = resp.json()

    return {
        "title": data.get("title"),
        "description": data.get("description") or "",
        "duration": data.get("duration"),             # секунды
        "published_at": data.get("publication_ts"),   # "2026-02-01T13:54:18"
        "channel_id": data.get("author", {}).get("id"),
        "channel_name": data.get("author", {}).get("name"),
        "channel_subscribers": data.get("feed_subscribers_count"),
    }


# --- лайки / дизлайки ---
def get_video_votes(video_hash: str) -> dict:
    url = f"https://rutube.ru/api/numerator/video/{video_hash}/vote"
    params = {"client": "wdp"}
    resp = requests.get(url, params=params, timeout=3)
    resp.raise_for_status()
    data = resp.json()
    return {
        "likes": data.get("positive"),
        "dislikes": data.get("negative"),
    }


# --- количество комментариев с обработкой 404/ошибок ---
def get_comments_count(video_hash: str) -> int | None:
    url = f"https://rutube.ru/api/v2/comments/video/{video_hash}/"
    params = {"client": "wdp", "sort_by": "date_added_desc"}

    try:
        resp = requests.get(url, params=params, timeout=3)

        # Если у видео нет/закрыт эндпоинт комментариев — не падаем
        if resp.status_code == 404:
            print(f"WARNING: comments endpoint 404 for {video_hash}, set comments_count=None")
            return None

        resp.raise_for_status()

    except requests.RequestException as e:
        print(f"ERROR: failed to get comments for {video_hash}: {e}")
        return None

    data = resp.json()
    return data.get("comments_count")


# --- ОБХОД ВСЕХ СТРАНИЦ СПИСКА ВИДЕО КАНАЛА ---
def get_all_channel_videos(channel_id: int, per_page: int = 50) -> list[dict]:
    """
    Обходит все страницы api/video/person/{channel_id}/
    и возвращает список всех видео канала.
    Формат элемента:
      {"url": "...", "hash": "...", "title": "...", "published_at": "...", "duration": ...}
    """
    base_url = f"https://rutube.ru/api/video/person/{channel_id}/"
    params = {
        "limit": per_page,
        "offset": 0,
    }

    videos: list[dict] = []
    url = base_url

    while True:
        resp = requests.get(url, params=params if url == base_url else None, timeout=3)
        resp.raise_for_status()
        data = resp.json()  # содержит has_next, next, results и т.п.

        for item in data.get("results", []):
            vid = {
                "url": item.get("video_url"),
                "hash": item.get("id"),
                "title": item.get("title"),
                "published_at": item.get("publication_ts"),
                "duration": item.get("duration"),
            }
            videos.append(vid)

        next_url = data.get("next")
        if not next_url:
            break

        url = next_url
        params = None  # в next уже всё зашито

    return videos


# --- объединяем всё по одному видео и считаем метрики ---
def get_video_stats(video_url: str, video_hash: str) -> dict:
    html = get_html(video_url)
    views = get_views_from_html(html) or 0

    core = get_video_core_stats(video_hash)
    votes = get_video_votes(video_hash)
    comments_count = get_comments_count(video_hash)

    likes = votes.get("likes") or 0
    dislikes = votes.get("dislikes") or 0
    comments = comments_count or 0

    # snapshot времени
    now = datetime.now(timezone.utc)
    snapshot_ts = now.isoformat(timespec="seconds")

    # разбор даты публикации
    published_raw = core["published_at"]
    published_dt = None
    if published_raw:
        try:
            published_dt = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
        except Exception:
            published_dt = None

    if published_dt:
        published_date = published_dt.date().isoformat()
        published_hour = published_dt.hour
        weekday = published_dt.weekday()  # 0=Monday
    else:
        published_date = "1970-01-01"
        published_hour = 0
        weekday = 0

    duration = core["duration"]
    duration_bucket = bucket_duration(duration)

    like_rate = safe_div(likes, views)
    comment_rate = safe_div(comments, views)
    engagement_rate = safe_div(likes + comments, views)
    net_likes = likes - dislikes
    likes_per_1k_views = safe_div(likes * 1000, views)
    comments_per_1k_views = safe_div(comments * 1000, views)

    result = {
        "snapshot_ts": snapshot_ts,
        "url": video_url,
        "hash": video_hash,

        "video_id": video_hash,
        "channel_id": core["channel_id"],
        "channel_name": core["channel_name"],
        "channel_subscribers": core["channel_subscribers"],

        "title": core["title"],
        "description": core["description"],
        "published_at": core["published_at"],
        "published_date": published_date,
        "published_hour": published_hour,
        "weekday": weekday,

        "duration": duration,
        "duration_bucket": duration_bucket,

        "views": views,
        "likes": likes,
        "dislikes": dislikes,
        "comments_count": comments_count,

        "like_rate": like_rate,
        "comment_rate": comment_rate,
        "engagement_rate": engagement_rate,
        "net_likes": net_likes,
        "likes_per_1k_views": likes_per_1k_views,
        "comments_per_1k_views": comments_per_1k_views,

        "tags": "",
        "category": "",
        "is_available": 1,
    }
    return result


# --- сбор статистики по списку видео с паузой и прогрессом ---
def collect_stats_for_videos(videos: list[dict], delay_seconds: int = 5) -> list[dict]:
    results = []
    total = len(videos)
    for i, v in enumerate(videos, start=1):
        video_url = v["url"]
        video_hash = v["hash"]
        print(f"[{i}/{total}] collecting stats for {video_hash} ...")

        try:
            stats = get_video_stats(video_url, video_hash)
            results.append(stats)
        except Exception as e:
            print(f"ERROR: failed to collect stats for {video_hash}: {e}")

        if i < total and delay_seconds > 0:
            time.sleep(delay_seconds)

    return results


# --- утилита для безопасного имени папки/файла ---
def make_safe(name: str) -> str:
    return (
        name.strip()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
        .replace("?", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("|", "_")
    )


if __name__ == "__main__":
    # 1) Тянем ВСЕ видео канала
    print("Fetching channel videos list...")
    channel_videos = get_all_channel_videos(CHANNEL_ID, per_page=50)
    print(f"Found {len(channel_videos)} videos")

    # 2) Собираем подробную статистику по каждому видео
    all_stats = collect_stats_for_videos(channel_videos, delay_seconds=5)

    if not all_stats:
        print("No stats collected, nothing to save")
        raise SystemExit(0)

    # 2.5) Сохраняем в Postgres (Neon)
    try:
        save_to_postgres(all_stats)
    except Exception as e:
        print(f"Failed to insert into Postgres: {e}")

    # 3) Имя канала из данных
    channel_name = all_stats[0].get("channel_name") or f"channel_{CHANNEL_ID}"
    safe_channel = make_safe(channel_name)
    base_filename = f"rutubedata_{safe_channel}"

    # 4) Папка data/<имя_канала>
    root_dir = "data"
    out_dir = os.path.join(root_dir, safe_channel)
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, f"{base_filename}.csv")
    json_path = os.path.join(out_dir, f"{base_filename}.json")

    fieldnames = [
        "snapshot_ts",
        "url",
        "hash",
        "video_id",
        "channel_id",
        "channel_name",
        "channel_subscribers",
        "title",
        "description",
        "published_at",
        "published_date",
        "published_hour",
        "weekday",
        "duration",
        "duration_bucket",
        "views",
        "likes",
        "dislikes",
        "comments_count",
        "like_rate",
        "comment_rate",
        "engagement_rate",
        "net_likes",
        "likes_per_1k_views",
        "comments_per_1k_views",
        "tags",
        "category",
        "is_available",
    ]

    with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in all_stats:
            writer.writerow(row)

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(all_stats)} rows to {csv_path}")
