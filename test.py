from __future__ import annotations
from typing import List, Dict, Any, Optional, Iterable
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import csv, json
import time
import sys
import math
from dataclasses import dataclass

API_KEY = "AIzaSyB4BBeseY59wo6k5ob02incoKc5ZSTFdAc"  # ここにあなたの API キー
CHANNEL_ID = "UCMPyGnBgm6l0KiLoxPI8x3A"  # 取得対象チャンネル
MAX_PAGES = 50        # search.list の最大ページ数
SEARCH_SLEEP = 0.15   # search.list の呼び出し間隔（秒）
VIDEOS_SLEEP = 0.1    # videos.list の呼び出し間隔（秒）
CSV_PATH: Optional[str] = "members_only_videos.csv"  # None にすると保存しない

# ------------------------------
# モデル
# ------------------------------
@dataclass
class VideoRow:
    video_id: str
    title: str
    published_at: str
    privacy_status: str
    has_view_count: bool
    url: str

# ------------------------------
# 基本ユーティリティ
# ------------------------------
def chunked(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def build_youtube(api_key: str):
    return build("youtube", "v3", developerKey=api_key)

# ------------------------------
# 収集: search.list（公開動画が返る想定）
# ------------------------------
def fetch_channel_video_ids(youtube, channel_id: str, max_pages: int = 50, sleep_sec: float = 0.15) -> List[str]:
    """
    チャンネルの動画IDを新着順に最大 max_pages ページ分取得。
    search.list は基本「公開動画」を返します。
    """
    ids: List[str] = []
    seen = set()
    page_token: Optional[str] = None
    pages = 0

    while True:
        req = youtube.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            order="date",
            maxResults=50,
            pageToken=page_token
        )
        res = req.execute()
        for it in res.get("items", []):
            vid = it["id"].get("videoId")
            if vid and vid not in seen:
                ids.append(vid)
                seen.add(vid)

        page_token = res.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break

        time.sleep(sleep_sec)

    return ids

# ------------------------------
# 判定: メンバー限定（あなたの基準をそのまま関数化）
# ------------------------------
def is_members_only_by_heuristic(video_item: Dict[str, Any]) -> bool:
    """
    前提：
      - status.privacyStatus が "public"
      - statistics.viewCount が「存在しない」
    を満たすものをメンバー限定と判定する。
    """
    status = video_item.get("status", {})
    stats = video_item.get("statistics", {})
    privacy = status.get("privacyStatus")
    has_view = "viewCount" in stats
    return (privacy == "public") and (not has_view)

# ------------------------------
# 収集: videos.list
# ------------------------------
def fetch_video_items(
    youtube,
    video_ids: List[str],
    sleep_sec: float = 0.1,
    max_retries: int = 3
) -> List[Dict[str, Any]]:
    """
    video_ids を 50件ずつ videos.list に投げて詳細を集める。
    """
    all_items: List[Dict[str, Any]] = []

    for batch in chunked(video_ids, 50):
        retries = 0
        while True:
            try:
                req = youtube.videos().list(
                    part="snippet,status,statistics",
                    id=",".join(batch),
                    maxResults=50
                )
                res = req.execute()
                all_items.extend(res.get("items", []))
                break
            except HttpError as e:
                retries += 1
                if retries > max_retries:
                    print(f"[WARN] videos.list 失敗（最終）：{e}", file=sys.stderr)
                    break
                # 簡易バックオフ
                sleep_for = (2 ** retries) * 0.5
                print(f"[WARN] videos.list エラー。{sleep_for:.1f}s 後に再試行: {e}", file=sys.stderr)
                time.sleep(sleep_for)

        time.sleep(sleep_sec)

    return all_items

# ------------------------------
# 整形
# ------------------------------
def to_video_row(item: Dict[str, Any]) -> VideoRow:
    vid = item.get("id", "")
    snip = item.get("snippet", {}) or {}
    status = item.get("status", {}) or {}
    stats = item.get("statistics", {}) or {}
    title = snip.get("title", "")
    published_at = snip.get("publishedAt", "")
    privacy = status.get("privacyStatus", "")
    has_view = "viewCount" in stats
    url = f"https://www.youtube.com/watch?v={vid}"
    return VideoRow(
        video_id=vid,
        title=title,
        published_at=published_at,
        privacy_status=privacy,
        has_view_count=has_view,
        url=url
    )

# ------------------------------
# CSV 保存（任意）
# ------------------------------
def save_as_csv(rows: List[VideoRow], path: str) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["video_id", "title", "published_at", "privacy_status", "has_view_count", "url"])
        for r in rows:
            w.writerow([r.video_id, r.title, r.published_at, r.privacy_status, r.has_view_count, r.url])

# ------------------------------
# メイン
# ------------------------------
def main():
    yt = build_youtube(API_KEY)

    print("[INFO] チャンネルの動画IDを収集中...", file=sys.stderr)
    ids = fetch_channel_video_ids(yt, CHANNEL_ID, max_pages=MAX_PAGES, sleep_sec=SEARCH_SLEEP)
    print(f"[INFO] 取得ID数: {len(ids)}", file=sys.stderr)

    if not ids:
        print("[INFO] 動画が見つかりませんでした。")
        return

    print("[INFO] 動画詳細を取得中...", file=sys.stderr)
    items = fetch_video_items(yt, ids, sleep_sec=VIDEOS_SLEEP)

    with open('test.json', 'w') as f:
        json.dump(items, f, indent=2)

    # 判定（あなたの基準）
    members_items = [it for it in items if is_members_only_by_heuristic(it)]
    rows = [to_video_row(it) for it in members_items]

    # 出力（標準出力）
    if rows:
        print("=== Members-only（仮）一覧 ===")
        for r in rows:
            print(f"{r.video_id}\t{r.published_at}\t{r.title}\t{r.url}")
    else:
        print("メンバー限定（仮判定）に該当する動画は見つかりませんでした。")

    # CSV（任意）
    if CSV_PATH:
        save_as_csv(rows, CSV_PATH)
        print(f"[INFO] CSV に保存しました -> {CSV_PATH}", file=sys.stderr)

if __name__ == "__main__":
    main()
