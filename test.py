from __future__ import annotations
from typing import List, Dict, Any, Optional, Iterable
from dataclasses import dataclass
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime, timedelta, timezone
import csv
import sys
import time

# ========= 設定 =========
API_KEY    = "AIzaSyB4BBeseY59wo6k5ob02incoKc5ZSTFdAc"
CHANNEL_ID = "UCMPyGnBgm6l0KiLoxPI8x3A"
CATEGORY   = "all"  # "all"(UUMO) | "videos"(UUMF) | "shorts"(UUMS) | "live"(UUMV)
CSV_PATH   = "members_only_averages.csv"  # 1 行の CSV を出力

# 日本時間（固定）
JST = timezone(timedelta(hours=9))

# ========= ユーティリティ =========
def members_only_playlist_id(channel_id: str, category: str = "all") -> str:
    if not channel_id.startswith("UC"):
        raise ValueError("channel_id は 'UC' で始まる必要があります")
    core = channel_id[2:]
    prefix = {"all": "UUMO", "videos": "UUMF", "shorts": "UUMS", "live": "UUMV"}.get(category, "UUMO")
    return prefix + core

def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def build_youtube(api_key: str):
    return build("youtube", "v3", developerKey=api_key)

# ========= API 呼び出し =========
def fetch_playlist_items(youtube, playlist_id: str, sleep_sec: float = 0.1) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    token = None
    while True:
        req = youtube.playlistItems().list(
            part="snippet,contentDetails,status",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=token,
        )
        res = req.execute()
        items.extend(res.get("items", []))
        token = res.get("nextPageToken")
        if not token:
            break
        time.sleep(sleep_sec)
    return items

def fetch_videos(youtube, video_ids: List[str], sleep_sec: float = 0.1) -> List[Dict[str, Any]]:
    all_items: List[Dict[str, Any]] = []
    for batch in chunked(video_ids, 50):
        retries = 0
        while True:
            try:
                req = youtube.videos().list(
                    part="snippet,statistics,status,contentDetails",
                    id=",".join(batch),
                    maxResults=50
                )
                res = req.execute()
                all_items.extend(res.get("items", []))
                break
            except HttpError as e:
                retries += 1
                if retries > 3:
                    print(f"[WARN] videos.list 失敗（最終）: {e}", file=sys.stderr)
                    break
                wait = (2**retries) * 0.5
                print(f"[WARN] 再試行 {retries} 回目: {wait:.1f}s 待機", file=sys.stderr)
                time.sleep(wait)
        time.sleep(sleep_sec)
    return all_items

# ========= 日付・平均計算 =========
def parse_published_at_utc(s: str) -> datetime:
    # YouTube は基本 UTC の ISO8601 (例: "2025-07-01T12:34:56Z")
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def jst_now_truncated_to_day() -> datetime:
    # 「本日（日本時間）以前」の“以前”の基準として、JST のその日の 23:59:59 ではなく
    # 「今日の 00:00:00」を境に“以前”を定義するなら、ここを 00:00:00 に切る。
    # 今回は「本日以前 = 今日を含む」解釈で、現在時刻で OK（厳密に 00:00 にしたければ下記を使う）
    # today = datetime.now(JST).replace(hour=0, minute=0, second=0, microsecond=0)
    return datetime.now(JST)

def within_period(published_jst: datetime, jst_now: datetime, months: int) -> bool:
    # おおまかに「months ヶ月前」= 30日×months として扱う（厳密な暦月が必要なら dateutil を使う）
    delta_days = 30 * months
    start = jst_now - timedelta(days=delta_days)
    return (published_jst <= jst_now) and (published_jst >= start)

def safe_int(d: Dict[str, Any], key: str) -> Optional[int]:
    if key in d:
        try:
            return int(d[key])
        except Exception:
            return None
    return None

def mean(values: List[int]) -> float:
    return sum(values) / len(values) if values else 0.0

# ========= メイン =========
def main():
    yt = build_youtube(API_KEY)
    plid = members_only_playlist_id(CHANNEL_ID, CATEGORY)
    print(f"[INFO] Members-only playlistId: {plid}", file=sys.stderr)

    # 1) メン限プレイリスト → videoId 収集
    pl_items = fetch_playlist_items(yt, plid)
    if not pl_items:
        print("メンバー限定プレイリストにアイテムが見つかりません。")
        return

    seen = set()
    video_ids: List[str] = []
    for it in pl_items:
        vid = it.get("contentDetails", {}).get("videoId")
        if vid and vid not in seen:
            seen.add(vid)
            video_ids.append(vid)

    print(f"[INFO] 取得 videoId: {len(video_ids)}", file=sys.stderr)

    # 2) videos.list で詳細取得
    videos = fetch_videos(yt, video_ids)

    # 3) 日本時間での現在
    now_jst = jst_now_truncated_to_day()

    # 4) 期間別で絞り込み（公開日ベース）
    likes_1m: List[int] = []
    comments_1m: List[int] = []
    likes_2m: List[int] = []
    comments_2m: List[int] = []

    for v in videos:
        sn = v.get("snippet", {}) or {}
        st = v.get("statistics", {}) or {}
        published_at = sn.get("publishedAt")
        if not published_at:
            continue

        # UTC → JST
        pub_utc = parse_published_at_utc(published_at)
        pub_jst = pub_utc.astimezone(JST)

        like = safe_int(st, "likeCount")
        comm = safe_int(st, "commentCount")

        # 直近 1 ヶ月
        if within_period(pub_jst, now_jst, 1):
            if like is not None: likes_1m.append(like)
            if comm is not None: comments_1m.append(comm)

        # 直近 2 ヶ月
        if within_period(pub_jst, now_jst, 2):
            if like is not None: likes_2m.append(like)
            if comm is not None: comments_2m.append(comm)

    avg_like_1m = mean(likes_1m)
    avg_comm_1m = mean(comments_1m)
    avg_like_2m = mean(likes_2m)
    avg_comm_2m = mean(comments_2m)

    # 参考ログ
    print(f"[INFO] 1ヶ月: 件数 like={len(likes_1m)} / comment={len(comments_1m)}", file=sys.stderr)
    print(f"[INFO] 2ヶ月: 件数 like={len(likes_2m)} / comment={len(comments_2m)}", file=sys.stderr)

    # 5) CSV を 1 行で保存
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["avg_like_1m", "avg_comment_1m", "avg_like_2m", "avg_comment_2m"])
        w.writerow([f"{avg_like_1m:.2f}", f"{avg_comm_1m:.2f}", f"{avg_like_2m:.2f}", f"{avg_comm_2m:.2f}"])

    print(f"[INFO] CSV 保存: {CSV_PATH}", file=sys.stderr)

if __name__ == "__main__":
    main()
