# 同階層:
#   - environment.json（API キー格納）
#   - vtuber_list.csv（カラム: channel_url）
# 出力:
#   - vtuber_analytics_summary.csv
#
# 使い方: そのまま実行（引数なし）。スクリプト冒頭の定数を編集して使う。

import csv, time, re, json
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build

api_key_json = json.load(open("./environment.json", "r"))
API_KEY = api_key_json["vtuber_analytics_api_key"]

# ====== ここを編集 ======
DATE_FROM = "2025-09-30"           # 期間開始 (yyyy-mm-dd)
DATE_TO   = "2025-10-06"           # 期間終了 (yyyy-mm-dd, 当日を含む)
INPUT_CSV = "vtuber_list.csv"
OUTPUT_CSV = "vtuber_analytics_summary.csv"
# ========================

YOUTUBE = build("youtube", "v3", developerKey=API_KEY)

def to_rfc3339_utc(date_str_from: str, date_str_to: str):
    """
    'yyyy-mm-dd' から RFC3339 UTC 文字列 (publishedAfter, publishedBefore) を作成。
    終了日は「翌日00:00:00Z」を before として半開区間にすることで当日を含める。
    """
    d0 = datetime.strptime(date_str_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d1 = datetime.strptime(date_str_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    return d0.isoformat().replace("+00:00", "Z"), d1.isoformat().replace("+00:00", "Z")

# ---------- URL解析 ----------
def extract_from_url(url):
    if url.startswith("@"):
        return ("handle", url)
    u = urlparse(url)
    path = (u.path or "").strip("/")

    m = re.match(r"channel/(UC[\w-]{20,})", path)
    if m:
        return ("channel_id", m.group(1))
    if path.startswith("@"):
        return ("handle", path.split("/")[0])
    m = re.match(r"user/([^/]+)", path)
    if m:
        return ("username", m.group(1))
    m = re.match(r"c/([^/]+)", path)
    if m:
        return ("custom", m.group(1))
    return ("custom", path.split("/")[0] if path else url)

def resolve_channel_id(kind, value):
    if kind == "channel_id":
        return value
    if kind == "handle":
        handle = value if value.startswith("@") else f"@{value}"
        res = YOUTUBE.channels().list(part="id", forHandle=handle).execute()
        items = res.get("items", [])
        return items[0]["id"] if items else None
    if kind == "username":
        res = YOUTUBE.channels().list(part="id", forUsername=value).execute()
        items = res.get("items", [])
        return items[0]["id"] if items else None
    # custom や曖昧な文字列は検索で解決
    res = YOUTUBE.search().list(part="snippet", q=value, type="channel", maxResults=1).execute()
    items = res.get("items", [])
    return items[0]["snippet"]["channelId"] if items else None

# ---------- チャンネル基本情報 ----------
def get_channel_info(channel_id):
    res = YOUTUBE.channels().list(part="snippet,statistics", id=channel_id, maxResults=1).execute()
    items = res.get("items", [])
    if not items:
        return None, None
    it = items[0]
    title = it["snippet"]["title"]
    stats = it["statistics"]
    subs = None if stats.get("hiddenSubscriberCount") else int(stats.get("subscriberCount", 0))
    return title, subs

# ---------- 期間内の動画IDを列挙 ----------
def list_video_ids_in_period(channel_id, published_after_iso, published_before_iso, max_pages=50):
    """
    期間で絞って動画IDを収集。max_pagesは保険（多すぎる際の上限）。
    """
    ids = []
    page_token = None
    pages = 0
    while True:
        req = YOUTUBE.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            order="date",
            publishedAfter=published_after_iso,
            publishedBefore=published_before_iso,
            maxResults=50,
            pageToken=page_token
        )
        res = req.execute()
        for it in res.get("items", []):
            vid = it["id"].get("videoId")
            if vid:
                ids.append(vid)
        page_token = res.get("nextPageToken")
        pages += 1
        if not page_token or pages >= max_pages:
            break
        time.sleep(0.15)  # 叩きすぎ防止
    return ids

# ---------- 視聴回数・コメント数をまとめて取得 ----------
def fetch_views_comments(video_ids):
    """
    returns: (view_counts:list[int], comment_counts:list[int])
    コメント無効は除外（commentCount欠落）。
    """
    views, comments = [], []
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        res = YOUTUBE.videos().list(part="statistics", id=",".join(chunk), maxResults=50).execute()
        for v in res.get("items", []):
            st = v.get("statistics", {})
            if "viewCount" in st:
                try:
                    views.append(int(st["viewCount"]))
                except ValueError:
                    pass
            if "commentCount" in st:
                try:
                    comments.append(int(st["commentCount"]))
                except ValueError:
                    pass
        time.sleep(0.15)
    return views, comments

def mean_or_zero(nums):
    return (sum(nums) / len(nums)) if nums else 0.0

def main():
    # 期間をRFC3339 (UTC) に変換
    after_iso, before_iso = to_rfc3339_utc(DATE_FROM, DATE_TO)

    # 入力URL一覧
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        urls = [row["channel_url"].strip() for row in reader if row.get("channel_url")]

    results = []
    for url in urls:
        print(f"▶ {url}")
        kind, val = extract_from_url(url)
        channel_id = resolve_channel_id(kind, val)
        if not channel_id:
            print("  ❌ チャンネル特定失敗")
            continue

        title, subs = get_channel_info(channel_id)
        if not title:
            print("  ❌ 情報取得失敗")
            continue

        # 期間内の動画を取得
        video_ids = list_video_ids_in_period(channel_id, after_iso, before_iso)
        uploads = len(video_ids)

        # 視聴回数・コメント数を集計
        views, comments = fetch_views_comments(video_ids)
        avg_views = mean_or_zero(views)
        avg_comments = mean_or_zero(comments)

        subs_text = "(非公開)" if subs is None else f"{subs:,}"
        print(f"  ✅ {title}")
        print(f"     登録者数: {subs_text}")
        print(f"     アップロード本数: {uploads}")
        print(f"     平均視聴回数: {avg_views:.2f}（対象{len(views)}本）")
        print(f"     平均コメント数: {avg_comments:.2f}（対象{len(comments)}本）")

        # ==== 出力CSVは標準出力と同じ情報だけ ====
        results.append({
            "channel_url": url,
            "channelTitle": title,
            "subscriberCount": subs if subs is not None else "",
            "uploads": uploads,
            "avgViewCount": f"{avg_views:.2f}",
            "avgCommentCount": f"{avg_comments:.2f}",
        })

    if results:
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=results[0].keys())
            w.writeheader()
            w.writerows(results)
        print(f"\n📄 {OUTPUT_CSV} に {len(results)}件を書き出しました")
    else:
        print("❌ 出力データなし")

if __name__ == "__main__":
    main()
