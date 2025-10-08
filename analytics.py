from __future__ import annotations
import csv, time, re, os, sys, json
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ====== 設定（ここだけ編集） ======
DATE_FROM  = "2025-09-30"   # yyyy-mm-dd（内部で publishedAfter に変換）
DATE_TO    = "2025-10-06"   # yyyy-mm-dd（当日含む）
INPUT_CSV  = "vtuber_list.csv"
OUTPUT_BASENAME = "vtuber_analytics_summary.csv"  # ←常にタイムスタンプ付で保存
# =================================

JST = timezone(timedelta(hours=9))

# ---------- APIキー・クライアントのローテータ ----------
class ApiKeyRotator:
    def __init__(self, env_path="./environment.json"):
        data = json.load(open(env_path, "r"))
        keys = []
        if isinstance(data.get("vtuber_analytics_api_keys"), list):
            keys = [k for k in data["vtuber_analytics_api_keys"] if k]
        elif data.get("vtuber_analytics_api_key"):
            keys = [data["vtuber_analytics_api_key"]]
        if not keys:
            raise RuntimeError("environment.json に API キーが見つかりません")
        self.keys = keys
        self.idx = 0
        self._client = build("youtube", "v3", developerKey=self.keys[self.idx])

    def client(self):
        return self._client

    def next(self) -> bool:
        """次のキーに切り替える。もう無ければ False。"""
        if self.idx + 1 >= len(self.keys):
            return False
        self.idx += 1
        self._client = build("youtube", "v3", developerKey=self.keys[self.idx])
        print(f"🔁 APIキーを切替えました（{self.idx+1}/{len(self.keys)}）", file=sys.stderr)
        return True

rotator = ApiKeyRotator()  # ← ここで environment.json を読み込む

def is_quota_error(e: HttpError) -> bool:
    try:
        if hasattr(e, "resp") and getattr(e.resp, "status", None) == 403:
            msg = e.content.decode("utf-8", errors="ignore")
            return ("quotaExceeded" in msg) or ("dailyLimitExceeded" in msg)
    except Exception:
        pass
    return False

def with_quota_rotation(make_request):
    """
    make_request = lambda yt: yt.XXXXX().list(...).execute() を返す“直前”のリクエストオブジェクト生成ラムダ。
    クォータエラーなら次のAPIキーに切替えて自動リトライ。
    """
    while True:
        try:
            req = make_request(rotator.client())
            return req.execute()
        except HttpError as e:
            if is_quota_error(e):
                if not rotator.next():
                    raise  # キーが尽きた
                # 切替えて再試行
                continue
            raise

# ---------- ユーティリティ ----------
def to_rfc3339_utc(date_from: str, date_to: str) -> Tuple[str, str]:
    d0 = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    d1 = datetime.strptime(date_to, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    return d0.isoformat().replace("+00:00", "Z"), d1.isoformat().replace("+00:00", "Z")

def output_path_with_timestamp(base_name: str) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H-%M-%S")  # Windows互換
    root, ext = os.path.splitext(base_name)
    return f"{root}_{ts}{ext or '.csv'}"

def write_results(rows: List[Dict[str, Any]], path: str, columns: List[str]):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in columns})

def chunked(seq: List[str], n: int):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

# ---------- URL → channelId（ルール優先 & 検索は最後） ----------
_chid_cache: Dict[str, Optional[str]] = {}

def extract_from_url(url: str) -> Tuple[str, str]:
    if url.startswith("@"):
        return ("handle", url)

    u = urlparse(url)
    path = (u.path or "").strip("/")

    m = re.match(r"^channel/(UC[\w-]{20,})$", path)
    if m: return ("channel_id", m.group(1))
    if path.startswith("@"): return ("handle", path.split("/")[0])
    m = re.match(r"^user/([^/]+)$", path)
    if m: return ("username", m.group(1))
    m = re.match(r"^c/([^/]+)$", path)
    if m: return ("custom", m.group(1))

    if path:
        seg = path.split("/")[0]
        if seg.startswith("@"):
            return ("handle", seg)
        return ("custom", seg)

    q = parse_qs(u.query or "")
    if "channel" in q and q["channel"]:
        val = q["channel"][0]
        if val.startswith("UC"):
            return ("channel_id", val)

    return ("guess", url)

def resolve_channel_id(url: str) -> Optional[str]:
    if url in _chid_cache:
        return _chid_cache[url]
    kind, value = extract_from_url(url)
    chid: Optional[str] = None
    try:
        if kind == "channel_id":
            chid = value
        elif kind == "handle":
            handle = value if value.startswith("@") else f"@{value}"
            res = with_quota_rotation(lambda yt: yt.channels().list(part="id", forHandle=handle, maxResults=1))
            items = res.get("items", [])
            if items: chid = items[0]["id"]
        elif kind == "username":
            res = with_quota_rotation(lambda yt: yt.channels().list(part="id", forUsername=value, maxResults=1))
            items = res.get("items", [])
            if items: chid = items[0]["id"]
        else:
            # custom/guess → 最後の手段で search.list
            res = with_quota_rotation(lambda yt: yt.search().list(part="snippet", q=value, type="channel", maxResults=1))
            items = res.get("items", [])
            if items: chid = items[0]["snippet"]["channelId"]
    except HttpError as e:
        print(f"[WARN] resolve_channel_id HttpError: {e}", file=sys.stderr)

    _chid_cache[url] = chid
    return chid

# ---------- チャンネル基本 ----------
def get_channel_info(channel_id: str) -> Tuple[Optional[str], Optional[int]]:
    res = with_quota_rotation(lambda yt: yt.channels().list(part="snippet,statistics", id=channel_id, maxResults=1))
    items = res.get("items", [])
    if not items: return None, None
    it = items[0]
    title = it["snippet"]["title"]
    stats = it["statistics"]
    subs = None if stats.get("hiddenSubscriberCount") else int(stats.get("subscriberCount", 0))
    return title, subs

# ---------- 期間内動画 ----------
def list_video_ids_in_period(channel_id: str, after_iso: str, before_iso: str, max_pages=50) -> List[str]:
    ids: List[str] = []
    token, pages = None, 0
    while True:
        res = with_quota_rotation(lambda yt: yt.search().list(
            part="id",
            channelId=channel_id,
            type="video",
            order="date",
            publishedAfter=after_iso,
            publishedBefore=before_iso,
            maxResults=50,
            pageToken=token
        ))
        for it in res.get("items", []):
            vid = it["id"].get("videoId")
            if vid: ids.append(vid)
        token = res.get("nextPageToken")
        pages += 1
        if not token or pages >= max_pages:
            break
        time.sleep(0.12)
    return ids

def fetch_video_stats(video_ids: List[str]) -> Tuple[List[int], List[int]]:
    views, comments = [], []
    for batch in chunked(video_ids, 50):
        res = with_quota_rotation(lambda yt: yt.videos().list(part="statistics", id=",".join(batch), maxResults=50))
        for v in res.get("items", []):
            st = v.get("statistics", {})
            if "viewCount" in st:
                try: views.append(int(st["viewCount"]))
                except: pass
            if "commentCount" in st:
                try: comments.append(int(st["commentCount"]))
                except: pass
        time.sleep(0.12)
    return views, comments

def mean_or_zero(nums: List[int]) -> float:
    return (sum(nums)/len(nums)) if nums else 0.0

# ---------- メン限（UUMO） ----------
def members_only_playlist_id(channel_id: str, category: str = "all") -> str:
    core = channel_id[2:]
    prefix = {"all":"UUMO","videos":"UUMF","shorts":"UUMS","live":"UUMV"}.get(category, "UUMO")
    return prefix + core

def fetch_playlist_items(playlist_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    token = None
    while True:
        res = with_quota_rotation(lambda yt: yt.playlistItems().list(
            part="snippet,contentDetails,status",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=token
        ))
        items.extend(res.get("items", []))
        token = res.get("nextPageToken")
        if not token: break
        time.sleep(0.12)
    return items

def fetch_videos_details(video_ids: List[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for batch in chunked(video_ids, 50):
        # リトライは with_quota_rotation がやるのでここでは不要
        res = with_quota_rotation(lambda yt: yt.videos().list(
            part="snippet,statistics,status,contentDetails",
            id=",".join(batch),
            maxResults=50
        ))
        out.extend(res.get("items", []))
        time.sleep(0.12)
    return out

def now_jst() -> datetime:
    return datetime.now(JST)

def within_months(pub_jst: datetime, now: datetime, months: int) -> bool:
    start = now - timedelta(days=30*months)
    return (start <= pub_jst <= now)

def parse_published_at_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def safe_int(d: Dict[str, Any], key: str) -> Optional[int]:
    if key in d:
        try: return int(d[key])
        except: return None
    return None

def members_averages(channel_id: str) -> Tuple[int, float, float, int, float, float]:
    """
    (count_1m, avg_like_1m, avg_comment_1m,
     count_2m, avg_like_2m, avg_comment_2m)
    """
    try:
        plid = members_only_playlist_id(channel_id, "all")
        pl_items = fetch_playlist_items(plid)
        if not pl_items:
            return 0, 0.0, 0.0, 0, 0.0, 0.0

        seen, video_ids = set(), []
        for it in pl_items:
            vid = (it.get("contentDetails") or {}).get("videoId")
            if vid and vid not in seen:
                seen.add(vid); video_ids.append(vid)

        videos = fetch_videos_details(video_ids)
        now = now_jst()

        count_1m = count_2m = 0
        likes_1m, comms_1m = [], []
        likes_2m, comms_2m = [], []

        for v in videos:
            sn = v.get("snippet") or {}
            st = v.get("statistics") or {}
            pub = sn.get("publishedAt")
            if not pub: continue
            pub_jst = parse_published_at_utc(pub).astimezone(JST)
            in_1m = within_months(pub_jst, now, 1)
            in_2m = within_months(pub_jst, now, 2)

            if in_1m: count_1m += 1
            if in_2m: count_2m += 1

            like = safe_int(st, "likeCount")
            com  = safe_int(st, "commentCount")
            if in_1m:
                if like is not None: likes_1m.append(like)
                if com  is not None: comms_1m.append(com)
            if in_2m:
                if like is not None: likes_2m.append(like)
                if com  is not None: comms_2m.append(com)

        return (
            count_1m, mean_or_zero(likes_1m),  mean_or_zero(comms_1m),
            count_2m, mean_or_zero(likes_2m),  mean_or_zero(comms_2m),
        )
    except HttpError as e:
        print(f"[WARN] members_averages HttpError: {e}", file=sys.stderr)
        return 0,0.0,0.0, 0,0.0,0.0
    except Exception as e:
        print(f"[WARN] members_averages error: {e}", file=sys.stderr)
        return 0,0.0,0.0, 0,0.0,0.0

# ---------- メイン ----------
def main():
    after_iso, before_iso = to_rfc3339_utc(DATE_FROM, DATE_TO)
    output_path = output_path_with_timestamp(OUTPUT_BASENAME)

    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        urls = [row["channel_url"].strip() for row in reader if row.get("channel_url")]

    # CSV/標準出力に載せる列（= 完全一致）
    COLUMNS = [
        "channel_url",
        "channelTitle",
        "subscriberCount",
        "uploads",              # 指定期間の公開本数
        "avgViewCount",         # 指定期間の平均視聴回数
        "avgCommentCount",      # 指定期間の平均コメント数
        "membersCount_1m",      # メン限1ヶ月の本数
        "membersAvgLike_1m",
        "membersAvgComment_1m",
        "membersCount_2m",      # メン限2ヶ月の本数
        "membersAvgLike_2m",
        "membersAvgComment_2m",
    ]

    results: List[Dict[str, Any]] = []

    for url in urls:
        print(f"▶ チャンネルURL: {url}")
        try:
            chid = resolve_channel_id(url)
            if not chid:
                print("  ❌ チャンネル特定失敗")
                continue

            title, subs = get_channel_info(chid)
            if not title:
                print("  ❌ 情報取得失敗")
                continue

            # 指定期間の公開動画一覧→視聴/コメント集計
            vids = list_video_ids_in_period(chid, after_iso, before_iso)
            uploads = len(vids)
            views, comments = fetch_video_stats(vids)
            avg_views = mean_or_zero(views)
            avg_comments = mean_or_zero(comments)

            # メン限：1/2ヶ月（本数→平均）
            (count_1m, avg_like_1m, avg_comm_1m,
             count_2m, avg_like_2m, avg_comm_2m) = members_averages(chid)

            row = {
                "channel_url": url,
                "channelTitle": title,
                "subscriberCount": subs if subs is not None else "",
                "uploads": uploads,
                "avgViewCount": f"{avg_views:.2f}",
                "avgCommentCount": f"{avg_comments:.2f}",
                "membersCount_1m": count_1m,
                "membersAvgLike_1m": f"{avg_like_1m:.2f}",
                "membersAvgComment_1m": f"{avg_comm_1m:.2f}",
                "membersCount_2m": count_2m,
                "membersAvgLike_2m": f"{avg_like_2m:.2f}",
                "membersAvgComment_2m": f"{avg_comm_2m:.2f}",
            }
            results.append(row)

            # —— 標準出力は CSV と同じ情報を日本語で（順序も揃える）——
            print(f"  ✅ チャンネル名: {row['channelTitle']}")
            print(f"     登録者数: {row['subscriberCount'] if row['subscriberCount'] != '' else '(非公開)'}")
            print(f"     期間内アップ本数: {row['uploads']}")
            print(f"     期間内平均視聴回数: {row['avgViewCount']} / 期間内平均コメント数: {row['avgCommentCount']}")
            print(f"     [メン限] 1ヶ月: 本数{row['membersCount_1m']} / いいね{row['membersAvgLike_1m']} / コメント{row['membersAvgComment_1m']}")
            print(f"     [メン限] 2ヶ月: 本数{row['membersCount_2m']} / いいね{row['membersAvgLike_2m']} / コメント{row['membersAvgComment_2m']}")
            print()

            # 障害時に備えて逐次保存（内容は上書き、ファイル名は固定のタイムスタンプ付き）
            write_results(results, output_path, COLUMNS)

        except HttpError as e:
            # with_quota_rotation 内でキー切替→尽きたらここに落ちてくる
            if is_quota_error(e):
                print("⛽ 全APIキーのクォータが尽きました。ここまでの結果を書き出して終了します。")
                write_results(results, output_path, COLUMNS)
                return
            else:
                print(f"  ❌ HttpError: {e}")
                continue
        except Exception as e:
            print(f"  ❌ 予期せぬエラー: {e}")
            continue

    # 最終保存
    write_results(results, output_path, COLUMNS)
    print(f"📄 出力: {output_path}（{len(results)}件）")

if __name__ == "__main__":
    main()
