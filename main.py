import os
import json
import re
import requests
from datetime import datetime, timezone

from notion_client import Client


# DISCORD_API = os.environ["DISCORD_API"]
# TOKEN = os.environ["DISCORD_TOKEN"]
# GUILD = os.environ["TARGET_GUILD_ID"]
# EMOJIS = set(e.strip() for e in os.environ["TARGET_EMOJIS"].split(",") if e.strip())
# STATE_PATH = os.environ["STATE_PATH"]
CHANNEL_NAME_CACHE = {}  # チャンネル名のキャッシュ


def extract_first_url(text: str) -> str:
    """本文中の最初のURLを返す。なければ空文字。"""
    pattern = re.compile(r"https?://[^\s<>\"')]+")
    m = pattern.findall(text)
    return m[0] if m else ""


def load_state():
    """前回保存時刻を読み込む"""
    if not os.path.exists(STATE_PATH):
        return {"last_checked_at": "1970-01-01T00:00:00Z"}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_state(iso):
    """保存時刻を保存する"""
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump({"last_checked_at": iso}, f, ensure_ascii=False)


def iso_to_ts(iso):
    """ISO 8601 形式の文字列を datetime オブジェクトに変換する"""
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def ts_to_iso(dt):
    """datetime オブジェクトを ISO 8601 形式の文字列に変換する"""
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def discord_get(url, params=None):
    """Discord API を叩いて JSON を取得する"""
    r = requests.get(
        url, headers={"Authorization": f"Bot {TOKEN}"}, params=params or {}
    )
    r.raise_for_status()
    return r.json()


def get_all_text_channels():
    """全てのテキストチャンネルのチャンネルIDを取得する"""
    channels = discord_get(f"{DISCORD_API}/guilds/{GUILD}/channels")

    text_channels = []
    for ch in channels:
        # type 0 = 通常のテキストチャンネル
        # type 5 = アナウンスチャンネル
        if ch.get("type") in [0, 5]:
            text_channels.append(ch["id"])

    return text_channels


def get_channel_name(channel_id: str) -> str:
    """チャンネルIDからチャンネル名を取得する"""
    # 可能なら一度だけAPIを叩いてキャッシュ
    if channel_id in CHANNEL_NAME_CACHE:
        return CHANNEL_NAME_CACHE[channel_id]
    try:
        ch = discord_get(f"{DISCORD_API}/channels/{channel_id}")
        name = ch.get("name") or str(channel_id)
    except Exception:
        name = str(channel_id)
    CHANNEL_NAME_CACHE[channel_id] = name
    return name


def iter_messages(channel_id, after_iso):
    """チャンネル内のメッセージを取得する"""
    cutoff = iso_to_ts(after_iso)  # これより新しいものだけ欲しい
    before_id = None

    while True:
        params = {"limit": 100}
        if before_id:
            params["before"] = before_id

        msgs = discord_get(f"{DISCORD_API}/channels/{channel_id}/messages", params)
        if not msgs:
            break

        # ページの最も新しい（先頭）時刻を見て、ページごと打ち切り判定
        newest_ts = datetime.fromisoformat(msgs[0]["timestamp"].replace("Z", "+00:00"))
        if newest_ts <= cutoff:
            # このページの最新ですら古い → 以降のページも全部古いので終了
            break

        # ページ内は古い→新しい順に処理し、閾値より新しいものだけyield
        for m in reversed(msgs):
            ts = datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00"))
            if ts > cutoff:
                yield m
            # ts <= cutoff はスキップ（returnしない！次の“より新しい”要素を見る）

        # 次ページは、今回ページの最も古いIDより“さらに古い側”を取りに行く
        before_id = msgs[-1]["id"]

        if len(msgs) < 100:
            break


def has_target_reaction(m) -> bool:
    """メッセージに対象のリアクションがあるかどうかを判定する"""
    for r in m.get("reactions", []):
        # カスタム絵文字は "<:name:id>" 形式、標準はそのまま
        name = r["emoji"].get("name")
        if name and name in EMOJIS and r.get("count", 0) > 0:
            return True
    return False


# --- Notion への転記 ---
def sink_notion(items):
    """Notion への転記を行う"""
    notion = Client(auth=os.environ["NOTION_TOKEN"])
    db_id = os.environ["NOTION_DB_ID"]

    for it in items:
        # NotionのTitleは空だとエラーになりやすいのでフォールバック
        title_text = it["content"].strip() or "(本文なし)"
        if len(title_text) > 2000:
            title_text = title_text[:2000]

        extracted_url = extract_first_url(title_text)

        properties = {
            # タイトル（Title）型：DB側のタイトル名を「本文」にしておく
            "本文": {"title": [{"text": {"content": title_text}}]},
            # リッチテキスト
            "チャンネル": {"rich_text": [{"text": {"content": it["channel_name"]}}]},
            # URL
            "リンク": {"url": extracted_url or None},
            # 日付
            "日時": {"date": {"start": it["ts"]}},
        }

        notion.pages.create(parent={"database_id": db_id}, properties=properties)


def main():
    """メイン関数"""
    state = load_state()
    after_iso = state["last_checked_at"]
    text_channels = get_all_text_channels()
    collected = []

    for ch in text_channels:
        channel_name = get_channel_name(ch)
        for m in iter_messages(ch, after_iso):
            if has_target_reaction(m):
                collected.append(
                    {
                        "content": m.get("content", ""),
                        "url": f"https://discord.com/channels/{GUILD}/{ch}/{m['id']}",
                        "channel_id": ch,
                        "channel_name": channel_name,
                        "ts": m[
                            "timestamp"
                        ],  # ISO 8601 (e.g., "2025-10-12T12:34:56.789000+00:00" or "...Z")
                    }
                )

    if collected:
        sink_notion(collected)

    # 実行終了時に最終チェック時刻を更新
    save_state(ts_to_iso(datetime.now(timezone.utc)))


if __name__ == "__main__":
    main()
