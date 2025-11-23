import argparse
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

from ai_client import AISummaryError, call_chat_analysis


log = logging.getLogger(__name__)


class Config:
    def __init__(self, raw: Dict[str, Any]) -> None:
        self.api_id: int = int(raw["api_id"])
        self.api_hash: str = str(raw["api_hash"])
        self.phone: str = str(raw["phone"])
        self.session_path: Path = Path(raw.get("session_path", "config/telethon.session"))
        self.chat_id: int = int(raw["chat_id"])
        self.db_path: Path = Path(raw.get("db_path", "data/messages.db"))
        self.report_dir: Path = Path(raw.get("report_dir", "reports"))
        self.media_dir: Path = Path(raw.get("media_dir", "data/media"))
        self.timezone: timezone = timezone.utc
        tz_name = raw.get("timezone")
        if tz_name:
            try:
                from zoneinfo import ZoneInfo

                self.timezone = ZoneInfo(tz_name)
            except Exception as exc:  # pragma: no cover - best effort fallback
                log.warning("Failed to load timezone %s: %s, fallback to UTC", tz_name, exc)
        self.last_id_path: Path = Path(raw.get("last_id_path", "data/last_id.txt"))
        self.pull_days: int = int(raw.get("pull_days", 2))
        self.send_report_to_me: bool = bool(raw.get("send_report_to_me", True))
        self.download_media: bool = bool(raw.get("download_media", True))
        self.max_media_mb: int = int(raw.get("max_media_mb", 10))
        self.enable_ai_summary: bool = bool(raw.get("enable_ai_summary", False))
        self.ai_api_base: str = str(raw.get("ai_api_base", "")).strip()
        self.ai_api_key: str = str(raw.get("ai_api_key", "")).strip()
        self.ai_model: str = str(raw.get("ai_model", "grok-beta")).strip()
        self.ai_max_categories: int = int(raw.get("ai_max_categories", 5))
        self.ai_timeout: float = float(raw.get("ai_timeout", 120.0))
        self.ai_style: Optional[str] = str(raw["ai_style"]).strip() if "ai_style" in raw else None
        self.ai_max_messages_per_batch: int = int(raw.get("ai_max_messages_per_batch", 200))


def load_config(path: Path) -> Config:
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return Config(raw)


def ensure_dirs(cfg: Config) -> None:
    cfg.session_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.report_dir.mkdir(parents=True, exist_ok=True)
    cfg.last_id_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.media_dir.mkdir(parents=True, exist_ok=True)


def ensure_db(cfg: Config) -> None:
    conn = sqlite3.connect(cfg.db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                user_id INTEGER,
                username TEXT,
                text TEXT,
                media_type TEXT,
                file_id TEXT,
                reply_to INTEGER,
                date TEXT NOT NULL,
                file_path TEXT,
                thread_id INTEGER,
                PRIMARY KEY (chat_id, message_id)
            );
            """
        )
        # Migration: ensure file_path column exists
        cols = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
        if "file_path" not in cols:
            conn.execute("ALTER TABLE messages ADD COLUMN file_path TEXT;")
        # Migration: ensure thread_id column exists and populate it
        if "thread_id" not in cols:
            conn.execute("ALTER TABLE messages ADD COLUMN thread_id INTEGER;")
            # 根据 reply_to 字段进行分类：
            # - 如果 reply_to 不为 NULL，则 thread_id = reply_to（属于回复该消息的线程）
            # - 如果 reply_to 为 NULL，则 thread_id = -1（顶层消息，统一归类）
            conn.execute(
                """
                UPDATE messages
                SET thread_id = CASE
                    WHEN reply_to IS NOT NULL THEN reply_to
                    ELSE -1
                END
                """
            )
            conn.commit()
            log.info("Added thread_id column and populated based on reply_to classification")
        else:
            # Migration: update existing records where reply_to is NULL to use thread_id = -1
            conn.execute(
                """
                UPDATE messages
                SET thread_id = -1
                WHERE reply_to IS NULL AND thread_id != -1
                """
            )
            conn.commit()
            updated = conn.execute("SELECT changes()").fetchone()[0]
            if updated > 0:
                log.info("Updated %s messages without reply_to to thread_id=-1", updated)
        conn.commit()
    finally:
        conn.close()


def get_last_id(cfg: Config) -> int:
    if not cfg.last_id_path.exists():
        return 0
    try:
        return int(cfg.last_id_path.read_text().strip() or "0")
    except ValueError:
        return 0


def set_last_id(cfg: Config, value: int) -> None:
    cfg.last_id_path.write_text(str(value))


def build_client(cfg: Config) -> TelegramClient:
    return TelegramClient(str(cfg.session_path), cfg.api_id, cfg.api_hash)


def normalize_dt(dt: datetime, tz: timezone) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)


def extract_media(meta) -> Tuple[Optional[str], Optional[str]]:
    if not meta:
        return None, None
    media_type = meta.__class__.__name__
    file_id = None
    try:
        file_id = meta.file.id  # type: ignore[attr-defined]
    except Exception:
        file_id = None
    return media_type, file_id


def is_video_or_voice(msg) -> bool:
    if getattr(msg, "video", None) or getattr(msg, "video_note", None) or getattr(msg, "voice", None):
        return True
    doc = getattr(msg, "document", None)
    if doc and getattr(doc, "attributes", None):
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                return True
            if isinstance(attr, DocumentAttributeAudio) and getattr(attr, "voice", False):
                return True
    return False


def build_media_path(msg, cfg: Config) -> Path:
    name = None
    if getattr(msg, "file", None):
        name = getattr(msg.file, "name", None)
        ext = getattr(msg.file, "ext", None)
    else:
        ext = None
    if name:
        return cfg.media_dir / f"{msg.id}_{name}"
    if ext:
        return cfg.media_dir / f"{msg.id}{ext}"
    return cfg.media_dir / f"{msg.id}.bin"


async def init_session(client: TelegramClient, cfg: Config) -> None:
    await client.connect()
    if await client.is_user_authorized():
        log.info("Session already authorized.")
        return
    log.info("Authorizing session for %s ...", cfg.phone)
    await client.send_code_request(cfg.phone)
    code = input("Enter the code you received: ")
    await client.sign_in(cfg.phone, code)
    log.info("Session saved to %s", cfg.session_path)


async def fetch_incremental(client: TelegramClient, cfg: Config) -> None:
    last_id = get_last_id(cfg)
    cutoff = datetime.now(tz=cfg.timezone) - timedelta(days=cfg.pull_days)
    conn = sqlite3.connect(cfg.db_path)
    conn.row_factory = sqlite3.Row
    inserted = 0
    max_id = last_id
    try:
        async for msg in client.iter_messages(
            cfg.chat_id,
            min_id=last_id,
            reverse=True,
        ):
            if msg is None:
                continue
            if msg.action:
                continue
            msg_dt = normalize_dt(msg.date, cfg.timezone)
            if msg_dt < cutoff:
                continue
            media_type, file_id = extract_media(msg.media)
            file_path: Optional[Path] = None
            if cfg.download_media and media_type and not is_video_or_voice(msg):
                size = getattr(msg.file, "size", None) if getattr(msg, "file", None) else None
                if size is not None and size <= cfg.max_media_mb * 1024 * 1024:
                    target = build_media_path(msg, cfg)
                    try:
                        await client.download_media(msg, file=target)
                        file_path = target
                    except Exception as exc:
                        log.warning("Download failed for msg %s: %s", msg.id, exc)
                else:
                    log.debug("Skip media download for msg %s due to size or unknown", msg.id)
            reply_to = msg.reply_to.reply_to_msg_id if msg.reply_to else None
            # 根据 reply_to 字段进行分类：
            # - 如果 reply_to 不为 NULL，则 thread_id = reply_to（属于回复该消息的线程）
            # - 如果 reply_to 为 NULL，则 thread_id = -1（顶层消息，统一归类）
            thread_id = reply_to if reply_to is not None else -1
            text = msg.message or ""
            conn.execute(
                """
                INSERT OR IGNORE INTO messages
                (chat_id, message_id, user_id, username, text, media_type, file_id, reply_to, date, file_path, thread_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    msg.chat_id,
                    msg.id,
                    msg.sender_id,
                    getattr(msg.sender, "username", None) if msg.sender else None,
                    text,
                    media_type,
                    file_id,
                    reply_to,
                    msg_dt.isoformat(),
                    str(file_path) if file_path else None,
                    thread_id,
                ),
            )
            inserted += 1
            if msg.id > max_id:
                max_id = msg.id
        conn.commit()
    finally:
        conn.close()

    if max_id != last_id:
        set_last_id(cfg, max_id)
    log.info("Pulled %s new messages (last_id %s -> %s)", inserted, last_id, max_id)


def format_user(user_id: Optional[int], username: Optional[str]) -> str:
    if username:
        return f"@{username}"
    if user_id:
        return f"user_{user_id}"
    return "unknown"


def generate_report(conn: sqlite3.Connection, cfg: Config, day_start: datetime) -> str:
    day_end = day_start + timedelta(days=1)
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT message_id, user_id, username, text, media_type, reply_to, date, thread_id
        FROM messages
        WHERE chat_id = ? AND date >= ? AND date < ?
        ORDER BY date ASC
        """,
        (cfg.chat_id, day_start.isoformat(), day_end.isoformat()),
    )
    rows = cur.fetchall()
    total = len(rows)

    user_stats: Dict[str, int] = {}
    media_stats: Dict[str, int] = {}
    thread_stats: Dict[int, int] = {}

    for row in rows:
        key = format_user(row["user_id"], row["username"])
        user_stats[key] = user_stats.get(key, 0) + 1
        if row["media_type"]:
            media_stats[row["media_type"]] = media_stats.get(row["media_type"], 0) + 1
        if row["reply_to"]:
            thread_stats[row["reply_to"]] = thread_stats.get(row["reply_to"], 0) + 1

    top_users = sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:5]
    top_threads = sorted(thread_stats.items(), key=lambda x: x[1], reverse=True)[:5]

    lines = []
    date_str = day_start.date().isoformat()
    lines.append(f"# {date_str} 群日报")
    lines.append(f"- 群 ID: `{cfg.chat_id}`")
    lines.append(f"- 时间范围: {day_start.isoformat()} ~ {day_end.isoformat()}")
    lines.append(f"- 总消息数: {total}")
    lines.append(f"- 发言人数: {len(user_stats)}")
    lines.append("")

    lines.append("## 活跃用户 Top 5")
    if top_users:
        for name, cnt in top_users:
            lines.append(f"- {name}: {cnt}")
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 媒体分布")
    if media_stats:
        for m, cnt in media_stats.items():
            lines.append(f"- {m}: {cnt}")
    else:
        lines.append("- 无")
    lines.append("")

    lines.append("## 热门回复线程")
    if top_threads:
        for mid, cnt in top_threads:
            lines.append(f"- 回复消息 {mid}: {cnt} 条回复")
    else:
        lines.append("- 无")

    lines.extend(build_ai_summary_section(rows, cfg, day_start))

    report = "\n".join(lines)
    report_path = cfg.report_dir / f"{date_str}.md"
    report_path.write_text(report, encoding="utf-8")
    log.info("Report written to %s", report_path)
    return report


def build_ai_summary_section(rows: List[sqlite3.Row], cfg: Config, day_start: datetime) -> List[str]:
    if not cfg.enable_ai_summary:
        return []

    lines = ["", "## AI 线程摘要"]

    if not cfg.ai_api_base:
        lines.append("- AI 摘要未生成：缺少 ai_api_base 配置。")
        log.warning("AI summary enabled but ai_api_base not set.")
        return lines

    if not cfg.ai_api_key:
        lines.append("- AI 摘要未生成：缺少 ai_api_key 配置。")
        log.warning("AI summary enabled but ai_api_key not set.")
        return lines

    # 按 thread_id 分组消息
    threads: Dict[int, List[sqlite3.Row]] = {}
    for row in rows:
        thread_id = row["thread_id"]
        if thread_id not in threads:
            threads[thread_id] = []
        threads[thread_id].append(row)

    # 过滤掉消息数量小于 10 的线程
    valid_threads = {tid: msgs for tid, msgs in threads.items() if len(msgs) >= 10}
    
    if not valid_threads:
        lines.append("- 没有符合条件的线程（消息数量 >= 10）。")
        return lines

    lines.append(f"- 共 {len(valid_threads)} 个线程符合分析条件（消息数量 >= 10）")
    lines.append("")

    tz_name = getattr(cfg.timezone, "key", None) or str(cfg.timezone)

    # 为每个符合条件的线程分别调用 AI 分析
    for thread_id, thread_rows in sorted(valid_threads.items(), key=lambda x: len(x[1]), reverse=True):
        thread_name = "顶层消息" if thread_id == -1 else f"线程 {thread_id}"
        total_messages = len(thread_rows)
        lines.append(f"### {thread_name}（{total_messages} 条消息）")

        # 如果消息数量超过阈值，进行分段处理
        if total_messages > cfg.ai_max_messages_per_batch:
            num_batches = (total_messages + cfg.ai_max_messages_per_batch - 1) // cfg.ai_max_messages_per_batch
            lines.append(f"  - 消息数量较多，将分成 {num_batches} 个批次处理（每批最多 {cfg.ai_max_messages_per_batch} 条）")
            lines.append("")

            all_overalls: List[str] = []
            all_categories: List[Dict[str, Any]] = []
            batch_failed = False

            # 分段处理
            for batch_idx in range(num_batches):
                start_idx = batch_idx * cfg.ai_max_messages_per_batch
                end_idx = min(start_idx + cfg.ai_max_messages_per_batch, total_messages)
                batch_rows = thread_rows[start_idx:end_idx]
                batch_num = batch_idx + 1

                messages = []
                for row in batch_rows:
                    messages.append(
                        {
                            "id": row["message_id"],
                            "user": format_user(row["user_id"], row["username"]),
                            "ts": row["date"],
                            "text": row["text"] or "",
                            "media_type": row["media_type"],
                            "reply_to": row["reply_to"],
                        }
                    )

                payload: Dict[str, Any] = {
                    "chat_id": cfg.chat_id,
                    "date": day_start.date().isoformat(),
                    "timezone": tz_name,
                    "thread_id": thread_id,
                    "messages": messages,
                    "batch_info": f"批次 {batch_num}/{num_batches}，共 {total_messages} 条消息",
                }
                if cfg.ai_max_categories:
                    payload["max_categories"] = cfg.ai_max_categories
                if cfg.ai_style:
                    payload["style"] = cfg.ai_style

                log.info(
                    "Calling AI summary for thread %s batch %s/%s: base=%s model=%s messages=%s",
                    thread_id,
                    batch_num,
                    num_batches,
                    cfg.ai_api_base,
                    cfg.ai_model,
                    len(messages),
                )
                try:
                    data = call_chat_analysis(
                        cfg.ai_api_base, cfg.ai_api_key, payload, model=cfg.ai_model, timeout=cfg.ai_timeout
                    )
                except AISummaryError as exc:
                    lines.append(f"    - AI 摘要生成失败：{exc}")
                    log.warning("AI summary failed for thread %s batch %s: %s", thread_id, batch_num, exc)
                    batch_failed = True
                    lines.append("")
                    continue

                batch_overall = data.get("overall")
                if batch_overall:
                    all_overalls.append(f"批次 {batch_num}: {batch_overall}")

                batch_categories = data.get("categories") or []
                if batch_categories:
                    all_categories.extend(batch_categories)

            # 合并所有批次的结果
            if batch_failed and not all_overalls and not all_categories:
                lines.append("  - 所有批次处理失败")
            else:
                if all_overalls:
                    lines.append("  - 总览（各批次摘要）：")
                    for overall in all_overalls:
                        lines.append(f"    - {overall}")

                if all_categories:
                    lines.append("")
                    lines.append("  - 分类（合并所有批次）：")
                    # 合并相同名称的分类
                    category_map: Dict[str, List[str]] = {}
                    for cat in all_categories:
                        name = cat.get("name") or "未命名分类"
                        summary = cat.get("summary") or ""
                        if name not in category_map:
                            category_map[name] = []
                        category_map[name].append(summary)

                    for name, summaries in category_map.items():
                        if len(summaries) == 1:
                            lines.append(f"    - {name}: {summaries[0]}")
                        else:
                            lines.append(f"    - {name}: {', '.join(summaries)}")

        else:
            # 消息数量不多，直接处理
            messages = []
            for row in thread_rows:
                messages.append(
                    {
                        "id": row["message_id"],
                        "user": format_user(row["user_id"], row["username"]),
                        "ts": row["date"],
                        "text": row["text"] or "",
                        "media_type": row["media_type"],
                        "reply_to": row["reply_to"],
                    }
                )

            payload: Dict[str, Any] = {
                "chat_id": cfg.chat_id,
                "date": day_start.date().isoformat(),
                "timezone": tz_name,
                "thread_id": thread_id,
                "messages": messages,
            }
            if cfg.ai_max_categories:
                payload["max_categories"] = cfg.ai_max_categories
            if cfg.ai_style:
                payload["style"] = cfg.ai_style

            log.info(
                "Calling AI summary for thread %s: base=%s model=%s messages=%s",
                thread_id,
                cfg.ai_api_base,
                cfg.ai_model,
                len(messages),
            )
            try:
                data = call_chat_analysis(
                    cfg.ai_api_base, cfg.ai_api_key, payload, model=cfg.ai_model, timeout=cfg.ai_timeout
                )
            except AISummaryError as exc:
                lines.append(f"  - AI 摘要生成失败：{exc}")
                log.warning("AI summary failed for thread %s: %s", thread_id, exc)
                lines.append("")
                continue

            overall = data.get("overall")
            if overall:
                lines.append(f"  - 总览：{overall}")

            categories = data.get("categories") or []
            if categories:
                lines.append("")
                lines.append("  - 分类：")
                for cat in categories:
                    name = cat.get("name") or "未命名分类"
                    summary = cat.get("summary") or ""
                    lines.append(f"    - {name}: {summary}")
            else:
                lines.append("  - 未返回分类结果。")

        lines.append("")

    return lines


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telethon group daily report")
    parser.add_argument("--config", default="config/config.json", help="Config path")
    parser.add_argument("--init-session", action="store_true", help="Authorize session")
    parser.add_argument("--pull", action="store_true", help="Pull incremental messages")
    parser.add_argument("--report", action="store_true", help="Generate daily report for today")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = parse_args()
    cfg = load_config(Path(args.config))
    ensure_dirs(cfg)
    ensure_db(cfg)

    if not any([args.init_session, args.pull, args.report]):
        log.info("Nothing to do. Use --init-session / --pull / --report.")
        return

    with build_client(cfg) as client:
        client.loop.run_until_complete(client.connect())
        if args.init_session:
            client.loop.run_until_complete(init_session(client, cfg))
            if not (args.pull or args.report):
                return

        if not client.loop.run_until_complete(client.is_user_authorized()):
            raise RuntimeError("Session not authorized. Run with --init-session first.")

        if args.pull:
            client.loop.run_until_complete(fetch_incremental(client, cfg))

        if args.report:
            conn = sqlite3.connect(cfg.db_path)
            try:
                today = datetime.now(tz=cfg.timezone).date()
                report_text = generate_report(
                    conn,
                    cfg,
                    datetime.combine(today, datetime.min.time(), tzinfo=cfg.timezone),
                )
            finally:
                conn.close()
            if cfg.send_report_to_me and report_text.strip():
                client.loop.run_until_complete(
                    client.send_message("me", report_text, parse_mode="md")
                )


if __name__ == "__main__":
    main()
