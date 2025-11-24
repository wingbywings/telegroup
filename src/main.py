import argparse
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from telethon import TelegramClient

from config import ChatConfig, Config, load_config
from constants import TOP_THREAD_ID
from database import ensure_dirs, ensure_db, get_last_id, set_last_id
from message_handler import (
    build_media_path,
    extract_media,
    is_video_or_voice,
    normalize_dt,
)
from report_generator import generate_report


log = logging.getLogger(__name__)


def build_client(cfg: Config) -> TelegramClient:
    return TelegramClient(str(cfg.session_path), cfg.api_id, cfg.api_hash)


async def get_chat_id_from_link(client: TelegramClient, chat_link: str) -> int:
    """
    从 Telegram 群聊分享链接获取 chat_id。
    
    Args:
        client: TelegramClient 实例
        chat_link: 群聊分享链接，例如 "https://t.me/nofx_dev_community"
    
    Returns:
        chat_id: 用于 API 调用的 chat_id（通常以 -100 开头）
    """
    entity = await client.get_entity(chat_link)
    real_id = entity.id
    # 对于超级群组，chat_id 通常是 -100 + real_id
    # 如果 real_id 已经是负数（可能是频道或其他类型），直接使用
    # 否则加上 -100 前缀（超级群组的常见格式）
    if real_id < 0:
        chat_id = real_id
        log.info("Entity ID is already negative, using as-is: %s", chat_id)
    else:
        # 将 real_id 转换为字符串，然后加上 -100 前缀
        chat_id = int(f"-100{real_id}")
    log.info("Resolved chat_link %s to chat_id %s (entity.id: %s)", chat_link, chat_id, real_id)
    return chat_id


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


async def fetch_incremental_for_chat(
    client: TelegramClient,
    cfg: Config,
    chat_config: ChatConfig,
    conn: sqlite3.Connection,
) -> None:
    """为单个群组拉取增量消息"""
    chat_id = chat_config.chat_id
    chat_name = chat_config.name or f"chat_{chat_id}"
    last_id = get_last_id(cfg, chat_id)
    now_cfg = datetime.now(tz=cfg.timezone)
    cutoff = now_cfg - timedelta(days=cfg.pull_days)
    inserted = 0
    skipped_by_time = 0
    max_id = last_id
    
    # 记录时间范围信息用于排查
    cutoff_utc = cutoff.astimezone(timezone.utc)
    now_utc = now_cfg.astimezone(timezone.utc)
    tz_name = getattr(cfg.timezone, "key", None) or str(cfg.timezone)
    log.info("Fetching messages for %s (chat_id: %s, last_id: %s)", chat_name, chat_id, last_id)
    log.info("Time range: now=%s (%s), cutoff=%s (%s), pull_days=%d", 
             now_cfg.isoformat(), tz_name, cutoff.isoformat(), tz_name, cfg.pull_days)
    log.info("Time range (UTC): now=%s, cutoff=%s", now_utc.isoformat(), cutoff_utc.isoformat())
    
    try:
        async for msg in client.iter_messages(
            chat_id,
            min_id=last_id,
            reverse=True,
        ):
            if msg is None:
                continue
            if msg.action:
                continue
            # 记录原始消息时间的时区信息
            msg_date_original = msg.date
            msg_dt = normalize_dt(msg.date, cfg.timezone)
            if msg_dt < cutoff:
                skipped_by_time += 1
                # 只记录前几条被过滤的消息，避免日志过多
                if skipped_by_time <= 3:
                    msg_utc = msg_date_original.astimezone(timezone.utc) if msg_date_original.tzinfo else msg_date_original.replace(tzinfo=timezone.utc)
                    log.debug("Skipped message %s: date=%s (UTC: %s, %s: %s) < cutoff=%s (%s)", 
                             msg.id, msg_date_original, msg_utc.isoformat(), tz_name, msg_dt.isoformat(), 
                             cutoff.isoformat(), tz_name)
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
            # 提取 reply_to 字段：处理回复消息的情况
            # Telethon 的 reply_to 可能是 MessageReplyHeader 对象
            reply_to = None
            if msg.reply_to:
                # 优先使用 reply_to_msg_id（普通回复）
                if hasattr(msg.reply_to, "reply_to_msg_id") and msg.reply_to.reply_to_msg_id:
                    reply_to = msg.reply_to.reply_to_msg_id
                # 如果是论坛主题回复，可能需要特殊处理（暂时忽略）
                elif hasattr(msg.reply_to, "forum_topic") and msg.reply_to.forum_topic:
                    log.debug("Message %s is a forum topic reply, skipping reply_to", msg.id)
            
            # 根据 reply_to 字段进行分类：
            # - 如果 reply_to 不为 NULL，则 thread_id = reply_to（属于回复该消息的线程）
            # - 如果 reply_to 为 NULL，则 thread_id = TOP_THREAD_ID（顶层消息，统一归类）
            thread_id = reply_to if reply_to is not None else TOP_THREAD_ID
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
    except Exception as exc:
        log.error("Error fetching messages for %s (chat_id: %s): %s", chat_name, chat_id, exc)
        raise

    if max_id != last_id:
        set_last_id(cfg, chat_id, max_id)
    log.info("Pulled %s new messages for %s (chat_id: %s, last_id %s -> %s, skipped_by_time: %s)", 
             inserted, chat_name, chat_id, last_id, max_id, skipped_by_time)


async def fetch_incremental(client: TelegramClient, cfg: Config) -> None:
    """为所有配置的群组拉取增量消息"""
    if not cfg.chats:
        log.warning("No chats configured. Nothing to fetch.")
        return
    
    conn = sqlite3.connect(cfg.db_path)
    conn.row_factory = sqlite3.Row
    try:
        for chat_config in cfg.chats:
            await fetch_incremental_for_chat(client, cfg, chat_config, conn)
        conn.commit()
    finally:
        conn.close()




def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Telethon group daily report")
    parser.add_argument("--config", default="config/config.json", help="Config path")
    parser.add_argument("--init-session", action="store_true", help="Authorize session")
    parser.add_argument("--pull", action="store_true", help="Pull incremental messages")
    parser.add_argument("--report", action="store_true", help="Generate daily report for today")
    return parser.parse_args()


def _validate_and_resolve_chats(client: TelegramClient, cfg: Config) -> None:
    """
    验证并解析所有群组的 chat_id
    
    Args:
        client: TelegramClient 实例
        cfg: 配置对象
    
    Raises:
        ValueError: 如果没有配置群组或配置无效
    """
    if not cfg.chats:
        raise ValueError("No chats configured. Please configure at least one chat in config.json")
    
    for chat_config in cfg.chats:
        # 如果配置了 chat_link，从链接获取 chat_id
        if chat_config.chat_link:
            if not chat_config.chat_id or chat_config.chat_id == 0:
                log.info("Resolving chat_id from chat_link: %s", chat_config.chat_link)
                chat_config.chat_id = client.loop.run_until_complete(
                    get_chat_id_from_link(client, chat_config.chat_link)
                )
            else:
                log.info("Both chat_link and chat_id are configured for %s. Using chat_id: %s", 
                         chat_config.name or "chat", chat_config.chat_id)
        elif not chat_config.chat_id or chat_config.chat_id == 0:
            raise ValueError(
                f"Either chat_id or chat_link must be configured for chat: {chat_config.name or 'unnamed'}"
            )
    
    # 更新向后兼容的单个 chat_id（用于旧代码）
    if cfg.chats:
        cfg.chat_id = cfg.chats[0].chat_id
        cfg.chat_link = cfg.chats[0].chat_link


def _generate_all_reports(client: TelegramClient, cfg: Config) -> None:
    """
    为所有配置的群组生成报告
    
    Args:
        client: TelegramClient 实例
        cfg: 配置对象
    """
    conn = sqlite3.connect(cfg.db_path)
    try:
        now_cfg = datetime.now(tz=cfg.timezone)
        today = now_cfg.date()
        day_start = datetime.combine(today, datetime.min.time(), tzinfo=cfg.timezone)
        tz_name = getattr(cfg.timezone, "key", None) or str(cfg.timezone)
        log.info("Generating reports for date: %s (%s), day_start: %s", 
                 today.isoformat(), tz_name, day_start.isoformat())
        all_reports: List[str] = []
        
        # 为每个群组生成报告
        for chat_config in cfg.chats:
            report_text = generate_report(
                conn,
                cfg,
                day_start,
                chat_config.chat_id,
                chat_config.name,
                chat_config.chat_type,
                chat_config.chat_link,
            )
            if report_text.strip():
                all_reports.append(report_text)
    finally:
        conn.close()
    
    # 如果配置了发送报告，将所有报告合并发送
    if cfg.send_report_to_me and all_reports:
        combined_report = "\n\n---\n\n".join(all_reports)
        client.loop.run_until_complete(
            client.send_message("me", combined_report, parse_mode="md")
        )


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

        # 验证并解析所有群组的 chat_id
        _validate_and_resolve_chats(client, cfg)

        if args.pull:
            client.loop.run_until_complete(fetch_incremental(client, cfg))

        if args.report:
            _generate_all_reports(client, cfg)


if __name__ == "__main__":
    main()
