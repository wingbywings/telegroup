"""数据库模块：处理数据库初始化和 last_id 管理"""
import json
import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional

from config import Config
from constants import TOP_THREAD_ID

log = logging.getLogger(__name__)


def ensure_dirs(cfg: Config) -> None:
    """确保所有必需的目录存在"""
    cfg.session_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.db_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.report_dir.mkdir(parents=True, exist_ok=True)
    cfg.last_id_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.media_dir.mkdir(parents=True, exist_ok=True)


def ensure_db(cfg: Config) -> None:
    """确保数据库表存在，并执行必要的迁移"""
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
            # - 如果 reply_to 为 NULL，则 thread_id = TOP_THREAD_ID（顶层消息，统一归类）
            conn.execute(
                """
                UPDATE messages
                SET thread_id = CASE
                    WHEN reply_to IS NOT NULL THEN reply_to
                    ELSE ?
                END
                """,
                (TOP_THREAD_ID,),
            )
            conn.commit()
            log.info("Added thread_id column and populated based on reply_to classification")
        else:
            # Migration: update existing records where reply_to is NULL to use thread_id = TOP_THREAD_ID
            conn.execute(
                """
                UPDATE messages
                SET thread_id = ?
                WHERE reply_to IS NULL AND thread_id != ?
                """,
                (TOP_THREAD_ID, TOP_THREAD_ID),
            )
            conn.commit()
            updated = conn.execute("SELECT changes()").fetchone()[0]
            if updated > 0:
                log.info("Updated %s messages without reply_to to thread_id=%s", updated, TOP_THREAD_ID)
        
        # 创建索引以优化 reply_to 查询性能
        # 检查索引是否已存在
        indexes = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()]
        if "idx_messages_reply_to" not in indexes:
            conn.execute("CREATE INDEX idx_messages_reply_to ON messages(chat_id, reply_to)")
            log.info("Created index idx_messages_reply_to for reply_to queries")
        if "idx_messages_thread_id" not in indexes:
            conn.execute("CREATE INDEX idx_messages_thread_id ON messages(chat_id, thread_id)")
            log.info("Created index idx_messages_thread_id for thread_id queries")
        
        conn.commit()
    finally:
        conn.close()


def get_replied_message(conn: sqlite3.Connection, chat_id: int, message_id: int) -> Optional[sqlite3.Row]:
    """
    查询被回复的消息详情
    
    Args:
        conn: 数据库连接
        chat_id: 群组ID
        message_id: 消息ID（被回复的消息ID）
    
    Returns:
        被回复的消息行，如果不存在则返回 None
    """
    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT message_id, user_id, username, text, media_type, date
        FROM messages
        WHERE chat_id = ? AND message_id = ?
        """,
        (chat_id, message_id),
    )
    return cur.fetchone()


def get_last_id(cfg: Config, chat_id: int) -> int:
    """获取指定群组的 last_id"""
    if not cfg.last_id_path.exists():
        # 尝试迁移旧的 last_id.txt 文件
        old_path = cfg.last_id_path.parent / "last_id.txt"
        if old_path.exists():
            try:
                old_value = int(old_path.read_text().strip() or "0")
                # 迁移到新格式
                data = {str(cfg.chat_id): old_value}
                cfg.last_id_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
                log.info("Migrated last_id.txt to last_id.json for chat_id %s", cfg.chat_id)
                return old_value
            except (ValueError, KeyError):
                pass
        return 0
    try:
        data = json.loads(cfg.last_id_path.read_text(encoding="utf-8"))
        # 支持旧格式（单个数字）和新格式（JSON 对象）
        if isinstance(data, dict):
            return int(data.get(str(chat_id), 0))
        else:
            # 旧格式：单个数字，只对第一个群组有效
            return int(data) if chat_id == cfg.chat_id else 0
    except (json.JSONDecodeError, ValueError, KeyError):
        return 0


def set_last_id(cfg: Config, chat_id: int, value: int) -> None:
    """设置指定群组的 last_id"""
    data: Dict[str, int] = {}
    if cfg.last_id_path.exists():
        try:
            existing = json.loads(cfg.last_id_path.read_text(encoding="utf-8"))
            if isinstance(existing, dict):
                data = existing
            else:
                # 迁移旧格式
                data = {str(cfg.chat_id): int(existing)}
        except (json.JSONDecodeError, ValueError):
            pass
    data[str(chat_id)] = value
    cfg.last_id_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
