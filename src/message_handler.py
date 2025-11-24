"""消息处理模块：处理消息的解析、格式化和媒体处理"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple

from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo

from config import Config


def normalize_dt(dt: datetime, tz: timezone) -> datetime:
    """
    将 datetime 对象标准化到指定时区。
    
    Telethon 返回的 msg.date 通常是 UTC 时区的 datetime 对象（有时可能没有时区信息）。
    此函数确保：
    1. 如果 datetime 没有时区信息，假设为 UTC
    2. 将 datetime 转换为配置的时区
    
    Args:
        dt: 原始 datetime 对象（可能没有时区信息，或为 UTC）
        tz: 目标时区（配置的时区，如 Asia/Shanghai）
    
    Returns:
        转换到目标时区的 datetime 对象
    """
    if dt.tzinfo is None:
        # Telethon 返回的时间如果没有时区信息，通常是 UTC
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)


def extract_media(meta: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    从消息媒体对象中提取媒体类型和文件ID
    
    Args:
        meta: Telethon 消息的 media 属性
    
    Returns:
        (media_type, file_id) 元组，如果无媒体则返回 (None, None)
    """
    if not meta:
        return None, None
    media_type = meta.__class__.__name__
    file_id = None
    try:
        file_id = meta.file.id  # type: ignore[attr-defined]
    except Exception:
        file_id = None
    return media_type, file_id


def is_video_or_voice(msg: Any) -> bool:
    """
    判断消息是否为视频或语音类型
    
    Args:
        msg: Telethon 消息对象
    
    Returns:
        如果是视频或语音返回 True，否则返回 False
    """
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


def build_media_path(msg: Any, cfg: Config) -> Path:
    """
    构建媒体文件的保存路径
    
    Args:
        msg: Telethon 消息对象
        cfg: 配置对象
    
    Returns:
        媒体文件的完整路径
    """
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


def format_user(user_id: Optional[int], username: Optional[str]) -> str:
    """
    格式化用户显示名称
    
    Args:
        user_id: 用户ID
        username: 用户名（可选）
    
    Returns:
        格式化的用户名称，优先使用 @username，否则使用 user_{id}，最后使用 unknown
    """
    if username:
        return f"@{username}"
    if user_id:
        return f"user_{user_id}"
    return "unknown"
