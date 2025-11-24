"""配置模块：处理应用配置的加载和解析"""
import json
import logging
from datetime import timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)


class ChatConfig:
    """单个群组的配置"""
    
    def __init__(self, raw: Dict[str, Any]) -> None:
        self.chat_id: int = int(raw.get("chat_id", 0))
        chat_link_raw = raw.get("chat_link")
        if chat_link_raw:
            self.chat_link: Optional[str] = str(chat_link_raw).strip()
            if not self.chat_link:  # 空字符串转为 None
                self.chat_link = None
        else:
            self.chat_link = None
        # 可选：群组名称，用于报告标题
        self.name: Optional[str] = raw.get("name")
        # 可选：群组类型，用于AI分析策略（"crypto" 加密货币群 / "tech" 技术项目群 / "news" 新闻类群组）
        # 如果不指定，AI会根据消息内容自动判断
        self.chat_type: Optional[str] = raw.get("chat_type")
        if self.chat_type:
            self.chat_type = self.chat_type.strip().lower()
            if self.chat_type not in ("crypto", "tech", "news"):
                log.warning("Invalid chat_type '%s' for chat %s, will auto-detect", self.chat_type, self.chat_id)
                self.chat_type = None
        # 可选：该群组的最小线程消息数量限制，用于AI分析
        # 如果不指定，使用全局默认值 MIN_THREAD_MESSAGES
        if "min_thread_messages" in raw:
            self.min_thread_messages: Optional[int] = int(raw["min_thread_messages"])
            if self.min_thread_messages < 1:
                log.warning("Invalid min_thread_messages %s for chat %s, must be >= 1, using default", 
                           self.min_thread_messages, self.chat_id)
                self.min_thread_messages = None
        else:
            self.min_thread_messages = None
        # 可选：是否需要thread_id分类
        # 如果设置为true，则根据reply_to进行thread_id分类（正常分类行为）
        # 如果设置为false或不配置，则该群组下所有消息的thread_id都将被设置为-1（不进行分类）
        # 默认为false，即不进行分类
        self.enable_thread_classification: bool = bool(raw.get("enable_thread_classification", False))


class Config:
    """应用主配置类"""
    
    def __init__(self, raw: Dict[str, Any]) -> None:
        self.api_id: int = int(raw["api_id"])
        self.api_hash: str = str(raw["api_hash"])
        self.phone: str = str(raw["phone"])
        self.session_path: Path = Path(raw.get("session_path", "config/telethon.session"))
        
        # 支持多个群组配置
        # 优先使用新的 chats 数组格式，如果没有则向后兼容旧的 chat_id/chat_link
        if "chats" in raw:
            self.chats: List[ChatConfig] = [ChatConfig(chat_raw) for chat_raw in raw["chats"]]
        else:
            # 向后兼容：将旧的 chat_id/chat_link 转换为 chats 数组
            chat_config = {}
            if "chat_id" in raw:
                chat_config["chat_id"] = raw["chat_id"]
            if "chat_link" in raw:
                chat_link_raw = raw["chat_link"]
                if chat_link_raw:
                    chat_config["chat_link"] = str(chat_link_raw).strip() or None
            if chat_config:
                self.chats = [ChatConfig(chat_config)]
            else:
                self.chats = []
        
        # 保持向后兼容的单个 chat_id 和 chat_link（用于旧代码）
        if self.chats:
            self.chat_id: int = self.chats[0].chat_id
            self.chat_link: Optional[str] = self.chats[0].chat_link
        else:
            self.chat_id = 0
            self.chat_link = None
        
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
        self.last_id_path: Path = Path(raw.get("last_id_path", "data/last_id.json"))
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
    """从 JSON 文件加载配置"""
    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    return Config(raw)
