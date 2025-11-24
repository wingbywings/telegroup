"""报告生成模块：生成日报和 AI 摘要"""
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ai_client import AISummaryError, call_chat_analysis
from config import Config
from constants import (
    CATEGORY_PRIORITY,
    DEFAULT_CATEGORY_PRIORITY,
    MIN_THREAD_MESSAGES,
    TOP_N_MESSAGE_IDS,
    TOP_N_THREADS,
    TOP_N_USERS,
    TOP_THREAD_ID,
)
from database import get_replied_message
from message_handler import format_user

log = logging.getLogger(__name__)


def _calculate_statistics(rows: List[sqlite3.Row]) -> Tuple[Dict[str, int], Dict[str, int], Dict[int, int]]:
    """
    计算消息统计数据
    
    Args:
        rows: 消息行列表
    
    Returns:
        (user_stats, media_stats, thread_stats) 元组
    """
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

    return user_stats, media_stats, thread_stats


def _build_report_header(
    day_start: datetime, day_end: datetime, chat_id: int, chat_name: Optional[str], total: int, user_count: int
) -> List[str]:
    """
    构建报告头部
    
    Args:
        day_start: 报告开始时间
        day_end: 报告结束时间
        chat_id: 群组ID
        chat_name: 群组名称
        total: 总消息数
        user_count: 发言人数
    
    Returns:
        报告头部行列表
    """
    lines = []
    date_str = day_start.date().isoformat()
    chat_display_name = chat_name or f"群组 {chat_id}"
    
    # 格式化日期显示
    weekday_map = {0: "周一", 1: "周二", 2: "周三", 3: "周四", 4: "周五", 5: "周六", 6: "周日"}
    weekday = weekday_map[day_start.weekday()]
    date_display = f"{date_str} {weekday}"
    
    # 格式化时间范围（只显示日期和时间，不显示时区）
    time_start = day_start.strftime("%H:%M")
    time_end = day_end.strftime("%H:%M")
    
    lines.append(f"# 📊 {date_display} {chat_display_name} 日报")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("### 📋 基本信息")
    lines.append("")
    lines.append("| 项目 | 内容 |")
    lines.append("|------|------|")
    lines.append(f"| **群组名称** | {chat_display_name} |")
    lines.append(f"| **群组 ID** | `{chat_id}` |")
    lines.append(f"| **报告日期** | {date_str} ({weekday}) |")
    lines.append(f"| **时间范围** | {time_start} ~ {time_end} |")
    lines.append(f"| **总消息数** | **{total}** 条 |")
    lines.append(f"| **发言人数** | **{user_count}** 人 |")
    lines.append("")
    lines.append("---")
    lines.append("")
    return lines


def _build_report_content(
    user_stats: Dict[str, int], 
    media_stats: Dict[str, int], 
    thread_stats: Dict[int, int],
    conn: sqlite3.Connection,
    chat_id: int,
    chat_link: Optional[str] = None,
) -> List[str]:
    """
    构建报告内容部分（活跃用户、媒体分布）
    
    Args:
        user_stats: 用户统计
        media_stats: 媒体统计
        thread_stats: 线程统计（已弃用，保留以兼容接口）
        conn: 数据库连接
        chat_id: 群组ID
        chat_link: 群组链接（可选）
    
    Returns:
        报告内容行列表
    """
    lines = []
    top_users = sorted(user_stats.items(), key=lambda x: x[1], reverse=True)[:TOP_N_USERS]

    # 活跃用户 Top 5
    lines.append("## 👥 活跃用户 Top 5")
    lines.append("")
    if top_users:
        lines.append("| 排名 | 用户名 | 消息数 |")
        lines.append("|------|--------|--------|")
        rank_icons = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        for idx, (name, cnt) in enumerate(top_users):
            rank_icon = rank_icons[idx] if idx < len(rank_icons) else f"{idx + 1}."
            lines.append(f"| {rank_icon} | {name} | **{cnt}** |")
    else:
        lines.append("*暂无活跃用户数据*")
    lines.append("")

    # 媒体分布
    lines.append("## 📎 媒体分布")
    lines.append("")
    if media_stats:
        # 媒体类型显示名称映射
        media_display_names = {
            "MessageMediaPhoto": "📷 图片",
            "MessageMediaDocument": "📄 文档",
            "MessageMediaWebPage": "🔗 网页链接",
            "MessageMediaPoll": "📊 投票",
            "MessageMediaVideo": "🎥 视频",
            "MessageMediaAudio": "🎵 音频",
            "MessageMediaVoice": "🎤 语音",
        }
        
        lines.append("| 媒体类型 | 数量 |")
        lines.append("|----------|------|")
        total_media = sum(media_stats.values())
        for media_type, cnt in sorted(media_stats.items(), key=lambda x: x[1], reverse=True):
            display_name = media_display_names.get(media_type, f"📎 {media_type}")
            percentage = (cnt / total_media * 100) if total_media > 0 else 0
            lines.append(f"| {display_name} | **{cnt}** ({percentage:.1f}%) |")
    else:
        lines.append("*今日无媒体消息*")
    lines.append("")

    return lines


def generate_report(
    conn: sqlite3.Connection,
    cfg: Config,
    day_start: datetime,
    chat_id: int,
    chat_name: Optional[str] = None,
    chat_type: Optional[str] = None,
    chat_link: Optional[str] = None,
    min_thread_messages: Optional[int] = None,
) -> str:
    """为指定群组生成日报"""
    day_end = day_start + timedelta(days=1)
    tz_name = getattr(cfg.timezone, "key", None) or str(cfg.timezone)
    day_start_utc = day_start.astimezone(timezone.utc)
    day_end_utc = day_end.astimezone(timezone.utc)

    # 记录查询时间范围用于排查
    chat_display_name = chat_name or f"chat_{chat_id}"
    log.info("Generating report for %s (chat_id: %s)", chat_display_name, chat_id)
    log.info(
        "Report time range (%s): %s ~ %s",
        tz_name,
        day_start.isoformat(),
        day_end.isoformat(),
    )
    log.info(
        "Report time range (UTC): %s ~ %s",
        day_start_utc.isoformat(),
        day_end_utc.isoformat(),
    )

    conn.row_factory = sqlite3.Row
    cur = conn.execute(
        """
        SELECT message_id, user_id, username, text, media_type, reply_to, date, thread_id
        FROM messages
        WHERE chat_id = ? AND date >= ? AND date < ?
        ORDER BY date ASC
        """,
        (chat_id, day_start.isoformat(), day_end.isoformat()),
    )
    rows = cur.fetchall()
    total = len(rows)

    log.info("Found %s messages in database for time range", total)

    # 计算统计数据
    user_stats, media_stats, thread_stats = _calculate_statistics(rows)

    # 构建报告
    lines = _build_report_header(day_start, day_end, chat_id, chat_name, total, len(user_stats))
    lines.extend(_build_report_content(user_stats, media_stats, thread_stats, conn, chat_id, chat_link))

    lines.extend(build_ai_summary_section(rows, cfg, day_start, chat_id, chat_name, chat_type, chat_link, conn, min_thread_messages))

    report = "\n".join(lines)
    # 报告文件名包含 chat_id，如果有名称则使用名称（清理特殊字符）
    date_str = day_start.date().isoformat()
    if chat_name:
        safe_name = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in chat_name)
        report_filename = f"{date_str}_{safe_name}_{chat_id}.md"
    else:
        report_filename = f"{date_str}_{chat_id}.md"
    report_path = cfg.report_dir / report_filename
    report_path.write_text(report, encoding="utf-8")
    log.info("Report written to %s", report_path)
    return report


def _group_messages_by_thread(rows: List[sqlite3.Row]) -> Dict[int, List[sqlite3.Row]]:
    """
    按 thread_id 分组消息，并确保每个线程内的消息按时间顺序排序
    
    Args:
        rows: 消息行列表（应该已经按时间排序）
    
    Returns:
        按 thread_id 分组的消息字典，每个线程内的消息按时间顺序排序
    """
    threads: Dict[int, List[sqlite3.Row]] = {}
    for row in rows:
        thread_id = row["thread_id"]
        if thread_id not in threads:
            threads[thread_id] = []
        threads[thread_id].append(row)
    
    # 确保每个线程内的消息按时间顺序排序
    for thread_id in threads:
        threads[thread_id].sort(key=lambda r: r["date"])
    
    return threads


def _convert_rows_to_messages(
    rows: List[sqlite3.Row], 
    conn: sqlite3.Connection, 
    chat_id: int
) -> List[Dict[str, Any]]:
    """
    将数据库行转换为消息字典列表，包含完整的回复关系信息
    
    如果消息回复了不在数据库中的消息，则跳过该消息（不发送给AI）
    确保返回的消息列表按时间顺序排序。
    
    Args:
        rows: 消息行列表（应该已经按时间排序）
        conn: 数据库连接
        chat_id: 群组ID
    
    Returns:
        消息字典列表，包含回复关系信息，按时间顺序排序
    """
    # 确保输入的消息按时间排序
    sorted_rows = sorted(rows, key=lambda r: r["date"])
    
    messages = []
    skipped_count = 0
    for row in sorted_rows:
        msg_dict = {
            "id": row["message_id"],
            "user": format_user(row["user_id"], row["username"]),
            "ts": row["date"],
            "text": row["text"] or "",
            "media_type": row["media_type"],
            "reply_to": row["reply_to"],
        }
        
        # 如果消息有回复关系，查询被回复的消息详情
        if row["reply_to"]:
            replied_msg = get_replied_message(conn, chat_id, row["reply_to"])
            if replied_msg:
                # 被回复的消息在数据库中，包含完整信息
                msg_dict["replied_message"] = {
                    "id": replied_msg["message_id"],
                    "user": format_user(replied_msg["user_id"], replied_msg["username"]),
                    "text": replied_msg["text"] or "",
                    "media_type": replied_msg["media_type"],
                    "ts": replied_msg["date"],
                }
                messages.append(msg_dict)
            else:
                # 被回复的消息不在数据库中，跳过这条回复消息
                skipped_count += 1
                log.debug("Skipping message %s: replied message %s not in database", row["message_id"], row["reply_to"])
                continue
        else:
            # 没有回复关系，直接添加
            messages.append(msg_dict)
    
    if skipped_count > 0:
        log.info("Skipped %s messages with replied messages not in database", skipped_count)
    
    return messages


def _build_ai_payload(
    chat_id: int,
    chat_name: Optional[str],
    chat_type: Optional[str],
    day_start: datetime,
    tz_name: str,
    thread_id: int,
    messages: List[Dict[str, Any]],
    cfg: Config,
    batch_info: Optional[str] = None,
) -> Dict[str, Any]:
    """
    构建 AI 分析请求的 payload
    
    Args:
        chat_id: 群组ID
        chat_name: 群组名称
        chat_type: 群组类型
        day_start: 报告开始时间
        tz_name: 时区名称
        thread_id: 线程ID
        messages: 消息列表
        cfg: 配置对象
        batch_info: 批次信息（可选）
    
    Returns:
        AI 请求 payload
    """
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "chat_name": chat_name,
        "chat_type": chat_type,
        "date": day_start.date().isoformat(),
        "timezone": tz_name,
        "thread_id": thread_id,
        "messages": messages,
    }
    if batch_info:
        payload["batch_info"] = batch_info
    if cfg.ai_max_categories:
        payload["max_categories"] = cfg.ai_max_categories
    if cfg.ai_style:
        payload["style"] = cfg.ai_style
    return payload


def _sort_categories_by_priority(categories: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    按优先级对分类进行排序
    
    Args:
        categories: 分类列表
    
    Returns:
        排序后的分类列表
    """
    def get_priority(cat: Dict[str, Any]) -> int:
        cat_name = cat.get("name", "")
        return CATEGORY_PRIORITY.get(cat_name, DEFAULT_CATEGORY_PRIORITY)

    return sorted(categories, key=get_priority)


def _merge_categories(categories_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    合并相同名称的分类
    
    Args:
        categories_list: 分类列表
    
    Returns:
        合并后的分类字典，key 为分类名称，value 包含合并后的消息ID和摘要列表
    """
    category_map: Dict[str, Dict[str, Any]] = {}
    for cat in categories_list:
        name = cat.get("name") or "未命名分类"
        if name not in category_map:
            category_map[name] = {"message_ids": [], "summaries": []}
        
        summary = cat.get("summary") or ""
        if summary:
            category_map[name]["summaries"].append(summary)
        
        msg_ids = cat.get("messages") or []
        category_map[name]["message_ids"].extend(msg_ids)
    
    # 去重消息ID
    for name in category_map:
        category_map[name]["message_ids"] = list(dict.fromkeys(category_map[name]["message_ids"]))
    
    return category_map


def _format_category_output(
    category_map: Dict[str, Dict[str, Any]], 
    is_batch: bool = False,
    chat_link: Optional[str] = None,
    message_map: Optional[Dict[int, Dict[str, Any]]] = None,
) -> List[str]:
    """
    格式化分类输出
    
    Args:
        category_map: 合并后的分类字典
        is_batch: 是否为批次处理
        chat_link: 群组链接，用于生成消息链接
        message_map: 消息ID到消息详情的映射
    
    Returns:
        格式化的分类输出行列表
    """
    lines = []
    
    def get_priority(cat_name: str) -> int:
        return CATEGORY_PRIORITY.get(cat_name, DEFAULT_CATEGORY_PRIORITY)
    
    sorted_names = sorted(category_map.keys(), key=get_priority)
    
    lines.append("#### 📂 分类详情")
    if is_batch:
        lines.append("*（合并所有批次）*")
    lines.append("")
    
    # 收集所有消息ID用于原始引用部分
    all_message_refs: List[Tuple[int, str]] = []
    
    for name in sorted_names:
        cat_data = category_map[name]
        message_ids = cat_data["message_ids"]
        summaries = cat_data["summaries"]
        
        # 分类标题
        lines.append(f"##### 🔸 {name}")
        lines.append("")
        
        if summaries:
            if is_batch and len(summaries) > 1:
                # 合并多个批次的摘要
                combined_summary = " | ".join(summaries)
            else:
                combined_summary = summaries[0]
            
            summary_lines = combined_summary.split("\n")
            for line in summary_lines:
                if line.strip():
                    lines.append(f"{line}")
            lines.append("")
        
        # 收集该分类的所有消息引用
        for msg_id in message_ids:
            if message_map and msg_id in message_map:
                msg_info = message_map[msg_id]
                text = msg_info.get("text", "").strip()
                # 截取消息文本的前50个字符，如果太长则添加省略号
                if text:
                    display_text = text[:50] + ("..." if len(text) > 50 else "")
                    # 转义Markdown链接文本中的特殊字符，避免破坏链接语法
                    # 在链接文本中，] 和 ) 需要转义，否则会破坏链接语法
                    display_text = display_text.replace("]", "\\]").replace(")", "\\)")
                else:
                    display_text = "[媒体消息]"
                
                all_message_refs.append((msg_id, display_text))
            else:
                all_message_refs.append((msg_id, ""))
    
    # 添加原始引用分类
    if all_message_refs:
        lines.append("---")
        lines.append("")
        lines.append("#### 📎 原始消息引用")
        lines.append("")
        # 去重消息ID（保持顺序）
        seen_ids = set()
        unique_refs = []
        for msg_id, display_text in all_message_refs:
            if msg_id not in seen_ids:
                seen_ids.add(msg_id)
                unique_refs.append((msg_id, display_text))
        
        # 为每个消息生成链接
        for idx, (msg_id, display_text) in enumerate(unique_refs, 1):
            if chat_link:
                msg_link = f"{chat_link}/{msg_id}"
                if display_text:
                    # 转义特殊字符以避免破坏Markdown格式（表格中的 | 需要转义）
                    safe_text = display_text.replace("|", "\\|")
                    lines.append(f"{idx}. [{msg_id}]({msg_link})：{safe_text}")
                else:
                    lines.append(f"{idx}. [{msg_id}]({msg_link})")
            else:
                if display_text:
                    safe_text = display_text.replace("|", "\\|")
                    lines.append(f"{idx}. {msg_id}：{safe_text}")
                else:
                    lines.append(f"{idx}. {msg_id}")
        lines.append("")
    
    return lines


def _process_single_thread(
    thread_rows: List[sqlite3.Row],
    thread_id: int,
    cfg: Config,
    day_start: datetime,
    tz_name: str,
    chat_id: int,
    chat_name: Optional[str],
    chat_type: Optional[str],
    chat_link: Optional[str],
    message_map: Dict[int, Dict[str, Any]],
    conn: sqlite3.Connection,
) -> List[str]:
    """
    处理单个线程（不分批）
    
    Args:
        thread_rows: 线程消息行
        thread_id: 线程ID
        cfg: 配置对象
        day_start: 报告开始时间
        tz_name: 时区名称
        chat_id: 群组ID
        chat_name: 群组名称
        chat_type: 群组类型
        conn: 数据库连接
    
    Returns:
        处理结果行列表
    """
    lines = []
    messages = _convert_rows_to_messages(thread_rows, conn, chat_id)
    payload = _build_ai_payload(
        chat_id, chat_name, chat_type, day_start, tz_name, thread_id, messages, cfg
    )

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
        lines.append("⚠️ **AI 摘要生成失败**")
        lines.append("")
        lines.append(f"错误信息：{exc}")
        lines.append("")
        log.warning("AI summary failed for thread %s: %s", thread_id, exc)
        return lines

    overall = data.get("overall")
    if overall:
        lines.append("#### 📝 总览")
        lines.append("")
        lines.append(f"> {overall}")
        lines.append("")

    categories = data.get("categories") or []
    if categories:
        sorted_categories = _sort_categories_by_priority(categories)
        category_map = _merge_categories(sorted_categories)
        lines.extend(_format_category_output(category_map, is_batch=False, chat_link=chat_link, message_map=message_map))
    else:
        lines.append("*未返回分类结果*")
        lines.append("")

    return lines


def _process_thread_batch(
    thread_rows: List[sqlite3.Row],
    thread_id: int,
    cfg: Config,
    day_start: datetime,
    tz_name: str,
    chat_id: int,
    chat_name: Optional[str],
    chat_type: Optional[str],
    chat_link: Optional[str],
    message_map: Dict[int, Dict[str, Any]],
    conn: sqlite3.Connection,
) -> List[str]:
    """
    处理线程的批次（分批处理）
    
    Args:
        thread_rows: 线程消息行
        thread_id: 线程ID
        cfg: 配置对象
        day_start: 报告开始时间
        tz_name: 时区名称
        chat_id: 群组ID
        chat_name: 群组名称
        chat_type: 群组类型
        conn: 数据库连接
    
    Returns:
        处理结果行列表
    """
    lines = []
    total_messages = len(thread_rows)
    num_batches = (total_messages + cfg.ai_max_messages_per_batch - 1) // cfg.ai_max_messages_per_batch
    
    lines.append("#### ⚙️ 批次处理信息")
    lines.append("")
    lines.append(f"消息数量较多，将分成 **{num_batches}** 个批次处理（每批最多 {cfg.ai_max_messages_per_batch} 条）")
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

        messages = _convert_rows_to_messages(batch_rows, conn, chat_id)
        payload = _build_ai_payload(
            chat_id,
            chat_name,
            chat_type,
            day_start,
            tz_name,
            thread_id,
            messages,
            cfg,
            batch_info=f"批次 {batch_num}/{num_batches}，共 {total_messages} 条消息",
        )

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
            lines.append(f"⚠️ **批次 {batch_num} AI 摘要生成失败**：{exc}")
            lines.append("")
            log.warning("AI summary failed for thread %s batch %s: %s", thread_id, batch_num, exc)
            batch_failed = True
            continue

        batch_overall = data.get("overall")
        if batch_overall:
            all_overalls.append(f"批次 {batch_num}: {batch_overall}")

        batch_categories = data.get("categories") or []
        if batch_categories:
            all_categories.extend(batch_categories)

    # 合并所有批次的结果
    if batch_failed and not all_overalls and not all_categories:
        lines.append("⚠️ **所有批次处理失败**")
        lines.append("")
    else:
        if all_overalls:
            lines.append("#### 📝 总览（各批次摘要）")
            lines.append("")
            for overall in all_overalls:
                lines.append(f"- {overall}")
            lines.append("")

        if all_categories:
            category_map = _merge_categories(all_categories)
            lines.extend(_format_category_output(category_map, is_batch=True, chat_link=chat_link, message_map=message_map))

    return lines


def build_ai_summary_section(
    rows: List[sqlite3.Row],
    cfg: Config,
    day_start: datetime,
    chat_id: int,
    chat_name: Optional[str] = None,
    chat_type: Optional[str] = None,
    chat_link: Optional[str] = None,
    conn: Optional[sqlite3.Connection] = None,
    min_thread_messages: Optional[int] = None,
) -> List[str]:
    if not cfg.enable_ai_summary:
        return []

    lines = ["", "---", "", "## 🤖 智能话题摘要"]

    if not cfg.ai_api_base:
        lines.append("⚠️ **AI 摘要未生成**：缺少 `ai_api_base` 配置")
        log.warning("AI summary enabled but ai_api_base not set.")
        return lines

    if not cfg.ai_api_key:
        lines.append("⚠️ **AI 摘要未生成**：缺少 `ai_api_key` 配置")
        log.warning("AI summary enabled but ai_api_key not set.")
        return lines
    
    if not conn:
        lines.append("⚠️ **AI 摘要未生成**：缺少数据库连接")
        log.warning("AI summary enabled but database connection not provided.")
        return lines

    # 确定使用的最小消息数量阈值：优先使用群组特定配置，否则使用全局默认值
    threshold = min_thread_messages if min_thread_messages is not None else MIN_THREAD_MESSAGES

    # 按 thread_id 分组消息
    threads = _group_messages_by_thread(rows)

    # 过滤掉消息数量小于阈值的线程
    valid_threads = {tid: msgs for tid, msgs in threads.items() if len(msgs) >= threshold}

    if not valid_threads:
        lines.append(f"*没有符合条件的线程（消息数量 >= {threshold}）*")
        return lines

    lines.append(f"**共 {len(valid_threads)} 个线程符合分析条件**")
    lines.append("")

    tz_name = getattr(cfg.timezone, "key", None) or str(cfg.timezone)

    # 创建消息ID到消息详情的映射（包括文本内容）
    message_map: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        message_map[row["message_id"]] = {
            "text": row["text"] or "",
            "user_id": row["user_id"],
            "username": row["username"],
        }

    # 为每个符合条件的线程分别调用 AI 分析
    for thread_id, thread_rows in sorted(valid_threads.items(), key=lambda x: len(x[1]), reverse=True):
        thread_name = "顶层消息" if thread_id == TOP_THREAD_ID else f"线程 {thread_id}"
        total_messages = len(thread_rows)
        lines.append(f"### 💭 {thread_name}（{total_messages} 条消息）")
        lines.append("")

        # 如果消息数量超过阈值，进行分段处理
        if total_messages > cfg.ai_max_messages_per_batch:
            batch_lines = _process_thread_batch(
                thread_rows, thread_id, cfg, day_start, tz_name, chat_id, chat_name, chat_type, chat_link, message_map, conn
            )
            lines.extend(batch_lines)
        else:
            # 消息数量不多，直接处理
            single_lines = _process_single_thread(
                thread_rows, thread_id, cfg, day_start, tz_name, chat_id, chat_name, chat_type, chat_link, message_map, conn
            )
            lines.extend(single_lines)

        lines.append("")

    return lines
