import logging
import json
import re
from typing import Any, Dict, List, Optional

import httpx


log = logging.getLogger(__name__)


class AISummaryError(RuntimeError):
    pass


def _extract_json_from_content(content: str) -> Dict[str, Any]:
    """
    从模型返回的内容中提取 JSON 对象。
    
    处理以下情况：
    1. 纯 JSON 字符串
    2. 包含 markdown 代码块的 JSON（```json ... ``` 或 ``` ... ```）
    3. 包含前后空白字符或换行
    4. 包含其他文本前缀/后缀（尝试提取第一个有效的 JSON 对象）
    
    Args:
        content: 模型返回的原始内容
        
    Returns:
        解析后的 JSON 字典
        
    Raises:
        AISummaryError: 如果无法提取有效的 JSON
    """
    if not content:
        raise AISummaryError("content is empty")
    
    # 去除前后空白字符
    content = content.strip()
    
    # 情况1: 尝试直接解析（纯 JSON）
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    
    # 情况2: 处理 markdown 代码块格式（```json ... ``` 或 ``` ... ```）
    # 匹配 ```json ... ``` 或 ``` ... ```
    code_block_pattern = r'```(?:json)?\s*\n?(.*?)\n?```'
    match = re.search(code_block_pattern, content, re.DOTALL)
    if match:
        json_str = match.group(1).strip()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass
    
    # 情况3: 尝试从文本中提取 JSON 对象（使用括号匹配）
    # 从第一个 { 开始，尝试找到匹配的 }
    start_idx = content.find('{')
    if start_idx >= 0:
        brace_count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_idx, len(content)):
            char = content[i]
            
            # 处理转义字符
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            # 处理字符串边界
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            # 只在非字符串状态下计算括号
            if not in_string:
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        json_str = content[start_idx:i+1]
                        try:
                            return json.loads(json_str)
                        except json.JSONDecodeError:
                            break
    
    # 情况4: 尝试查找 JSON 数组（如果返回的是数组格式）
    # 从第一个 [ 开始，尝试找到匹配的 ]
    start_idx = content.find('[')
    if start_idx >= 0:
        bracket_count = 0
        in_string = False
        escape_next = False
        
        for i in range(start_idx, len(content)):
            char = content[i]
            
            # 处理转义字符
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            # 处理字符串边界
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            # 只在非字符串状态下计算括号
            if not in_string:
                if char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
                    if bracket_count == 0:
                        json_str = content[start_idx:i+1]
                        try:
                            result = json.loads(json_str)
                            # 如果解析成功但返回的是数组，包装成字典
                            if isinstance(result, list):
                                return {"categories": result}
                            return result
                        except json.JSONDecodeError:
                            break
    
    # 如果所有方法都失败，抛出错误
    raise AISummaryError(
        f"无法从模型返回内容中提取有效的 JSON。内容预览: {content[:200]}..."
    )


def call_chat_analysis(
    api_base: str,
    api_key: str,
    payload: Dict[str, Any],
    model: str = "grok-beta",
    timeout: float = 120.0,
) -> Dict[str, Any]:
    """
    Call x.ai-compatible chat/completions and ask model to return structured JSON.
    """
    url = api_base.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # 从 payload 中提取群组类型和名称
    chat_type = payload.get("chat_type")  # "crypto" 或 "tech" 或 "news" 或 None
    chat_name = payload.get("chat_name", "")
    
    # 构建系统提示词
    sys_prompt_parts = [
        "你是一个专业的群聊消息分析助手，擅长从大量聊天记录中提取真正有价值的结构性信息。",
        "",
        "## 核心理念：三类信息分类",
        "群聊消息通常包含三类信息，你需要优先抓取前两类：",
        "",
        "1. **事件类**（最重要）：项目发布、版本更新、合约漏洞、主网停机、钱包被攻击、官方公告等",
        "2. **观点类**（次要）：成员观点、市场判断、技术讨论等，可作为情绪参考",
        "3. **噪音类**（忽略）：闲聊、无意义争论、表情包、情绪化言论等（约占80%-95%）",
        "",
        "## 筛选策略",
    ]
    
    # 根据群组类型添加不同的筛选标准
    if chat_type == "crypto":
        sys_prompt_parts.extend([
            "这是一个**加密货币群**，重点关注以下6大类信号（一旦出现必须记录）：",
            "1. 官方公告 / 版本更新",
            "2. 重要钱包、交易所、大户的突发事件",
            "3. 黑客攻击 / 合约漏洞",
            "4. 新版本上线 / 主网升级",
            "5. 合作伙伴、融资、监管类新闻",
            "6. 社区讨论度突然升高（高热词）",
            "",
            "其他观点、吹水、争吵 → 全部略过。",
        ])
    elif chat_type == "tech":
        sys_prompt_parts.extend([
            "这是一个**技术项目群**，重点关注以下结构性信息（只记项目的进化轨迹）：",
            "1. 版本变更（commit / tag / PR）",
            "2. 核心开发者的说明 / 解释",
            "3. 新功能的设计讨论",
            "4. Bug 报告",
            "5. Bug 修复 & Patch 发布",
            "6. 路线图变化",
        ])
    elif chat_type == "news":
        sys_prompt_parts.extend([
            "这是一个**新闻类群组**，重点关注以下重要新闻和信息：",
            "1. 重大新闻事件（突发新闻、重要政策、社会事件等）",
            "2. 新闻要点和关键信息（时间、地点、人物、事件核心）",
            "3. 新闻来源和可信度（官方发布、权威媒体、社交媒体等）",
            "4. 新闻背景和上下文（历史背景、相关事件、影响范围）",
            "5. 后续发展和追踪（事件进展、官方回应、后续报道）",
            "6. 重要观点和分析（专家解读、深度分析、不同观点）",
            "",
            "注意：",
            "- 优先提取真实、可验证的新闻事件",
            "- 区分新闻事实和观点评论",
            "- 忽略重复转发、无来源消息、谣言和未经证实的信息",
            "- 关注新闻的时间线和因果关系",
        ])
    else:
        sys_prompt_parts.extend([
            "请根据消息内容自动判断群组类型：",
            "- 如果涉及加密货币、交易、DeFi、NFT等 → 按加密货币群标准筛选",
            "- 如果涉及代码、开发、技术讨论、项目开发 → 按技术项目群标准筛选",
            "- 如果涉及新闻、时事、社会事件、媒体报道等 → 按新闻类群组标准筛选",
            "- 如果无法判断，优先提取事件类和重要观点类信息",
        ])
    
    sys_prompt_parts.extend([
        "",
        "## 对话分析模式（重要）",
        "**请按照对话模式分析消息，而不是孤立地分析每条消息：**",
        "",
        "1. **时间顺序理解**：",
        "   - 消息已按时间顺序排列（ts字段），请严格按照时间先后顺序理解对话流程",
        "   - 例如：2025-11-24 09:00:00 的消息应该在 2025-11-24 12:05:44 的消息之前被理解",
        "   - 理解消息之间的因果关系：前面的消息可能引发后续的讨论或回应",
        "",
        "2. **对话逻辑推理**：",
        "   - 识别对话的起始点：谁提出了问题或话题",
        "   - 追踪对话的发展：如何从初始话题展开讨论",
        "   - 理解回复关系：利用 reply_to 字段理解消息之间的回复关系",
        "   - 识别对话的转折点：话题何时发生变化或深入",
        "   - 理解对话的结论：讨论如何结束或达成共识",
        "",
        "3. **上下文关联**：",
        "   - 将每条消息放在整个对话的上下文中理解",
        "   - 识别消息之间的关联性：哪些消息是对同一话题的回应",
        "   - 理解对话的连贯性：前后消息如何形成完整的讨论",
        "",
        "4. **避免孤立分析**：",
        "   - 不要单独分析每条消息，而要理解整个对话的脉络",
        "   - 注意消息之间的引用关系（replied_message字段）",
        "   - 理解对话的整体意图和目的，而不是单个消息的字面意思",
        "",
        "## 输出格式要求",
        "返回一个JSON对象，包含以下字段：",
        "- overall（字符串）：总体摘要，简要描述本线程的核心内容和对话流程",
        "- categories（数组）：分类列表，每个分类包含 {name, summary, messages}",
        "",
        "## 分类命名规范（按重要性排序）",
        "请使用以下标准分类名称，确保信息结构化：",
        "1. **关键事件**：重要事件、突发事件、官方公告等",
        "2. **技术更新/开发者动态**：版本发布、功能更新、开发者说明等",
        "3. **潜在风险&预警**：安全漏洞、攻击事件、风险提示等",
        "4. **市场/社区情绪**：市场观点、社区情绪趋势、有价值观点汇总",
        "5. **需要关注的后续行动**：待办事项、后续计划、需要跟进的内容",
        "",
        "## summary 字段格式要求",
        "每个分类的 summary 应包含以下信息（如果相关）：",
        "- 事件/更新的简要描述（结合对话上下文）",
        "- 对话的发展过程：如何从初始消息发展到最终结论",
        "- 原始消息的关键内容（引用关键信息）",
        "- 补充信息（如群内其他成员的补充说明、解释等）",
        "",
        "示例格式：",
        '"Solana 主网上出现短暂拥堵\\n- 对话流程：@dev_jason 首先发布节点日志提到"3 分钟无法处理区块"，随后群内 validator @mark 解释原因是 stake pool rebalancing 导致延迟，最后 @admin 确认问题已解决\\n- 原始消息：@dev_jason 发布节点日志\\n- 补充：@mark 的技术解释和 @admin 的确认"',
        "",
        "如果某个分类没有相关内容，可以省略。",
        "在messages字段中仅保留消息id（数字）以便溯源。",
        "请用中文回答。",
    ])
    
    sys_prompt = "\n".join(sys_prompt_parts)
    
    # 构建用户提示词
    user_prompt_parts = [
        "请分析以下群聊消息，严格按照上述要求提取结构性信息。",
        "",
        "**重要：请按照对话模式分析，而不是孤立地分析每条消息：**",
        "",
        "1. **时间顺序**：消息已按时间顺序排列（ts字段），请严格按照时间先后顺序理解对话流程。",
        "   例如：2025-11-24 09:00:00 的消息应该在 2025-11-24 12:05:44 的消息前面被理解。",
        "",
        "2. **对话逻辑**：",
        "   - 识别对话的起始、发展和结论",
        "   - 理解消息之间的因果关系和回复关系（注意 reply_to 和 replied_message 字段）",
        "   - 推理出对话的整体逻辑和连贯性",
        "   - 将每条消息放在整个对话的上下文中理解",
        "",
        "3. **提取信息**：",
        "   - 优先提取事件类信息（项目发布、漏洞、攻击、更新等）",
        "   - 忽略噪音类信息（闲聊、无意义争论、表情包等）",
        "   - 观点类信息仅保留有价值的市场判断或技术观点",
        "   - 如果消息中没有任何有价值信息，overall 可以说明'本线程主要为闲聊，无重要信息'",
        "",
        "输入数据：",
    ]
    
    user_prompt = "\n".join(user_prompt_parts) + "\n" + json.dumps(payload, ensure_ascii=False, indent=2)

    data: Dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
    }

    log.info("AI request prompt (system): %s", sys_prompt)
    log.info("AI request prompt (user): %s", user_prompt)

    try:
        resp = httpx.post(url, headers=headers, json=data, timeout=timeout)
    except (
        httpx.ReadTimeout,
        httpx.ConnectTimeout,
        httpx.WriteTimeout,
        httpx.PoolTimeout,
        httpx.TimeoutException,
    ) as exc:
        raise AISummaryError(f"请求超时（{timeout}秒），请尝试增加 ai_timeout 配置或检查网络连接") from exc
    except Exception as exc:  # pragma: no cover - network errors
        raise AISummaryError(f"请求失败: {exc}") from exc

    log.info("AI raw response status=%s body=%s", resp.status_code, resp.text)

    if resp.status_code >= 400:
        raise AISummaryError(f"bad status {resp.status_code}: {resp.text}")

    try:
        body = resp.json()
    except Exception as exc:
        raise AISummaryError(f"invalid JSON response: {exc}") from exc

    content: Optional[str] = None
    try:
        choices: List[Dict[str, Any]] = body.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content")
    except Exception:
        content = None

    if not content:
        raise AISummaryError("no content returned from model")

    try:
        return _extract_json_from_content(content)
    except AISummaryError:
        # 重新抛出 AISummaryError，保持原始错误信息
        raise
    except Exception as exc:
        raise AISummaryError(f"解析 JSON 时发生错误: {exc}，内容预览: {content[:200]}...") from exc
