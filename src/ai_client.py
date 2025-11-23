import logging
import json
from typing import Any, Dict, List, Optional

import httpx


log = logging.getLogger(__name__)


class AISummaryError(RuntimeError):
    pass


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

    sys_prompt = (
        "你是一个对群聊消息进行摘要的助手。"
        "给定聊天的元数据和消息，请返回一个包含以下键的JSON对象："
        "overall（字符串），categories（{name, summary, messages}组成的列表）。"
        "请简明扼要；如有提供max_categories，则分类不超过该数量。"
    )
    user_prompt = (
        "请分析以下群聊消息，并根据要求生成前述的JSON对象。"
        "请用中文回答。"
        "在messages字段中仅保留消息id以便溯源。"
        "输入JSON：\n" + json.dumps(payload, ensure_ascii=False)
    )

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
        return json.loads(content)
    except Exception as exc:
        raise AISummaryError(f"model returned non-JSON content: {content}") from exc
