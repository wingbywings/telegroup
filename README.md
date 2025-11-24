# Telegram 群消息智能分析系统

该系统基于 Telethon 库，实现Telegram指定群组聊天消息的统计分析。可以按天增量拉取指定群消息，接入AI智能分析，最终生成 Markdown 日报。

## 目录结构
- `src/`：主脚本与工具
- `config/`：配置文件（拷贝 example 后填写）
- `data/`：SQLite 存储
- `reports/`：日报输出

## 快速开始
1) 创建虚拟环境并安装依赖：
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   ```
2) 复制配置模板并填写你的信息：
   ```bash
   cp config/config.example.json config/config.json
   # 修改 api_id、api_hash、phone、chat_id 等
   ```
3) 首次登录会触发 Telethon 验证码，生成会话文件：
   ```bash
   python src/main.py --init-session
   ```
4) 手动拉取当天增量并生成日报（示例）：
   ```bash
   python src/main.py --pull --report
   ```
5) 如需定时调用，可使用 cron/launchd/Task Scheduler 调用 `--pull --report` 即可。

## 配置字段说明（参考 `config/config.example.json`）
- `api_id` / `api_hash` / `phone`：Telegram API 凭证与手机号，手机号请包含国家码。
- `session_path`：Telethon 会话文件路径，默认 `config/telethon.session`。
- `db_path` / `last_id_path` / `report_dir`：SQLite 数据库、增量记录与日报目录，可按需调整路径（确保目录存在）。
- `timezone`：日报按此时区切割日期；`pull_days` 控制向前回溯的天数。
- `send_report_to_me`：是否将生成的日报发到 Saved Messages。
- `download_media` / `media_dir` / `max_media_mb`：控制是否下载媒体、存储目录与大小上限（仅下载非视频/非语音）。
- `enable_ai_summary` 与 `ai_*`：可选 AI 归类/摘要配置，关闭则不会请求外部 API。
- `chats`：待拉取的群列表，提供 `chat_id` 或 `chat_link` 即可，`name` 用于标记；`chat_type`、`min_thread_messages`、`enable_thread_classification` 控制线程分类策略。

## 后续
- 实现增量抓取、SQLite 写入、Markdown 报表、发送到 “Saved Messages”。
- 媒体下载策略：仅下载小于 `max_media_mb`（默认 10MB）的非视频/非语音文件，路径在 `data/media/`。
- 可按需扩展：关键词摘要、Top 活跃用户、错误重试等。
