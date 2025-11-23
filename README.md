# Telethon 群消息日汇总（增量拉取方案）

基于 Telethon 的轻量脚本：按天增量拉取指定群消息，存入 SQLite，再生成 Markdown 日报（可发给自己或存本地）。

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
5) 如需定时，使用 cron/launchd/Task Scheduler 调用 `--pull --report` 即可。

## 后续
- 实现增量抓取、SQLite 写入、Markdown 报表、发送到 “Saved Messages”。
- 媒体下载策略：仅下载小于 `max_media_mb`（默认 10MB）的非视频/非语音文件，路径在 `data/media/`。
- 可按需扩展：关键词摘要、Top 活跃用户、错误重试等。
