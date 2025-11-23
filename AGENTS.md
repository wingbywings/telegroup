# Repository Guidelines

## 项目结构与模块组织
- `src/`: 主逻辑（Telethon 增量拉取、SQLite 写入、日报生成）。入口 `src/main.py`。
- `config/`: 配置模板与会话文件位置。实际配置使用 `config/config.json`（已在 `.gitignore`），示例见 `config/config.example.json`。
- `data/`: SQLite 数据库、last_id 记录、可选媒体下载目录 `data/media/`（已忽略）。
- `reports/`: 每日日报 Markdown 输出。
- `README.md`: 使用说明；`requirements.txt`: Python 依赖。

## 构建、测试与开发命令
- 创建虚拟环境并安装依赖：`python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
- 首次登录（生成 session）：`python src/main.py --init-session`
- 拉取增量并生成日报：`python src/main.py --pull --report`
- 仅拉取：`python src/main.py --pull`；仅日报（需已有数据）：`python src/main.py --report`
- 计划任务：将上述命令放入 cron/launchd，建议每日 00:05。

## 编码风格与命名规范
- 语言：Python 3.10+，PEP8 风格，4 空格缩进。
- 配置：JSON，键名用小写加下划线。
- 日志：使用标准库 `logging`，INFO 级别默认。
- 数据库：SQLite，表 `messages` 以 `(chat_id, message_id)` 为主键，新增列请加轻量迁移逻辑。

## 测试规范
- 当前无测试框架；添加新逻辑时建议使用 `pytest` 并放在 `tests/`。
- 为复杂统计函数添加单元测试，命名 `test_<module>.py`，函数名 `test_<case>`。
- 手动验证：运行 `--pull` 后检查 `data/messages.db` 与 `reports/*.md` 输出。

## 提交与 Pull Request 规范
- Commit 信息需简洁明了，明确描述改动内容，例如：`feat: 新增媒体下载限制`、`fix: 修复时区回退问题`。
- PR 建议包含：变更摘要、测试方式（命令输出或说明）、配置/迁移影响（如新增 config 字段、DB 列）。
- 避免提交包含真实的 `config/config.json`、`.session`、`data/*.db`、`reports/*.md`、`data/media/`。

## 安全与配置提示
- API ID/Hash、手机号、session 文件应保持私密，配置文件与 session 已在 `.gitignore`。
- 媒体下载默认仅非视频/语音且小于 `max_media_mb`，目录为 `data/media/`；可在配置中关闭或调整阈值。
- 如需改变时区、群 ID 或存储路径，更新 `config/config.json` 并确保目录存在。
