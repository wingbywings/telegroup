#!/usr/bin/env python3
"""测试时区处理和时间范围计算"""
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# 模拟配置
config_tz = ZoneInfo("Asia/Shanghai")
pull_days = 2

# 模拟当前时间（配置时区）
now_cfg = datetime.now(tz=config_tz)
cutoff = now_cfg - timedelta(days=pull_days)

print("=" * 60)
print("时区处理测试")
print("=" * 60)
print(f"配置时区: {config_tz}")
print(f"当前时间（配置时区）: {now_cfg}")
print(f"截止时间（配置时区）: {cutoff}")
print(f"截止时间（UTC）: {cutoff.astimezone(timezone.utc)}")
print()

# 模拟 Telethon 返回的消息时间（通常是 UTC）
# 测试几个边界情况
test_cases = [
    ("刚刚在截止时间之前（UTC）", cutoff.astimezone(timezone.utc) - timedelta(seconds=1)),
    ("正好在截止时间（UTC）", cutoff.astimezone(timezone.utc)),
    ("刚刚在截止时间之后（UTC）", cutoff.astimezone(timezone.utc) + timedelta(seconds=1)),
    ("2天前（UTC）", datetime.now(tz=timezone.utc) - timedelta(days=2)),
    ("3天前（UTC）", datetime.now(tz=timezone.utc) - timedelta(days=3)),
]

def normalize_dt(dt: datetime, tz: timezone) -> datetime:
    """模拟 normalize_dt 函数"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz)

print("测试消息时间处理：")
print("-" * 60)
for desc, msg_date_utc in test_cases:
    # 模拟 normalize_dt 处理
    msg_dt_normalized = normalize_dt(msg_date_utc, config_tz)
    should_include = msg_dt_normalized >= cutoff
    
    print(f"\n{desc}:")
    print(f"  消息时间（UTC）: {msg_date_utc}")
    print(f"  消息时间（配置时区）: {msg_dt_normalized}")
    print(f"  截止时间（配置时区）: {cutoff}")
    print(f"  是否包含: {should_include} {'✓' if should_include else '✗'}")
    print(f"  时间差: {msg_dt_normalized - cutoff}")

print()
print("=" * 60)
print("潜在问题检查：")
print("=" * 60)

# 检查是否存在时区不一致的问题
print("\n1. cutoff 计算使用的是配置时区的当前时间")
print(f"   cutoff = datetime.now(tz={config_tz}) - timedelta(days={pull_days})")
print(f"   结果: {cutoff}")

print("\n2. msg.date 从 Telethon 获取，通常是 UTC")
print("   需要确认 Telethon 返回的 msg.date 的时区信息")

print("\n3. normalize_dt 函数处理：")
print("   - 如果 msg.date.tzinfo 为 None，设置为 UTC")
print("   - 然后转换为配置时区")
print("   - 这应该是正确的")

print("\n4. 比较 msg_dt < cutoff 时，两者都在配置时区")
print("   理论上应该没问题，但需要验证")

print("\n5. 建议添加日志输出以验证：")
print("   - 记录 cutoff 时间（UTC 和配置时区）")
print("   - 记录每条消息的时间（UTC 和配置时区）")
print("   - 记录被过滤的消息数量")

