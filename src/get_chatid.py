from telethon import TelegramClient

api_id = 32307061
api_hash = "2ef2c1bf1deca360bae00ec331de3711"

client = TelegramClient("session", api_id, api_hash)

async def main():
    entity = await client.get_entity("https://t.me/nofx_dev_community")  # 或者整个链接
    real_id = entity.id
    chat_id = -1000000000000 + real_id if real_id < 0 else -1000000000000 + real_id  # 通常只需加 -100 前缀
    print("entity.id:", entity.id)
    print("chat_id (for API):", f"-100{entity.id}")

client.start()
client.loop.run_until_complete(main())
