from telethon import TelegramClient
from telethon import events
from config import Config


class Telegram:
    _client: TelegramClient | None = None
    _message_callback = None

    @classmethod
    async def start(cls):
        if cls._client:
            return  # already started

        cls._client = TelegramClient(
            "bot",
            api_id=Config.API_ID,
            api_hash=Config.API_HASH,
        )

        await cls._client.start(bot_token=Config.BOT_TOKEN)  # type: ignore
        print("Telegram Bot started...")
        await cls._client.send_message(Config.USER_ID, "MarketBot started!")

    @classmethod
    async def send_message(cls, message: str):
        if not cls._client:
            return
        await cls._client.send_message(Config.USER_ID, message)

    @classmethod
    def set_message_callback(cls, callback):
        cls._message_callback = callback

    @classmethod
    async def listen_messages(cls):
        if not cls._client:
            raise RuntimeError("Telegram client not started")

        @cls._client.on(events.NewMessage(chats=Config.USER_ID))
        async def handler(event):
            if cls._message_callback:
                await cls._message_callback(event.message.message)

        await cls._client.run_until_disconnected()  # type: ignore
