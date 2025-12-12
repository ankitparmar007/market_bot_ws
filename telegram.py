from telethon import TelegramClient
from telethon import events
from config import Config


class Telegram:
    __client: TelegramClient = TelegramClient(
        "bot", api_id=Config.API_ID, api_hash=Config.API_HASH
    )
    __message_callback = None

    @classmethod
    async def start(cls):
        await cls.__client.start(bot_token=Config.BOT_TOKEN)  # type: ignore
        print("Telegram Bot started...")
        await cls.__client.send_message(Config.USER_ID, "MarketBot started!")

    @classmethod
    async def send_message(cls, message: str):
        print(f"Sending message: {message}")
        await cls.__client.send_message(Config.USER_ID, message)

    @classmethod
    def set_message_callback(cls, callback):
        """
        callback: async function(message: str)
        """
        cls.__message_callback = callback

    @classmethod
    async def listen_messages(cls):
        @cls.__client.on(events.NewMessage(chats=Config.USER_ID))
        async def handler(event):
            text = event.message.message
            if cls.__message_callback:
                await cls.__message_callback(text)

        # IMPORTANT: keep Telethon alive
        await cls.__client.run_until_disconnected()  # type: ignore
