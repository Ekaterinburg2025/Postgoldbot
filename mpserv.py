import os
import logging
from flask import Flask, request
from aiogram import Bot, Dispatcher, types
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Update
import asyncio

TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())

# 👇 Регистрируем бота в диспетчере
dp["bot"] = bot

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        json_data = request.get_json(force=True)
        update = Update.model_validate(json_data)  # aiogram 3.x
        asyncio.run(dp.feed_update(bot, update))
    except Exception as e:
        logging.exception("Ошибка при обработке webhook: %s", e)
    return "OK", 200

# 👋 Простой хендлер
@dp.message()
async def echo_handler(message: types.Message):
    await message.answer("✅ Бот работает!")

if __name__ == "__main__":
    WEBHOOK_HOST = os.getenv("WEBHOOK_URL") or "https://postgoldbot.onrender.com"
    WEBHOOK_PATH = "/webhook"
    WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

    async def on_startup():
        await bot.set_webhook(WEBHOOK_URL)
        print(f"✅ Webhook установлен: {WEBHOOK_URL}")

    asyncio.run(on_startup())
    app.run(host="0.0.0.0", port=10000)
