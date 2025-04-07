
import os
import logging
import asyncio

from flask import Flask, request
from aiogram import Bot, Dispatcher, types

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
RENDER_EXTERNAL_HOSTNAME = os.getenv("RENDER_EXTERNAL_HOSTNAME")
WEBHOOK_URL = f"https://{RENDER_EXTERNAL_HOSTNAME}/webhook"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

app = Flask(__name__)

@dp.message_handler(commands=['start'])
async def handle_start(message: types.Message):
    await message.reply("✅ Бот работает через Webhook!")

@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update(**request.json)
    Bot.set_current(bot)  # <-- добавь вот эту строку
    asyncio.run(dp.process_update(update))
    return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))

    # Установка webhook перед запуском сервера
    asyncio.run(bot.set_webhook(WEBHOOK_URL))
    print("✅ Webhook установлен:", WEBHOOK_URL)

    app.run(host="0.0.0.0", port=port)
