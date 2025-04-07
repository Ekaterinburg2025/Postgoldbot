import logging
import asyncio
from flask import Flask, request
from aiogram import Bot, Dispatcher, types
import os

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Bot init
BOT_TOKEN = os.getenv("BOT_TOKEN") or "ТУТ_ТВОЙ_ТОКЕН"  # <-- НЕ ЗАБУДЬ УКАЗАТЬ ТОКЕН
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# Flask app
app = Flask(__name__)

@app.route("/webhook", methods=["POST"])
def webhook():
    json_data = request.get_json(force=True)
    logger.info("📩 Пришёл апдейт: %s", json_data)

    try:
        update = types.Update(**json_data)
        asyncio.run(dp.process_update(update))
    except Exception as e:
        logger.exception("❌ Ошибка при обработке webhook: %s", e)

    return "OK", 200

@app.route("/", methods=["GET"])
def root():
    return "Бот запущен и ждёт webhook!", 200

# Простейший хендлер для теста
@dp.message()
async def handle_message(message: types.Message):
    await message.answer("👋 Привет! Я жив!")

if __name__ == "__main__":
    # Установка webhook (по необходимости)
    WEBHOOK_URL = os.getenv("WEBHOOK_URL") or "https://postgoldbot.onrender.com/webhook"
    asyncio.run(bot.set_webhook(WEBHOOK_URL))
    print(f"✅ Webhook установлен: {WEBHOOK_URL}")

    app.run(host="0.0.0.0", port=10000)
