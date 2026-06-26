import os
import time
import pymongo
from pymongo import MongoClient
import threading
import re
import html
from bson.objectid import ObjectId
from urllib.parse import quote
import requests


# 👇 УНИВЕРСАЛЬНЫЙ КАССИР CRYPTOBOT (ДЛЯ РЕКЛАМЫ И ШТРАФОВ) 👇
def get_crypto_pay_url(custom_payload, amount_stars, description, asset=None):
    import os
    import requests
    
    amount_rub = int(amount_stars * 1.8)
    API_TOKEN = os.getenv("CRYPTO_TOKEN")
    
    if not API_TOKEN:
        print("❌ ОШИБКА: Токен CRYPTO_TOKEN не найден!", flush=True)
        return None

    url = "https://pay.crypt.bot/api/createInvoice"
    
    headers = {
        "Crypto-Pay-API-Token": API_TOKEN,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    payload = {
        "currency_type": "fiat",
        "fiat": "RUB",
        "amount": str(amount_rub), 
        "payload": custom_payload,
        "description": description
    }
    
    if asset:
        payload["asset"] = asset
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        res = response.json()
        
        if res.get("ok"): 
            return res["result"]["mini_app_invoice_url"] # 💥 Запускаем красивое Mini-App окно!
    except Exception as e: 
        print(f"❌ Ошибка связи с CryptoBot: {e}", flush=True)
        
    return None

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

ATTEMPTS_PER_PAGE = 10
POSTS_PER_PAGE = 10

import pytz
from pytz import timezone
from datetime import datetime, timedelta

def now_ekb():
    return datetime.now(timezone('Asia/Yekaterinburg'))

ekb_tz = pytz.timezone('Asia/Yekaterinburg')
today = now_ekb().astimezone(ekb_tz).date()

def to_ekb_str(dt, format_str='%d.%m.%Y %H:%M'):
    """Принудительно переводит время из MongoDB (UTC) обратно в ЕКБ для админки"""
    if dt is None: return "неизвестно"
    if dt.tzinfo is None: # Если дата из Mongo (она всегда UTC)
        dt = pytz.utc.localize(dt)
    return dt.astimezone(pytz.timezone('Asia/Yekaterinburg')).strftime(format_str)

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

from flask import Flask, request, Response

# Собственная функция для экранирования спецсимволов Markdown
def escape_md(text):
    escape_chars = r'\_*[]()~`>#+-=|{}.!'
    for ch in escape_chars:
        text = text.replace(ch, f"\\{ch}")
    return text

# 👇 УМНЫЙ ФИЛЬТР СТОП-СЛОВ (КРАСНАЯ ЗОНА + ЧЕРНАЯ ЗОНА) 👇
def check_stop_words(text, ignore_black_zone=False):
    if not text: return False, None
    text_lower = text.lower()
    dict_data = db['settings'].find_one({"_id": "skynet_dictionary"}) or {}
    
    # 1. КРАСНАЯ ЗОНА (Проверяем ВСЕГДА у всех)
    for w in dict_data.get("red", []):
        pattern = w.get("pattern", rf"\b{w['word']}\b")
        try:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return True, w['word']
        except: pass

    # 2. ЧЕРНАЯ ЗОНА (Пропускаем, если у юзера VIP-тариф)
    if not ignore_black_zone:
        for w in dict_data.get("black", []):
            pattern = w.get("pattern", rf"{w['word']}")
            try:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    match = re.search(pattern, text_lower, re.IGNORECASE)
                    return True, match.group(0)
            except: pass

    return False, None

def escape_html(text):
    """
    Экранирует спецсимволы для HTML.
    """
    if not isinstance(text, str):
        text = str(text)
    return html.escape(text)

# Получаем токен из переменной окружения
TOKEN = os.getenv('BOT_TOKEN')
bot = telebot.TeleBot(TOKEN)

# Создаём Flask-приложение
app = Flask(__name__)

# ==================== БАЗА ДАННЫХ MONGODB ====================
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise ValueError("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задана переменная MONGO_URI на сервере!")

mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client['elite_bot_db'] # Подключаемся к ЕДИНОЙ базе Скайнета!

# Коллекции, которые нам понадобятся:
ad_subs_collection = db['ad_subscriptions'] # НОВАЯ: Подписки на рекламу
ad_posts_collection = db['ad_posts']        # НОВАЯ: Опубликованные посты
autopost_queue = db['autopost_queue']       # НОВАЯ: Очередь автопубликаций
promocodes_collection = db['promocodes']    # СУЩЕСТВУЮЩАЯ ИЗ СКАЙНЕТА
admins_collection = db['admins']            # НОВАЯ: Список админов
# =============================================================

# ADMIN ID (ваш ID)
ADMIN_CHAT_ID = -1002196190507

# 🔒 Вечные (статичные) админы
CORE_ADMINS = [479938867, 7235010425]

import traceback

def send_error_to_admin(e, context=""):
    """Отправляет ошибку напрямую в админский чат"""
    error_trace = traceback.format_exc()
    error_msg = f"🚨 <b>КРИТИЧЕСКАЯ ОШИБКА ({context})</b>\n\n<pre>{escape_html(error_trace[-800:])}</pre>"
    try:
        bot.send_message(ADMIN_CHAT_ID, error_msg, parse_mode="HTML")
    except:
        pass

# Самоочистка базы данных (чтобы сервер никогда не переполнился)
db['failed_attempts'].create_index("time", expireAfterSeconds=2592000) # Удаляет логи отказов через 30 дней
ad_posts_collection.create_index("time", expireAfterSeconds=7776000) # Удаляет историю постов через 90 дней

def log_failed_attempt(user_id, network, city, reason):
    """Логирует неудачную попытку публикации напрямую в MongoDB."""
    db['failed_attempts'].insert_one({
        "user_id": user_id,
        "network": network,
        "city": city,
        "time": now_ekb(),
        "reason": reason
    })
    print(f"[FAILED] {user_id}, {network}, {city}, {reason}")

def add_post_to_history(user_id, user_name, network, city, chat_id, message_id, deleted=False, deleted_by=None):
    """Сохраняет пост в архив MongoDB."""
    ad_posts_collection.insert_one({
        "user_id": user_id,
        "user_name": user_name,
        "network": network,
        "city": city,
        "time": now_ekb(),
        "chat_id": chat_id,
        "message_id": message_id,
        "deleted": deleted,
        "deleted_by": deleted_by
    })

# 🧠 Автогенерация all_cities на основе chat_ids_* и учёта особых случаев

# ==================== СЛОВАРИ ЧАТОВ (Синхронизировано со Скайнетом) ====================
chat_ids_mk = {
    "Екатеринбург": -1002210043742,
    "Челябинск": -1002238514762,
    "БЕЗ ПРЕДРАССУДКОВ": -1001219669239,
    "RAINBOW MAN": -1003496028436,
    "Пермь": -1002205127231,
    "Ижевск": -1001604781452,
    "Казань": -1002228881675,
    "Оренбург": -1002255568202,
    "Уфа": -1002196469365,
    "Новосибирск": -1002235645677,
    "Красноярск": -1002248474008,
    "Барнаул": -1002234471215,
    "Омск": -1002151258573,
    "Саратов": -1002426762134,
    "Воронеж": -1002207503508,
    "Самара": -1001852671383,
    "Волгоград": -1002167762598,
    "Нижний Новгород": -1001631628911,
    "Калининград": -1002217056197,
    "Иркутск": -1002685095003,
    "Кемерово": -1002147522863,
    "Москва": -1002208434096,
    "Санкт-Петербург": -1002485776859,
    "Общая группа Юга": -1001814693664,
    "Тюмень": -1002210623988,
    "ХМАО": -1002210623988,
    "ЯМАЛ": -1002210623988,
    "Казахстан": -1003091556050,
    "Мужской Чат": -1002169723426,
    "Фетиши": -1002197215824,
    "Аренда Жилья": -1001238252865,
    "Секс Туризм": -1002236337328,
    "Галерея": -1002217967528,
    "Тестовая группа 🛠️": -1002426733876
}

chat_ids_parni = {
    "Екатеринбург": -1002413948841,
    "Тюмень": -1002255622479,
    "Омск": -1002274367832,
    "Челябинск": -1002406302365,
    "Пермь": -1002280860973,
    "Курган": -1002469285352,
    "ХМАО": -1002287709568,
    "Уфа": -1002448909000,
    "Новосибирск": -1002261777025,
    "ЯМАЛ": -1002371438340,
    "Оренбург": -1003888335997,
    "Москва": -1003856528145,
    "Санкт-Петербург": -1003519420984,
    "Красноярск": -1003347456711
}

chat_ids_ns = {
    "Новосибирск": -1001824149334,
    "Челябинск": -1002233108474,
    "Пермь": -1001753881279,
    "Уфа": -1001823390636,
    "ЯМАЛ": -1002145851794,
    "Москва": -1001938448310,
    "ХМАО": -1001442597049,
    "Екатеринбург": -1002169473861,
    "Тюмень": -1002170955867,
    "Санкт-Петербург": -1002335014334,
    "Тюмень 2": -1001427433513,
    "Челябинск 2": -1002193127380
}

chat_ids_rainbow = {
    "Екатеринбург": -1002419653224
}

chat_ids_gayznak = {
    "Красноярск": -1002335149925,
    "Екатеринбург": -1002571605722,
    "Пермь": -1002599206099,
    "Тюмень": -1002553431228,
    "Новосибирск": -1002627786446,
    "Самара": -1002301984331,
    "Казань": -1002277433049,
    "Воронеж": -1002428155161,
    "Кемерово": -1002418700136,
    "Иркутск": -1002454522264,
    "Челябинск": -1003366643944,
    "Орёл": -1003323558103,
    "Саратов": -1003638608363,
    "Архангельск": -1003120218775,
    "Ярославль": -1003332193158,
    "Тверь": -1003369813272,
    "Великий Новгород": -1003429766543,
    "Владимир": -1003276544901,
    "Мурманск": -1003302580641,
    "Рязань": -1003460247519,
    "Смоленск": -1003423811230,
    "Тамбов": -1003225139634,
    "Липецк": -1003487872172,
    "Тула": -1003482077625,
    "Брянск": -1003372917376,
    "Волгоград": -1002476113714
}

all_cities = {}

def insert_to_all(city, net_key, real_name, chat_id):
    clean_city = city.replace(" 2", "")
    if clean_city not in all_cities:
        all_cities[clean_city] = {}
    if net_key not in all_cities[clean_city]:
        all_cities[clean_city][net_key] = []
    all_cities[clean_city][net_key].append({"name": real_name, "chat_id": chat_id})

for city, chat_id in chat_ids_mk.items(): insert_to_all(city, "mk", city, chat_id)
for city, chat_id in chat_ids_parni.items(): insert_to_all(city, "parni", city, chat_id)
for city, chat_id in chat_ids_ns.items(): insert_to_all(city, "ns", city, chat_id)
for city, chat_id in chat_ids_rainbow.items(): insert_to_all(city, "rainbow", city, chat_id)
for city, chat_id in chat_ids_gayznak.items(): insert_to_all(city, "gayznak", city, chat_id)

# Статичные подписи для каждой сети с новой строкой и дополнительной подписью
network_signatures = {
    "Мужской Клуб": (
        "🕸️Реклама. Согласовано с администрацией сети МК.\n\n"
        "<b>Администрация сети не рекомендует вносить какую-либо предоплату. Если ВАС обманули или развели, сообщите в бота поддержки!</b>\n"
        "<i>Реклама. Не является публичной офертой.</i>"
    ),
    "ПАРНИ 18+": (
        "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫\n\n"
        "<b>Администрация сети не рекомендует вносить какую-либо предоплату. Если ВАС обманули или развели, сообщите в бота поддержки!</b>\n"
        "<i>Реклама. Не является публичной офертой.</i>"
    ),
    "НС": (
        "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫\n\n"
        "<b>Администрация сети не рекомендует вносить какую-либо предоплату. Если ВАС обманули или развели, сообщите в бота поддержки!</b>\n"
        "<i>Реклама. Не является публичной офертой.</i>"
    ),
    "Радуга": (
        "Рекламная интеграция согласована с администратором.\n\n"
        "Администрация не несёт ответственности за объявления пользователей.\n"
        "Не вносите предоплату незнакомым лицам.\n"
        "<i>Реклама. Не является публичной офертой.</i>"
    ),
    "Гей Знакомства": (
        "Рекламная интеграция согласована с администратором.\n\n"
        "Будьте осторожны при общении, не переводите деньги незнакомым людям.\n"
        "<i>Реклама. Не является публичной офертой.</i>"
    )
}

# ==================== БИГ-ЧАТЫ И ЦЕНООБРАЗОВАНИЕ ====================
BIG_CHATS = [
    chat_ids_mk.get("БЕЗ ПРЕДРАССУДКОВ"),
    chat_ids_mk.get("Галерея"),
    chat_ids_mk.get("Мужской Чат"),
    chat_ids_mk.get("Фетиши"),
    chat_ids_mk.get("Аренда Жилья"),
    chat_ids_mk.get("Секс Туризм")
]

def get_price_for_chat(chat_id, days):
    """Динамический расчет стоимости рекламы из MongoDB"""
    try:
        prices = db['settings'].find_one({"_id": "skynet_pricing"})
    except:
        prices = None

    if not prices:
        # Резервный бэкап тарифов на случай сбоя связи с Mongo
        prices = {
            "vip_big_chat_1": 1095, "vip_big_chat_7": 7656,
            "reg_small_1": 105, "reg_small_7": 490, "reg_small_15": 720, "reg_small_30": 938,
            "reg_big_1": 105, "reg_big_7": 656, "reg_big_15": 1288, "reg_big_30": 1563
        }

    # 1. Если это БИГ-чат
    if chat_id in BIG_CHATS:
        if days == 1: return prices.get("vip_big_chat_1", 1095)
        if days == 7: return prices.get("vip_big_chat_7", 7656)
        return None
        
    # 2. Узнаем размер обычного чата
    try:
        count = bot.get_chat_member_count(chat_id)
    except:
        count = 500 
        
    # 3. Выдаем цену по динамической матрице
    if count > 1000:
        day_map = {1: "reg_big_1", 7: "reg_big_7", 15: "reg_big_15", 30: "reg_big_30"}
        fallback_map = {1: 105, 7: 656, 15: 1288, 30: 1563}
    else:
        day_map = {1: "reg_small_1", 7: "reg_small_7", 15: "reg_small_15", 30: "reg_small_30"}
        fallback_map = {1: 105, 7: 490, 15: 720, 30: 938}
        
    key = day_map.get(days)
    return prices.get(key, fallback_map.get(days))

def get_main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Создать новое объявление", "Удалить объявление", "Удалить все объявления", "📊 Моя статистика")
    return markup

def format_time(dt):
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt)
        except:
            return "неизвестно"
    return dt.strftime("%d.%m.%Y %H:%M")

def get_user_name(user):
    name = escape_md(user.first_name)
    if user.username:
        return f"[{name}](https://t.me/{user.username})"
    else:
        return f"[{name}](tg://user?id={user.id})"

# Функция для выбора срока оплаты
def select_duration_for_payment(message, user_id, network, city):
    if message.text == "Назад":
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)
        if network == "Мужской Клуб":
            cities = list(chat_ids_mk.keys())
        elif network == "ПАРНИ 18+":
            cities = list(chat_ids_parni.keys())
        elif network == "НС":
            cities = list(chat_ids_ns.keys())
        elif network == "Радуга":
            cities = list(chat_ids_rainbow.keys())
        elif network == "Гей Знакомства":
            cities = list(chat_ids_gayznak.keys())
        markup.add(*cities)
        markup.add("Назад")
        bot.send_message(message.chat.id, "📍 Выберите город для добавления пользователя:", reply_markup=markup)
        bot.register_next_step_handler(message, lambda m: select_city_for_payment(m, user_id, network))
        return

    duration = message.text
    if duration == "День":
        days = 1
    elif duration == "Неделя":
        days = 7
    elif duration == "Месяц":
        days = 30
    else:
        bot.send_message(message.chat.id, "❗ Ошибка! Выберите правильный срок.")
        bot.register_next_step_handler(message, lambda m: select_duration_for_payment(m, user_id, network, city))
        return

    expiry_date = now_ekb() + timedelta(days=days)

    # 💥 ЗАПИСЬ В БАЗУ ДАННЫХ
    ad_subs_collection.insert_one({
        "user_id": user_id,
        "network": network,
        "city": city,
        "end_date": expiry_date, # <-- Исправлено на expiry_date
        "purchase_date": now_ekb(),
        "has_pin": False,        # <-- При ручной выдаче закреп по умолчанию выключен
        # 👇 НОВЫЕ ФЛАГИ-ГЛУШИТЕЛИ 👇
        "notified_72h": True if days <= 3 else False,
        "notified_24h": True if days <= 1 else False,
        "notified_3h": False
    })

    try:
        user_info = bot.get_chat(user_id)
        user_name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip()
        if not user_name: user_name = user_info.username or "Имя не указано"
    except:
        user_name = "Имя не найдено"

    if message.chat.id != ADMIN_CHAT_ID:
        bot.send_message(message.chat.id, f"✅ {user_name} (ID: {user_id}) добавлен в «{network}» ({city}) до {expiry_date.strftime('%d.%m.%Y')}.")

    bot.send_message(ADMIN_CHAT_ID, f"👨‍💼 Выданы права (руками):\n{user_name} (ID: {user_id})\nСеть: {network}\nГород: {city}\n📅 До: {expiry_date.strftime('%d.%m.%Y')}")

def get_user_statistics(user_id):
    """Получает статистику пользователя напрямую из MongoDB."""
    stats = {"published": 0, "remaining": 0, "details": {}}
    limit_total = 0
    today_start = now_ekb().replace(hour=0, minute=0, second=0, microsecond=0)

    # 1. Получаем все активные доступы юзера
    active_subs = list(ad_subs_collection.find({"user_id": user_id, "end_date": {"$gt": now_ekb()}}))

    # Разворачиваем "Все сети" в конкретные сети для проверки лимитов
    networks_to_check = []
    for sub in active_subs:
        if sub["network"] == "Все сети":
            for net in ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства"]:
                networks_to_check.append((net, sub["city"]))
        else:
            networks_to_check.append((sub["network"], sub["city"]))

    networks_to_check = list(set(networks_to_check)) # Убираем дубликаты

    # 2. Считаем публикации за сегодня
    for network, city in networks_to_check:
        today_posts_count = ad_posts_collection.count_documents({
            "user_id": user_id,
            "network": network,
            "city": city,
            "time": {"$gte": today_start}
        })

        limit_total += 3
        if network not in stats["details"]:
            stats["details"][network] = {}

        stats["details"][network][city] = {
            "published": today_posts_count,
            "remaining": max(0, 3 - today_posts_count)
        }
        stats["published"] += today_posts_count

    stats["remaining"] = max(0, limit_total - stats["published"])
    return stats

def get_admin_statistics():
    """Сбор статистики для админов (по всем юзерам за сегодня)"""
    statistics = {}
    today_start = now_ekb().replace(hour=0, minute=0, second=0, microsecond=0)

    # Берем ВСЕ сегодняшние посты из базы
    today_posts = list(ad_posts_collection.find({"time": {"$gte": today_start}}))
    
    for post in today_posts:
        uid = post["user_id"]
        net = post["network"]
        city = post["city"]
        
        if uid not in statistics:
            statistics[uid] = {"published": 0, "remaining": 0, "details": {}, "links": set()}
            
        if net not in statistics[uid]["details"]:
            statistics[uid]["details"][net] = {}
            
        if city not in statistics[uid]["details"][net]:
            statistics[uid]["details"][net][city] = {"published": 0, "remaining": 3} # Выдаем базовый лимит
            
        # Плюсуем счетчики
        statistics[uid]["details"][net][city]["published"] += 1
        statistics[uid]["details"][net][city]["remaining"] = max(0, 3 - statistics[uid]["details"][net][city]["published"])
        statistics[uid]["published"] += 1
        
        # Добавляем ссылку
        chat_id_short = str(post["chat_id"]).replace("-100", "")
        statistics[uid]["links"].add(f"https://t.me/c/{chat_id_short}/{post['message_id']}")

    # Преобразуем множества ссылок в списки
    for uid in statistics:
        statistics[uid]["links"] = list(statistics[uid]["links"])
        
    return statistics

@bot.message_handler(commands=['start'])
def start(message):
    try:
        if message.chat.type != "private":
            bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
            return

        bot.send_message(
            message.chat.id,
            "Привет! Я PostGoldBot. 👋\nВыберите действие:",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        bot.send_message(ADMIN_CHAT_ID, f"Ошибка в /start: {e}")

from collections import defaultdict

# 🛠 Админ-панель
@bot.message_handler(commands=['admin'])
def admin_panel(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, "⛔ У вас нет прав для выполнения этой команды.")
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("➕ Добавить оплатившего", callback_data="admin_add_paid_user"))
    markup.add(types.InlineKeyboardButton("📋 Список оплативших", callback_data="admin_list_paid_users"))
    markup.add(types.InlineKeyboardButton("⏳ Изменить срок оплаты", callback_data="admin_change_duration"))
    markup.add(types.InlineKeyboardButton("👑 Добавить администратора", callback_data="admin_add_admin"))
    markup.add(types.InlineKeyboardButton("📊 Статистика публикаций", callback_data="admin_statistics"))
    markup.add(types.InlineKeyboardButton("📛 Попытки без доступа", callback_data="show_failed_attempts:0"))
    markup.add(types.InlineKeyboardButton("🗂 История постов", callback_data="admin_post_history:0"))
    markup.add(types.InlineKeyboardButton("🗑 Удалить объявления пользователя", callback_data="admin_delete_user_posts"))

    bot.send_message(message.chat.id, "🛠 *Админ-панель:*", reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data == "admin_add_paid_user")
def handle_add_paid_user(call):
    bot.send_message(call.message.chat.id, "Введите ID пользователя:")
    bot.register_next_step_handler(call.message, process_user_id_for_payment)

@bot.callback_query_handler(func=lambda call: call.data == "admin_list_paid_users")
def handle_list_paid_users(call):
    show_paid_users(call.message)

@bot.callback_query_handler(func=lambda call: call.data == "admin_change_duration")
def handle_change_duration_request(call):
    bot.send_message(call.message.chat.id, "Введите ID пользователя для изменения срока:")
    bot.register_next_step_handler(call.message, select_user_for_duration_change)

# --- МОДЕРАЦИЯ VIP-РЕКЛАМЫ ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("vip_approve_") or call.data.startswith("vip_reject_"))
def handle_vip_moderation(call):
    bot.answer_callback_query(call.id) # 🛑 Убираем "часики" загрузки с кнопки
    
    # Правильная распаковка: call.data выглядит как "vip_approve_123456"
    parts = call.data.split("_")
    action = parts[1] # Теперь здесь будет точно "approve" или "reject"
    user_id = int(parts[2])

    if action == "approve":
        # 1. Записываем юзеру VIP-статус для расчета цен
        db['users'].update_one({"_id": user_id}, {"$set": {"temp_ad_type": "vip"}}, upsert=True)
        
        # 2. Пишем пользователю, что всё ок
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("🚀 Выбрать сеть и оплатить", callback_data="start_vip_payment"))
        try:
            bot.send_message(user_id, "🎉 <b>Ваш ресурс успешно одобрен!</b>\nТеперь вы можете выбрать сеть, город и оплатить размещение (к прайсу применена наценка +50% за увод аудитории).\n\nЖмите кнопку ниже:", parse_mode="HTML", reply_markup=markup)
        except: pass
        
        # 3. Убираем кнопки у админа, чтобы не нажать дважды
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        bot.send_message(call.message.chat.id, f"✅ <b>Заявка ОДОБРЕНА (Пользователь ID: <code>{user_id}</code>)</b>", reply_to_message_id=call.message.message_id, parse_mode="HTML")

    elif action == "reject":
        # Убираем статус на всякий случай
        db['users'].update_one({"_id": user_id}, {"$unset": {"temp_ad_type": ""}})
        try:
            bot.send_message(user_id, "❌ К сожалению, мы не можем разместить рекламу данного ресурса в нашей сети. Заявка отклонена.", reply_markup=get_main_keyboard())
        except: pass
        
        # Убираем кнопки у админа
        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        bot.send_message(call.message.chat.id, f"❌ <b>Заявка ОТКЛОНЕНА (Пользователь ID: <code>{user_id}</code>)</b>", reply_to_message_id=call.message.message_id, parse_mode="HTML")

# --- СТАРТ ПОСЛЕ ОДОБРЕНИЯ ---
@bot.callback_query_handler(func=lambda call: call.data == "start_vip_payment")
def resume_vip_payment(call):
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
    bot.register_next_step_handler(call.message, select_network_step)

@bot.callback_query_handler(func=lambda call: call.data == "admin_statistics")
def handle_admin_statistics(call):
    show_statistics_for_admin(call.message.chat.id)

@bot.callback_query_handler(func=lambda call: call.data == "admin_delete_user_posts")
def handle_admin_delete_user_posts(call):
    bot.send_message(call.message.chat.id, "🆔 Введите ID пользователя, чьи объявления нужно удалить:")
    bot.register_next_step_handler(call.message, delete_user_posts_step)

# Функция для добавления оплатившего
def process_user_id_for_payment(message):
    try:
        user_id = int(message.text)
        bot.send_message(message.chat.id, "️ Выберите сеть для добавления пользователя:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, lambda m: select_network_for_payment(m, user_id))
    except ValueError:
        bot.send_message(message.chat.id, " Ошибка: ID должен быть числом.")

# Функция для выбора сети при добавлении оплатившего
def select_network_for_payment(message, user_id):
    if message.text == "Назад":
        admin_panel(message)
        return

    network = message.text
    if network not in ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства", "Все сети"]:
        bot.send_message(message.chat.id, "❗ Ошибка! Выберите правильную сеть.")
        bot.register_next_step_handler(message, lambda m: select_network_for_payment(m, user_id))
        return

    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)
    network_key = normalize_network_key(network)

    if network == "Все сети":
        # Только города, где хотя бы 2+ сетей доступны
        cities = [city for city, data in all_cities.items() if len(data.keys()) >= 2]
    else:
        cities = [city for city, data in all_cities.items() if network_key in data]

    for city in cities:
        markup.add(city)
    markup.add("Назад")
    bot.send_message(message.chat.id, "📍 Выберите город для добавления пользователя:", reply_markup=markup)
    bot.register_next_step_handler(message, lambda m: select_city_for_payment(m, user_id, network))

def select_city_for_payment(message, user_id, network):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "️Выберите сеть для добавления пользователя:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, lambda m: select_network_for_payment(m, user_id))
        return

    city = message.text
    network_key = normalize_network_key(network)

    # Повторно получаем список допустимых городов для проверки
    if network == "Все сети":
        allowed_cities = [c for c, d in all_cities.items() if len(d.keys()) >= 2]
    else:
        allowed_cities = [c for c, d in all_cities.items() if network_key in d]

    if city not in allowed_cities:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        for c in allowed_cities:
            markup.add(c)
        markup.add("Назад")
        bot.send_message(message.chat.id, "📍 Пожалуйста, выберите город из списка:", reply_markup=markup)
        bot.register_next_step_handler(message, lambda m: select_city_for_payment(m, user_id, network))
        return

    # Всё ок — идём дальше
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("День", "Неделя", "Месяц")
    bot.send_message(message.chat.id, "⏳ Выберите срок оплаты:", reply_markup=markup)
    bot.register_next_step_handler(message, lambda m: select_duration_for_payment(m, user_id, network, city))

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_failed_attempts"))
def show_failed_attempts(call):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "⛔ Нет доступа.")
        return

    try:
        parts = call.data.split(":")
        page = int(parts[1]) if len(parts) > 1 else 0
    except:
        page = 0

    try:
        # ЗАПРОС К MONGODB ВМЕСТО SQLITE
        attempts = list(db['failed_attempts'].find().sort("time", pymongo.DESCENDING))

        if not attempts:
            bot.answer_callback_query(call.id, "✅ Нет попыток без доступа.")
            return

        start = page * ATTEMPTS_PER_PAGE
        end = start + ATTEMPTS_PER_PAGE
        total_pages = (len(attempts) - 1) // ATTEMPTS_PER_PAGE + 1
        page_attempts = attempts[start:end]

        response = f"<b>📛 Попытки публикации без доступа (стр. {page+1} из {total_pages}):</b>\n\n"
        for attempt in page_attempts:
            user_id = attempt.get('user_id', 'Неизвестно')
            network = escape_html(attempt.get('network', ''))
            city = escape_html(attempt.get('city', ''))
            reason = escape_html(attempt.get('reason', ''))
            time_val = attempt.get('time')
            time_formatted = time_val.strftime('%d.%m.%Y %H:%M') if isinstance(time_val, datetime) else "неизвестно"

            response += (f"👤 ID: <code>{user_id}</code>\n"
                         f"🌐 Сеть: <b>{network}</b>, Город: <b>{city}</b>\n"
                         f"🕐 {time_formatted}\n"
                         f"❌ Причина: <i>{reason}</i>\n\n")

        keyboard = InlineKeyboardMarkup()
        buttons = []
        if page > 0: buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"show_failed_attempts:{page - 1}"))
        if end < len(attempts): buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"show_failed_attempts:{page + 1}"))
        if buttons: keyboard.row(*buttons)

        bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=keyboard)
        bot.answer_callback_query(call.id)
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ Ошибка: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_post_history:"))
def show_post_history(call):
    try:
        page = int(call.data.split(":")[1])
        posts_per_page = 5 

        # ОПТИМИЗАЦИЯ: Считаем общее кол-во и берем из базы ТОЛЬКО 5 нужных постов (чтобы бот летал)
        total_posts = ad_posts_collection.count_documents({})
        if total_posts == 0:
            bot.answer_callback_query(call.id, "История постов пуста.")
            return

        total_pages = (total_posts - 1) // posts_per_page + 1
        posts = list(ad_posts_collection.find().sort("time", pymongo.DESCENDING).skip(page * posts_per_page).limit(posts_per_page))

        report = f"<b>📜 История публикаций (стр. {page + 1} из {total_pages}):</b>\n\n"
        for post in posts:
            user_id = post.get('user_id')
            raw_user_name = post.get('user_name', 'Неизвестен')
            
            # 🛡 ИСПРАВЛЕНИЕ HTML: Вырезаем старые теги <a href>, которые застряли в базе с прошлых времен
            clean_name = re.sub(r'<[^>]+>', '', str(raw_user_name))
            clean_name = escape_html(clean_name)
            
            network = escape_html(post.get('network', ''))
            city = escape_html(post.get('city', ''))
            chat_id = post.get('chat_id')
            message_id = post.get('message_id')
            deleted = post.get('deleted', False)
            
            time_val = post.get('time')
            formatted_time = time_val.strftime('%d.%m.%Y %H:%M') if isinstance(time_val, datetime) else "неизвестно"

            chat_id_short = str(chat_id).replace("-100", "") if chat_id else ""
            status_line = f"❌ <b>Статус:</b> Удалён" if deleted else f"✅ <b>Статус:</b> Активен"

            report += (f"👤 <b>Юзер:</b> <a href='tg://user?id={user_id}'>{clean_name}</a> (ID: <code>{user_id}</code>)\n"
                       f"🌐 <b>Сеть/Группа:</b> {network} ({city})\n"
                       f"🕒 <b>Время:</b> {formatted_time}\n"
                       f"{status_line}\n"
                       f"🔗 <a href='https://t.me/c/{chat_id_short}/{message_id}'>Перейти к посту</a>\n\n")

        keyboard = types.InlineKeyboardMarkup()
        buttons = []
        if page > 0: buttons.append(types.InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_post_history:{page - 1}"))
        if page < total_pages - 1: buttons.append(types.InlineKeyboardButton("Вперёд ➡️", callback_data=f"admin_post_history:{page + 1}"))
        if buttons: keyboard.row(*buttons)

        bot.edit_message_text(report, chat_id=call.message.chat.id, message_id=call.message.message_id, parse_mode="HTML", reply_markup=keyboard)
    except Exception as e:
        bot.answer_callback_query(call.id, f"Ошибка: {e}")

def is_admin(user_id):
    """Проверка прав администратора через MongoDB"""
    if user_id in CORE_ADMINS:
        return True
    return bool(admins_collection.find_one({"_id": user_id}))

def add_admin_step(message):
    """Добавление нового админа в БД"""
    try:
        new_admin_id = int(message.text)
        admins_collection.update_one(
            {"_id": new_admin_id}, 
            {"$set": {"added_by": message.from_user.id, "time": now_ekb()}}, 
            upsert=True
        )
        bot.send_message(message.chat.id, f"✅ Пользователь {new_admin_id} добавлен как администратор.")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Ошибка: ID должен быть числом.")
 
# Функция для отображения списка оплативших ИЗ MONGODB
def show_paid_users(message):
    now = now_ekb()
    # Ищем все подписки, которые еще не истекли
    active_subs = list(ad_subs_collection.find({"end_date": {"$gt": now}}).sort("end_date", 1))

    if not active_subs:
        bot.send_message(message.chat.id, "📋 <b>Список активных оплат:</b>\n\nПусто. Никто еще не купил рекламу.", parse_mode="HTML")
        return

    response = "📋 <b>Список активных оплат (MongoDB):</b>\n"
    current_user = None
    
    for sub in active_subs:
        user_id = sub.get("user_id")
        network = escape_html(sub.get("network"))
        city = escape_html(sub.get("city"))
        end_date = sub.get("end_date")
        date_str = to_ekb_str(end_date)

        if user_id != current_user:
            try:
                user_info = bot.get_chat(user_id)
                name = escape_html(user_info.first_name or "Без имени")
                username = user_info.username
                user_line = f"\n👤 <a href='tg://user?id={user_id}'>{user_id}</a> | {name}"
                if username: user_line += f" (@{username})"
            except:
                user_line = f"\n👤 Пользователь: <code>{user_id}</code>"
            
            response += f"{user_line}\n"
            current_user = user_id

        response += f" - 🧩 {network}, 📍 {city} (до {date_str})\n"

    bot.send_message(message.chat.id, response, parse_mode="HTML")

# --- 1. Вывод списка активных подписок юзера ---
def select_user_for_duration_change(message):
    try:
        user_id = int(message.text)
        # Ищем активные подписки юзера
        active_subs = list(ad_subs_collection.find({"user_id": user_id, "end_date": {"$gt": now_ekb()}}))
        
        if not active_subs:
            bot.send_message(message.chat.id, "❌ У пользователя нет активных подписок в базе.")
            return

        markup = types.InlineKeyboardMarkup(row_width=1)
        for sub in active_subs:
            net = sub.get('network', 'Неизвестно')
            city = sub.get('city', 'Неизвестно')
            sub_id = str(sub['_id']) # Уникальный ID конкретной покупки
            end_date = to_ekb_str(sub['end_date'], '%d.%m.%Y')
            
            btn_text = f"🧩 {net} | 📍 {city} (до {end_date})"
            markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"manage_sub_{sub_id}"))
            
        bot.send_message(message.chat.id, f"📋 Найдены активные подписки для ID <code>{user_id}</code>.\nВыберите, какую именно изменить:", reply_markup=markup, parse_mode="HTML")
    except ValueError:
        bot.send_message(message.chat.id, "❌ Ошибка: ID должен быть числом.")

# --- 2. Меню выбора (+1 день, -1 неделя и т.д.) ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("manage_sub_"))
def handle_manage_sub_selection(call):
    sub_id = call.data.replace("manage_sub_", "")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("+1 день", callback_data=f"change_duration_{sub_id}_1"),
               types.InlineKeyboardButton("+1 неделя", callback_data=f"change_duration_{sub_id}_7"),
               types.InlineKeyboardButton("+1 месяц", callback_data=f"change_duration_{sub_id}_30"))
    markup.add(types.InlineKeyboardButton("-1 день", callback_data=f"change_duration_{sub_id}_-1"),
               types.InlineKeyboardButton("-1 неделя", callback_data=f"change_duration_{sub_id}_-7"),
               types.InlineKeyboardButton("-1 месяц", callback_data=f"change_duration_{sub_id}_-30"))
    
    # 👇 НОВЫЕ КНОПКИ ДЛЯ ССЫЛОК 👇
    markup.add(types.InlineKeyboardButton("✅ Разрешить ссылки", callback_data=f"allow_links_{sub_id}"),
               types.InlineKeyboardButton("❌ Запретить ссылки", callback_data=f"deny_links_{sub_id}"))
    
    bot.edit_message_text("⏳ Выберите действие для выбранной подписки:", call.message.chat.id, call.message.message_id, reply_markup=markup)

# --- 3. Физическое применение изменений в MongoDB ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("change_duration_"))
def handle_duration_change(call):
    try:
        parts = call.data.split("_")
        # ЗАЩИТА: проверяем, что кусков данных точно хватает
        if len(parts) < 4:
            bot.answer_callback_query(call.id, "❌ Устаревшая кнопка.")
            return
            
        sub_id = parts[2]
        days = int(parts[3])

        # Ищем эту конкретную подписку по ObjectId
        sub = ad_subs_collection.find_one({"_id": ObjectId(sub_id)})
        
        if not sub:
            bot.answer_callback_query(call.id, "❌ Подписка не найдена или уже истекла.")
            return

        # Накидываем или убавляем дни
        new_date = sub["end_date"] + timedelta(days=days)
        ad_subs_collection.update_one({"_id": ObjectId(sub_id)}, {"$set": {"end_date": new_date}})

        bot.answer_callback_query(call.id, f"✅ Срок изменён на {days} дней.")
        
        # Отчитываемся админу об успехе (используем наш новый переводчик to_ekb_str)
        success_text = (f"✅ <b>Срок успешно изменён!</b>\n\n"
                        f"🌐 Сеть: <b>{escape_html(sub.get('network'))}</b>\n"
                        f"📍 Город: <b>{escape_html(sub.get('city'))}</b>\n"
                        f"⏳ Новая дата окончания: <b>{to_ekb_str(new_date)}</b>")
        bot.edit_message_text(success_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")

    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка при изменении срока.")

# --- Выдача и отзыв прав на ссылки для конкретной подписки ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("allow_links_") or call.data.startswith("deny_links_"))
def handle_links_permission(call):
    action, _, sub_id = call.data.split("_")
    
    # Определяем, выдаем или забираем права
    is_allowed = (action == "allow")
    
    # Обновляем конкретную подписку в MongoDB
    result = ad_subs_collection.update_one(
        {"_id": ObjectId(sub_id)}, 
        {"$set": {"can_post_links": is_allowed}}
    )
    
    if result.modified_count > 0:
        status_text = "✅ Разрешение на публикацию ссылок ВЫДАНО!" if is_allowed else "🚫 Разрешение на ссылки ОТОЗВАНО."
        bot.answer_callback_query(call.id, status_text, show_alert=True)
    else:
        bot.answer_callback_query(call.id, "⚠️ Подписка уже имеет этот статус или не найдена.")

@bot.message_handler(commands=['statistics'])
def show_statistics_for_admin(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "⛔ У вас нет прав для просмотра статистики.")
        return

    stats = get_admin_statistics()
    if not stats:
        bot.send_message(chat_id, "ℹ️ Нет данных о публикациях за сегодня.")
        return

    response = "<b>📊 Статистика публикаций за сегодня:</b>\n\n"

    for user_id, user_stats in stats.items():
        try:
            user_info = bot.get_chat(user_id)
            user_name = escape_html(user_info.first_name)
            user_link = (f"<a href='https://t.me/{user_info.username}'>{user_name}</a>" if user_info.username 
                         else f"<a href='tg://user?id={user_info.id}'>{user_name}</a>")
        except:
            user_link = f"ID <code>{user_id}</code>"

        response += (f"👤 {user_link}\n"
                     f"📨 Опубликовано: <b>{user_stats['published']}</b>\n")

        if user_stats["details"]:
            response += "🧾 <b>Детали по сетям:</b>\n"
            for network, cities in user_stats["details"].items():
                for city, data in cities.items():
                    # Ищем подписку в Mongo
                    sub = ad_subs_collection.find_one({"user_id": user_id, "network": {"$in": ["Все сети", network]}, "city": city})
                    expire_str = f"⏳ до {sub['end_date'].strftime('%d.%m.%Y')}" if sub else "(Без активной подписки)"
                    
                    response += (f"  └ 🧩 <b>{escape_html(network)}</b>, 📍<b>{escape_html(city)}</b> {expire_str}:\n"
                                 f"     Опубликовано: <b>{data['published']} / 3</b>\n")

        if user_stats["links"]:
            unique_links = list(set(user_stats["links"]))
            response += "🔗 <b>Ссылки:</b>\n"
            for link in unique_links:
                response += f"  • <a href='{link}'>{link}</a>\n"
        response += "\n"

    try:
        bot.send_message(chat_id, response, parse_mode="HTML", disable_web_page_preview=True)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка отправки: <code>{escape_html(str(e))}</code>", parse_mode="HTML")

# --- ВЕРНУВШИЕСЯ ПОМОЩНИКИ ---
def get_network_markup():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add(
        "Мужской Клуб",
        "ПАРНИ 18+",
        "НС",
        "Радуга",
        "Гей Знакомства",
        "Все сети",
        "Назад"
    )
    return markup

def normalize_network_key(name):
    """Приводим название сети к ключу all_cities: mk, parni, ns, rainbow, gayznak"""
    if name == "Мужской Клуб": return "mk"
    elif name == "ПАРНИ 18+": return "parni"
    elif name in ["НС", "Знакомства 66", "Знакомства 74"]: return "ns"
    elif name == "Радуга": return "rainbow"
    elif name == "Гей Знакомства": return "gayznak"
    return None

def get_user_html_link(user):
    name = html.escape(user.first_name or "Без имени")
    if user.last_name:
        name += " " + html.escape(user.last_name)
    return f'<a href="tg://user?id={user.id}">{name}</a>'
# -----------------------------

@bot.message_handler(func=lambda message: message.text == "Создать новое объявление")
def create_new_post_category(message):
    if message.chat.type != "private": return
    
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True, row_width=1)
    markup.add("💆‍♂️ Встречи / Услуги / Массажи", "📢 Реклама TG-групп / Каналов", "Назад")
    bot.send_message(message.chat.id, "Выберите категорию вашего объявления:", reply_markup=markup)
    bot.register_next_step_handler(message, handle_category)

def handle_category(message):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Главное меню", reply_markup=get_main_keyboard())
        return
        
    if "групп" in message.text:
        bot.send_message(
            message.chat.id, 
            "Отлично! 🎪 Но перед оплатой мы должны убедиться, что тематика вашего ресурса подходит для нашей сети.\n\n"
            "Пожалуйста, отправьте ссылку на ваш канал/группу или @username:", 
            reply_markup=types.ReplyKeyboardRemove()
        )
        bot.register_next_step_handler(message, request_vip_approval)
        return
        
    # Если это обычное объявление — сохраняем статус "std" и идем дальше
    db['users'].update_one({"_id": message.from_user.id}, {"$set": {"temp_ad_type": "std"}}, upsert=True)
    bot.send_message(message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
    bot.register_next_step_handler(message, select_network_step)

# НОВАЯ ФУНКЦИЯ: Принимаем ссылку и шлем админу
def request_vip_approval(message):
    if message.text in ["Назад", "/start"]:
        bot.send_message(message.chat.id, "Главное меню", reply_markup=get_main_keyboard())
        return
        
    link = message.text
    user_id = message.from_user.id
    user_name = escape_html(message.from_user.first_name)

    # 1. Отправляем заявку Админу
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton("✅ Одобрить", callback_data=f"vip_approve_{user_id}"),
        types.InlineKeyboardButton("❌ Отклонить", callback_data=f"vip_reject_{user_id}")
    )
    bot.send_message(
        ADMIN_CHAT_ID,
        f"🚨 <b>Новая заявка на VIP-рекламу!</b>\n👤 Пользователь: <a href='tg://user?id={user_id}'>{user_name}</a> (<code>{user_id}</code>)\n🔗 Ссылка: {escape_html(link)}",
        parse_mode="HTML",
        reply_markup=markup
    )

    # 2. Успокаиваем пользователя
    bot.send_message(message.chat.id, "⏳ <b>Заявка отправлена на модерацию.</b>\nКак только администратор проверит ресурс, вы получите уведомление!", parse_mode="HTML", reply_markup=get_main_keyboard())

def select_network_step(message):
    if message.text == "Назад":
        # Возвращаем на шаг назад к выбору категории
        create_new_post_category(message)
        return

    selected_network = message.text.strip()
    valid_networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства", "Все сети"]

    if selected_network in valid_networks:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)

        if selected_network == "Все сети":
            cities = [city for city, nets in all_cities.items() if len(nets) >= 2]
        else:
            key = normalize_network_key(selected_network)
            cities = [city for city, nets in all_cities.items() if key in nets]

        for city in cities:
            markup.add(city)
        markup.add("Выбрать другую сеть", "Назад")

        bot.send_message(
            message.chat.id,
            "📍 <b>Выберите город</b> для публикации или нажмите «<i>Выбрать другую сеть</i>»:",
            reply_markup=markup,
            parse_mode="HTML"
        )
        # Передаем эстафету функции проверки оплаты
        bot.register_next_step_handler(message, select_city_check_payment, selected_network)
    else:
        bot.send_message(message.chat.id, "❌ Ошибка! Пожалуйста, выберите одну из предложенных сетей.", parse_mode="HTML")
        bot.register_next_step_handler(message, select_network_step)

def select_city_check_payment(message, selected_network):
    if message.text == "Назад" or message.text == "Выбрать другую сеть":
        bot.send_message(message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network_step)
        return

    city = message.text
    user_id = message.from_user.id
    networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства"] if selected_network == "Все сети" else [selected_network]

    # --- ПРОВЕРКА ОПЛАТЫ НА ВХОДЕ ---
    has_access = False
    for network in networks:
        net_key = normalize_network_key(network)
        if all_cities.get(city, {}).get(net_key) and is_user_paid(user_id, network, city):
            has_access = True
            break

    if not has_access:
        # ВЫВОДИМ ПЕЙВОЛ (Логика из старой функции)
        network = networks[0] # Берем первую для расчета
        net_key = normalize_network_key(network)
        
        # Если вдруг выбранного города нет в этой сети, ищем первую подходящую
        if not all_cities.get(city, {}).get(net_key):
            for n in networks:
                if all_cities.get(city, {}).get(normalize_network_key(n)):
                    network = n
                    net_key = normalize_network_key(n)
                    break

        chat_id = all_cities[city][net_key][0]["chat_id"]
        
        cheap_stars_text = (
            "<b>💡 Лайфхак: Как купить звёзды ДЕШЕВЛЕ официального курса?</b>\n\n"
            "Перед оплатой рекомендуем приобрести звёзды через проверенный сервис. "
            "Это выйдет значительно выгоднее, чем покупать их напрямую через Telegram.\n\n"
            "<b>Инструкция:</b>\n"
            "1️⃣ Перейти: <a href='https://t.me/Avrrorkastarbot?start=7924963993'>Купить звезды дешево</a>\n"
            "2️⃣ Нажать кнопку «⭐️ Купить звезды»\n"
            "3️⃣ Выбрать пункт «👤 Себе»\n"
            "4️⃣ Выбрать пакет звезд для вашего тарифа\n"
            "5️⃣ Оплатите удобным способом\n\n"
            "После покупки возвращайтесь сюда и выбирайте тариф ниже! 👇"
        )
        try: bot.send_message(message.chat.id, cheap_stars_text, parse_mode="HTML", disable_web_page_preview=True)
        except: pass

        # 👇 ПОДНЯЛИ ПЕРЕМЕННУЮ СЮДА 👇
        callback_net_key = "all" if selected_network == "Все сети" else net_key

        markup = types.InlineKeyboardMarkup(row_width=1)
        markup.add(types.InlineKeyboardButton("🎫 У меня есть промокод", callback_data=f"ad_promo_{callback_net_key}_{city}"))
        
        active_subs_count = len(ad_subs_collection.distinct("network", {"user_id": user_id, "city": city, "end_date": {"$gt": now_ekb()}}))
        buying_now = (len([n for n, d in all_cities[city].items() if d]) - active_subs_count) if selected_network == "Все сети" else 1
        total_nets = (active_subs_count + buying_now)
        
        discount = 0
        if total_nets == 3: discount = 10
        elif total_nets == 4: discount = 20
        elif total_nets >= 5: discount = 30

        # --- Достаем VIP статус для наценки ---
        user_data = db['users'].find_one({"_id": user_id})
        is_vip = user_data.get("temp_ad_type") == "vip" if user_data else False
        markup_multiplier = 1.5 if is_vip else 1.0 # 👈 НАЦЕНКА +50%

        # (Отсюда строку callback_net_key = ... удаляем, она теперь сверху)

        for days in [1, 7, 15, 30]:
            base_p = get_price_for_chat(chat_id, days)
            if base_p:
                # Умножаем на 1.5 ДО применения скидок за мульти-сеть
                base_p = int(base_p * markup_multiplier)
                
                f_price = int((base_p * buying_now) * (1 - discount / 100))
                pin_p = int(f_price * 1.2)
                
                btn_prefix = "🔥 VIP:" if is_vip else "💳"
                btn_t = f"{btn_prefix} {days} дн. (-{discount}% за {f_price}⭐️)" if discount > 0 else f"{btn_prefix} {days} дн. ({f_price}⭐️)"
                
                # 👇 МЕНЯЕМ net_key НА callback_net_key 👇
                markup.row(
                    types.InlineKeyboardButton(btn_t, callback_data=f"ad_pay_{days}_{callback_net_key}_{city}"),
                    types.InlineKeyboardButton(f"📌 +Закреп ({pin_p}⭐️)", callback_data=f"ad_paypin_{days}_{callback_net_key}_{city}")
                )

        bot.send_message(
            message.chat.id,
            f"⛔ У вас нет доступа к публикации в <b>{escape_html(selected_network)}</b> ({escape_html(city)}).\n\nПриобретите доступ:",
            reply_markup=markup,
            parse_mode="HTML"
        )
        log_failed_attempt(user_id, selected_network, city, "Нет доступа")
        return # Останавливаем процесс, ждем оплату

    # ЕСЛИ ОПЛАТА ЕСТЬ — ИДЕМ ДАЛЬШЕ И ПРОСИМ ТЕКСТ
    bot.send_message(message.chat.id, f"✅ Доступ подтверждён!\n\nНапишите текст объявления для <b>{selected_network} ({city})</b>:", parse_mode="HTML", reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(message, process_text_step, selected_network, city)

def process_text_step(message, selected_network, city):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Вы вернулись в главное меню.", reply_markup=get_main_keyboard())
        return

    text = message.text or message.caption or ""
    if not text:
        bot.send_message(message.chat.id, "❌ Ошибка! Сначала отправьте ТЕКСТ объявления:")
        bot.register_next_step_handler(message, process_text_step, selected_network, city)
        return

    # Проверка стоп-слов
    sub = ad_subs_collection.find_one({"user_id": message.from_user.id, "city": city, "network": {"$in": ["Все сети", selected_network]}, "end_date": {"$gt": now_ekb()}})
    can_post_links = sub.get("can_post_links", False) if sub else False
    is_bad, trigger_word = check_stop_words(text, ignore_black_zone=can_post_links)
    
    if is_bad:
        bot.send_message(message.chat.id, f"❌ <b>Объявление отклонено!</b>\n\nВ тексте найдено запрещенное слово: <b>{trigger_word}</b>\n\nИсправьте текст и отправьте заново:", parse_mode="HTML")
        bot.register_next_step_handler(message, process_text_step, selected_network, city)
        return

    # 1. Сохраняем чистый текст во временную корзину MongoDB
    db['users'].update_one({"_id": message.from_user.id}, {"$set": {"temp_ad_text": text, "temp_ad_media": []}}, upsert=True)

    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    markup.add("✅ Все файлы загружены. Далее")
    markup.add("Назад")
    
    bot.send_message(message.chat.id, "📸 Теперь отправьте фото или видео (до 10 штук).\n<i>Если медиа не нужно, просто нажмите кнопку ниже 👇</i>", parse_mode="HTML", reply_markup=markup)
    bot.register_next_step_handler(message, process_ad_media_loop, selected_network, city)


def process_ad_media_loop(message, selected_network, city):
    uid = message.from_user.id

    if message.text == "✅ Все файлы загружены. Далее":
        user_data = db['users'].find_one({"_id": uid})
        text = user_data.get("temp_ad_text", "")
        media = user_data.get("temp_ad_media", [])
        
        media_type = None
        file_id = None
        
        if len(media) == 1:
            media_type = media[0]['type']
            file_id = media[0]['id']
        elif len(media) > 1:
            media_type = "album"
            file_id = "album_data" # Просто метка

        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=1)
        markup.add("✅ Опубликовать разово (сейчас)")
        markup.add("🔁 Настроить Автопубликацию")
        markup.add("❌ Нет, изменить текст")
        bot.send_message(message.chat.id, f"Ваш текст:\n{text}\n\nВсё верно?", reply_markup=markup)
        bot.register_next_step_handler(message, handle_confirmation_step, text, media_type, file_id, selected_network, city)
        return

    if message.text == "Назад":
        bot.send_message(message.chat.id, "Создание отменено.", reply_markup=get_main_keyboard())
        return

    # Продолжаем слушать чат
    bot.register_next_step_handler(message, process_ad_media_loop, selected_network, city)

    media_item = None
    if message.photo: media_item = {"type": "photo", "id": message.photo[-1].file_id}
    elif message.video: media_item = {"type": "video", "id": message.video.file_id}

    if media_item:
        user_data = db['users'].find_one({"_id": uid})
        current_media = user_data.get('temp_ad_media', [])
        
        if len(current_media) >= 10:
            if not message.media_group_id: # Не спамим, если это один большой альбом
                bot.send_message(message.chat.id, "🚫 Лимит 10 файлов исчерпан! Жмите «Далее».")
        else:
            db['users'].update_one({"_id": uid}, {"$push": {"temp_ad_media": media_item}})
            if not message.media_group_id:
                bot.send_message(message.chat.id, f"📥 Файл принят ({len(current_media) + 1}/10)")

def handle_confirmation_step(message, text, media_type, file_id, selected_network, city):
    if message.text == "❌ Нет, изменить текст" or message.text.lower() == "нет, изменить текст":
        bot.send_message(message.chat.id, "Хорошо, напишите текст объявления заново:")
        bot.register_next_step_handler(message, process_text_step, selected_network, city)
        return

    # 👇 ЛОГИКА ВЫБОРА ИНТЕРВАЛА 👇
    if message.text == "🔁 Настроить Автопубликацию":
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True, row_width=4)
        markup.add("4", "6", "8", "12")
        markup.add("Отмена")
        bot.send_message(message.chat.id, "⏱ <b>Настройка автопостинга:</b>\nЧерез сколько часов автоматически повторять пост?\n\n<i>Нажмите кнопку или напишите цифру вручную (от 1 до 24):</i>", parse_mode="HTML", reply_markup=markup)
        bot.register_next_step_handler(message, process_autopost_interval, text, media_type, file_id, selected_network, city)
        return

    user_id = message.from_user.id
    user_name = f'<b>{get_user_html_link(message.from_user)}</b>'
    text = escape_html(text)
    networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства"] if selected_network == "Все сети" else [selected_network]

    was_published = False

    for network in networks:
        net_key = normalize_network_key(network)
        city_data = all_cities.get(city, {}).get(net_key)

        if not city_data: continue

        # --- Проверка лимитов ---
        user_stats = get_user_statistics(user_id)
        city_stats = user_stats.get("details", {}).get(network, {}).get(city, {})

        if city_stats.get("remaining", 0) <= 0:
            bot.send_message(message.chat.id, f"⛔ Лимит публикаций на сегодня исчерпан для <b>{escape_html(network)}</b> ({escape_html(city)})", parse_mode="HTML")
            continue

        # --- Проверка на ссылки (Наша новая логика) ---
        sub = ad_subs_collection.find_one({"user_id": user_id, "city": city, "network": {"$in": ["Все сети", network]}, "end_date": {"$gt": now_ekb()}})
        can_post_links = sub.get("can_post_links", False) if sub else False

        if not can_post_links:
            has_links = bool(re.search(r'(t\.me/|@\w+|http)', text.lower()))
            if has_links:
                bot.send_message(message.chat.id, "❌ Ссылки и @username запрещены! Уберите их из текста.")
                ask_for_new_post(message)
                return

        # --- Публикация ---
        signature = network_signatures.get(network, "")
        full_text = f"📢 Объявление от {user_name}:\n\n{text}\n\n{signature}"
        
        reply_markup = types.InlineKeyboardMarkup()
        reply_markup.add(types.InlineKeyboardButton(text="Напиши мне в ЛС", url=f"tg://user?id={user_id}", style="success", icon_custom_emoji_id="5470060791883374114"))

        for location in city_data:
            chat_id = location["chat_id"]
            try:
                if media_type == "album":
                    # Забираем корзину с медиа из базы
                    user_data = db['users'].find_one({"_id": user_id})
                    media_array = user_data.get("temp_ad_media", [])
                    
                    media_list = []
                    for m in media_array:
                        if m['type'] == 'photo': media_list.append(types.InputMediaPhoto(m['id']))
                        else: media_list.append(types.InputMediaVideo(m['id']))
                    
                    # 1. Отправляем альбом (ТГ запрещает кнопки на альбомах)
                    bot.send_media_group(chat_id, media_list)
                    # 2. Следом кидаем текст с Inline-кнопкой ЛС
                    sent_msg = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)
                    main_msg_id = sent_msg.message_id
                    
                elif media_type == "photo": 
                    sent_msg = bot.send_photo(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                    main_msg_id = sent_msg.message_id
                elif media_type == "video": 
                    sent_msg = bot.send_video(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                    main_msg_id = sent_msg.message_id
                else: 
                    sent_msg = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)
                    main_msg_id = sent_msg.message_id

                # Пишем в историю
                add_post_to_history(user_id, message.from_user.first_name or "Без имени", network, location['name'], chat_id, main_msg_id)
                bot.send_message(message.chat.id, f"✅ Опубликовано в <b>{network}</b> ({location['name']}).", parse_mode="HTML")
                was_published = True

                # Проверка на закрепление
                if sub and sub.get("has_pin"):
                    try: bot.pin_chat_message(chat_id, main_msg_id, disable_notification=True)
                    except: pass

            except telebot.apihelper.ApiTelegramException as e:
                bot.send_message(message.chat.id, f"❌ Ошибка в {network}: {e.description}")

    ask_for_new_post(message)

def process_autopost_interval(message, text, media_type, file_id, selected_network, city):
    if message.text == "Отмена":
        bot.send_message(message.chat.id, "Настройка автопоста отменена.", reply_markup=get_main_keyboard())
        return
        
    try:
        interval = int(message.text)
        if interval < 1 or interval > 24:
            raise ValueError
    except ValueError:
        bot.send_message(message.chat.id, "❌ Пожалуйста, введите корректную цифру от 1 до 24.")
        bot.register_next_step_handler(message, process_autopost_interval, text, media_type, file_id, selected_network, city)
        return
        
    user_id = message.from_user.id
    
    # 1. Публикуем ПЕРВЫЙ пост прямо сейчас (перенаправляем обратно в твою оригинальную функцию)
    message.text = "✅ Опубликовать разово (сейчас)"
    handle_confirmation_step(message, text, media_type, file_id, selected_network, city)
    
    media_array = []
    if media_type == "album":
        media_array = db['users'].find_one({"_id": user_id}).get("temp_ad_media", [])

    autopost_queue.insert_one({
        "user_id": user_id,
        "network": selected_network,
        "city": city,
        "text": text,
        "media_type": media_type,
        "file_id": file_id,
        "media_array": media_array, # <--- ТЕПЕРЬ СОХРАНЯЕТ АЛЬБОМ
        "interval_hours": interval,
        "posts_left": 2, 
        "next_run": now_ekb() + timedelta(hours=interval)
    })
    
    bot.send_message(message.chat.id, f"🔁 <b>Автопостинг включен!</b>\n\nПервый пост только что вышел. Следующие 2 поста выйдут автоматически с интервалом в <b>{interval} ч.</b>", parse_mode="HTML")

def ask_for_new_post(message):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Да", "Нет")
    bot.send_message(message.chat.id, "Хотите создать ещё одно объявление?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_new_post_choice)

def handle_new_post_choice(message):
    if message.text.lower() == "да":
        bot.send_message(message.chat.id, "Напишите текст объявления:")
        bot.register_next_step_handler(message, process_text)
    else:
        bot.send_message(
            message.chat.id,
            "Спасибо за использование бота! 🙌\nЕсли хотите создать новое объявление, нажмите кнопку ниже.",
            reply_markup=get_main_keyboard()
        )

@bot.message_handler(func=lambda message: message.text == "📊 Моя статистика")
def handle_stats_button(message):
    try:
        user_id = message.from_user.id
        stats = get_user_statistics(user_id)

        response = (f"📊 <b>Ваша статистика на сегодня:</b>\n"
                    f"📨 Опубликовано: <b>{stats['published']}</b>\n"
                    f"📉 Осталось публикаций: <b>{stats['remaining']}</b>\n")

        if stats["details"]:
            response += "\n🗂️ <b>Детали по сетям и городам:</b>\n"
            for network, cities in stats["details"].items():
                for city, data in cities.items():
                    # Ищем подписку в Mongo
                    sub = ad_subs_collection.find_one({"user_id": user_id, "network": {"$in": ["Все сети", network]}, "city": city})
                    expire_str = f"⏳ до {sub['end_date'].strftime('%d.%m.%Y')}" if sub else "(Срок истек)"

                    response += (f"  └ 🧩 <b>{network}</b>, 📍<b>{city}</b> {expire_str}:\n"
                                 f"     • Опубликовано: <b>{data['published']}</b>, Осталось: <b>{data['remaining']}</b>\n")

        bot.send_message(message.chat.id, response, parse_mode="HTML")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Произошла ошибка при получении статистики: {e}")

# --- АДМИНСКОЕ УДАЛЕНИЕ ---
def delete_user_posts_step(message):
    try:
        user_id = int(message.text)
        posts = list(ad_posts_collection.find({"user_id": user_id, "deleted": False}))

        if not posts:
            bot.send_message(message.chat.id, "❌ У пользователя нет активных объявлений.")
            return

        preview = f"📋 Найдено <b>{len(posts)}</b> объявлений у ID <code>{user_id}</code>:\n\n"
        for post in posts:
            date_str = format_time(post["time"])
            preview += f"• 🧩 <b>{post['network']}</b> | 📍<b>{post['city']}</b> | 🕒 {date_str}\n"

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("✅ Удалить все", callback_data=f"confirm_delete_{user_id}"))
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="cancel_delete"))

        bot.send_message(message.chat.id, preview, reply_markup=markup, parse_mode="HTML")

    except ValueError:
        bot.send_message(message.chat.id, "❌ Введите корректный числовой ID.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete_") or call.data == "cancel_delete")
def handle_delete_confirmation(call):
    if call.data == "cancel_delete":
        bot.edit_message_text("❌ Удаление отменено.", call.message.chat.id, call.message.message_id)
        return

    user_id = int(call.data.split("_")[-1])
    posts = list(ad_posts_collection.find({"user_id": user_id, "deleted": False}))
    deleted = 0

    for post in posts:
        try:
            bot.delete_message(post["chat_id"], post["message_id"])
            deleted += 1
        except: pass

        ad_posts_collection.update_one({"_id": post["_id"]}, {"$set": {"deleted": True, "deleted_by": "Админ"}})

    bot.edit_message_text(f"✅ Удалено {deleted} объявлений пользователя ID: <code>{user_id}</code>.", call.message.chat.id, call.message.message_id, parse_mode="HTML")

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode('utf-8'))
    bot.process_new_updates([update])
    return 'ok', 200

@app.route('/')
def index():
    return '✅ Бот запущен и работает!'

@app.route('/crypto_webhook', methods=['POST'])
def crypto_webhook():
    """Слушатель оплат из CryptoBot"""
    try:
        data = request.json
        # CryptoBot присылает событие invoice_paid, когда счет оплачен
        if data and data.get("update_type") == "invoice_paid":
            # Достаем наш спрятанный payload (он выглядит как payload___user_id)
            invoice_payload = data["payload"]["payload"] 
            amount_rub = data["payload"]["amount"]
            
            # Разделяем строку на оригинальный payload и ID юзера
            parts = invoice_payload.split("___")
            if len(parts) != 2:
                return 'ok', 200
            
            original_payload = parts[0]
            user_id = int(parts[1])
            
            # === ПОВТОРЯЕМ ЛОГИКУ ВЫДАЧИ ПРАВ ИЗ successful_payment ===
            has_pin = "_pin" in original_payload 
            is_vip = "_vip" in original_payload
            
            clean_payload = original_payload.replace("ad_access_vip_", "").replace("ad_access_", "").replace("_pin", "")
            p_parts = clean_payload.split('_')
            
            if p_parts[0] == "discount":
                days = int(p_parts[1])
                net_key = p_parts[2]
                city = p_parts[3]
                promo_code = p_parts[4]
                promocodes_collection.update_one({"_id": promo_code}, {"$inc": {"used_count": 1}})
            else:
                days = int(p_parts[0])
                net_key = p_parts[1]
                city = p_parts[2]

            names = {"mk": "Мужской Клуб", "parni": "ПАРНИ 18+", "ns": "НС", "rainbow": "Радуга", "gayznak": "Гей Знакомства", "all": "Все сети"}
            network = names.get(net_key, net_key)

            end_date = now_ekb() + timedelta(days=days)

            # 1. Записываем доступ в базу
            ad_subs_collection.insert_one({
                "user_id": user_id,
                "network": network,
                "city": city,
                "end_date": end_date,
                "purchase_date": now_ekb(),
                "has_pin": has_pin,
                "can_post_links": is_vip, 
                "notified_72h": True if days <= 3 else False,
                "notified_24h": True if days <= 1 else False,
                "notified_3h": False
            })
            
            # 2. Очищаем корзину
            db['users'].update_one({"_id": user_id}, {"$unset": {"temp_ad_type": ""}})
            
            # 3. Пишем в бухгалтерию (чтобы видеть доходы в крипте)
            db['daily_revenue'].insert_one({
                "type": "ads_crypto", 
                "amount": float(amount_rub), 
                "timestamp": time.time(), 
                "date": now_ekb().strftime("%d.%m.%Y")
            })

            # 4. Уведомление админу
            try: bot.send_message(ADMIN_CHAT_ID, f"🟢 <b>КРИПТО-ОПЛАТА!</b>\nЮзер: <code>{user_id}</code>\nСеть: <b>{network}</b>\nГород: <b>{city}</b>\nСрок: <b>{days}</b> дн.", parse_mode="HTML")
            except: pass

            # 5. Сообщение счастливому рекламодателю
            try: bot.send_message(user_id, f"✅ <b>Крипто-оплата успешно получена!</b>\n\nДоступ к сети <b>{network}</b> ({city}) открыт на {days} дней.\nЖмите кнопку ниже, чтобы разместить пост!", parse_mode="HTML", reply_markup=get_main_keyboard())
            except: pass

    except Exception as e:
        print(f"Ошибка Webhook CryptoBot: {e}")

    return 'ok', 200

def is_user_paid(user_id, network, city):
    """Проверяет доступ пользователя через MongoDB"""
    # Ищем все активные подписки пользователя для конкретной сети/города
    current_time = now_ekb()
    
    # Ищем документы в Mongo
    query = {
        "user_id": user_id,
        "city": city,
        "network": {"$in": ["Все сети", network]},
        "end_date": {"$gt": current_time} # Берем только те, где срок еще не истек
    }
    
    active_subscription = ad_subs_collection.find_one(query)
    
    if active_subscription:
        return True
        
    return False

@bot.pre_checkout_query_handler(func=lambda query: query.invoice_payload.startswith("ad_access_"))
def checkout_process(pre_checkout_query):
    bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def successful_payment(message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload
    # 👇 НОВАЯ СТРОЧКА (Достаем сумму) 👇
    amount = message.successful_payment.total_amount

    if payload.startswith("ad_access_"):
        # 👇 НОВАЯ СТРОЧКА (Пишем в копилку) 👇
        db['daily_revenue'].insert_one({"type": "ads", "amount": amount, "timestamp": time.time(), "date": now_ekb().strftime("%d.%m.%Y")})

        has_pin = "_pin" in payload 
        is_vip = "_vip" in payload # Проверяем VIP
        
        # Очищаем строку от всех префиксов и суффиксов
        clean_payload = payload.replace("ad_access_vip_", "").replace("ad_access_", "").replace("_pin", "")
        parts = clean_payload.split('_')
        
        # Теперь первый элемент (parts[0]) всегда либо "discount", либо количество дней
        if parts[0] == "discount":
            days = int(parts[1])
            net_key = parts[2]
            city = parts[3]
            promo_code = parts[4]
            promocodes_collection.update_one({"_id": promo_code}, {"$inc": {"used_count": 1}})
        else:
            days = int(parts[0])
            net_key = parts[1]
            city = parts[2]

        # 💎 ПЕРЕВОДЧИК: Возвращаем красивое имя перед записью в базу!
        names = {"mk": "Мужской Клуб", "parni": "ПАРНИ 18+", "ns": "НС", "rainbow": "Радуга", "gayznak": "Гей Знакомства", "all": "Все сети"}
        network = names.get(net_key, net_key)

        end_date = now_ekb() + timedelta(days=days)

        # Проверяем, была ли это VIP-оплата
        is_vip = "_vip_" in payload

        # 💥 ЗАПИСЬ В БАЗУ ДАННЫХ
        ad_subs_collection.insert_one({
            "user_id": user_id,
            "network": network,
            "city": city,
            "end_date": end_date,
            "purchase_date": now_ekb(),
            "has_pin": has_pin,
            "can_post_links": is_vip, # 👈 Автоматически разрешаем ссылки для VIP!
            "notified_72h": True if days <= 3 else False,
            "notified_24h": True if days <= 1 else False,
            "notified_3h": False
        })
        
        # Сбрасываем временный статус, чтобы следующая покупка не была с наценкой
        db['users'].update_one({"_id": user_id}, {"$unset": {"temp_ad_type": ""}})

        # Уведомление админу
        try:
            bot.send_message(ADMIN_CHAT_ID, f"💰 **Новая продажа!**\nЮзер: <code>{user_id}</code>\nСеть: <b>{network}</b>\nГород: <b>{city}</b>\nСрок: <b>{days}</b> дн.", parse_mode="HTML")
        except: pass

        # Сообщение юзеру
        try:
            bot.send_message(user_id, f"✅ <b>Оплата успешно получена!</b>\n\nДоступ к сети <b>{network}</b> ({city}) открыт на {days} дней.\nЖмите кнопку ниже, чтобы разместить пост!", parse_mode="HTML", reply_markup=get_main_keyboard())
        except: pass

# ================= ПРОМОКОДЫ ДЛЯ РЕКЛАМЫ =================
@bot.callback_query_handler(func=lambda call: call.data.startswith('ad_promo_'))
def handle_ad_promo(call):
    bot.answer_callback_query(call.id) # 🛑 Снимаем залипание!
    
    parts = call.data.split('_')
    network = parts[2] # РОДНОЕ ПОЛНОЕ НАЗВАНИЕ
    city = parts[3]
    
    msg = bot.send_message(call.message.chat.id, "👇 <b>Введите ваш промокод ответом на это сообщение:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_ad_promo, network, city)

def process_ad_promo(message, network, city):
    promo_text = message.text.strip().upper()
    promo_data = promocodes_collection.find_one({"_id": promo_text})
    
    if not promo_data or not promo_data.get("is_active"):
        bot.send_message(message.chat.id, "❌ Промокод не найден или уже недействителен.")
        return
        
    if promo_data["used_count"] >= promo_data.get("usage_limit", 1):
        bot.send_message(message.chat.id, "❌ Лимит активаций этого промокода исчерпан.")
        return
        
    if promo_data.get("target") not in ["all", "ads"]:
        bot.send_message(message.chat.id, "❌ Этот промокод нельзя применить к покупке рекламы.")
        return

    # Сохраняем введенный промокод временно в профиль юзера
    db['users'].update_one({"_id": message.from_user.id}, {"$set": {"temp_promo": promo_text}}, upsert=True)

    discount_promo = promo_data["value"]
    markup = types.InlineKeyboardMarkup(row_width=1)
    
    # --- Достаем VIP статус для наценки ---
    user_data = db['users'].find_one({"_id": message.from_user.id})
    is_vip = user_data.get("temp_ad_type") == "vip" if user_data else False
    markup_multiplier = 1.5 if is_vip else 1.0

    discount_net = 0  # 👈 ДОБАВЛЯЕМ ПЕРЕМЕННУЮ ДЛЯ СКИДКИ ЗА ПАКЕТ СЕТЕЙ

    if network == "all":
        first_avail_net = list(all_cities[city].keys())[0]
        chat_id = all_cities[city][first_avail_net][0]["chat_id"]
        
        active_subs_count = len(ad_subs_collection.distinct("network", {"user_id": message.from_user.id, "city": city, "end_date": {"$gt": now_ekb()}}))
        buying_now = len([n for n, d in all_cities[city].items() if d]) - active_subs_count
        
        # 👇 ВОССТАНАВЛИВАЕМ РАСЧЕТ ПАКЕТНОЙ СКИДКИ 👇
        total_nets = active_subs_count + buying_now
        if total_nets == 3: discount_net = 10
        elif total_nets == 4: discount_net = 20
        elif total_nets >= 5: discount_net = 30
    else:
        chat_id = all_cities[city][network][0]["chat_id"]
        buying_now = 1

    for days in [1, 7, 15, 30]:
        base_p = get_price_for_chat(chat_id, days)
        if base_p:
            base_p = int(base_p * markup_multiplier)
            
            # 👇 ТЕПЕРЬ СКИДКИ ПРИМЕНЯЮТСЯ КАСКАДОМ 👇
            # 1. Пакетная скидка (Все сети)
            price_with_net_discount = int((base_p * buying_now) * (1 - discount_net / 100))
            
            # 2. Скидка по промокоду
            f_price = int(price_with_net_discount * (1 - discount_promo / 100))
            
            pin_p = int(f_price * 1.2)
            
            btn_prefix = "🔥 VIP:" if is_vip else "💳"
            
            markup.row(
                types.InlineKeyboardButton(f"{btn_prefix} {days} дн. (-{discount_promo}% за {f_price}⭐️)", callback_data=f"ad_pay_{days}_{network}_{city}"),
                types.InlineKeyboardButton(f"📌 +Закреп ({pin_p}⭐️)", callback_data=f"ad_paypin_{days}_{network}_{city}")
            )
            
    bot.send_message(message.chat.id, f"✅ <b>Промокод применен!</b> Выберите тариф:", reply_markup=markup, parse_mode="HTML")

# --- БЫСТРОЕ ПРОДЛЕНИЕ ИЗ УВЕДОМЛЕНИЙ ---
@bot.callback_query_handler(func=lambda call: call.data.startswith("renew_"))
def handle_renew_request(call):
    parts = call.data.split('_')
    net_key = parts[1]
    city = parts[2]

    names = {"mk": "Мужской Клуб", "parni": "ПАРНИ 18+", "ns": "НС", "rainbow": "Радуга", "gayznak": "Гей Знакомства"}
    network = names.get(net_key, net_key)

    try:
        chat_id = all_cities[city][net_key][0]["chat_id"]
    except KeyError:
        bot.answer_callback_query(call.id, "❌ Ошибка: Город или сеть больше не существуют.")
        return

    markup = types.InlineKeyboardMarkup(row_width=1)

    # Достаем актуальные цены и генерируем кнопки оплаты
    for days in [1, 7, 15, 30]:
        base_price = get_price_for_chat(chat_id, days)
        if base_price:
            pin_price = int(base_price * 1.2)
            markup.row(
                types.InlineKeyboardButton(f"💳 {days} дн. ({base_price}⭐️)", callback_data=f"ad_pay_{days}_{net_key}_{city}"),
                types.InlineKeyboardButton(f"📌 +Закреп ({pin_price}⭐️)", callback_data=f"ad_paypin_{days}_{net_key}_{city}")
            )

    bot.edit_message_text(
        f"♻️ <b>Продление рекламы:</b> {network} ({city})\n\nВыберите новый тарифный план:",
        call.message.chat.id, 
        call.message.message_id, 
        reply_markup=markup, 
        parse_mode="HTML"
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith('ad_pay_') or call.data.startswith('ad_paypin_'))
def handle_ad_checkout(call):
    bot.answer_callback_query(call.id)
    
    is_pin = call.data.startswith('ad_paypin_')
    parts = call.data.split('_')
    
    days = int(parts[2])
    net_key = parts[3] 
    city = parts[4]
    
    # --- VIP Наценка и Промо ---
    user_data = db['users'].find_one({"_id": call.from_user.id})
    is_vip = user_data.get("temp_ad_type") == "vip" if user_data else False
    temp_promo = user_data.get("temp_promo") if user_data else None

    if net_key == "all":
        network = "Все сети"
        # Для базовой цены берем первый доступный чат в этом городе
        first_avail_net = list(all_cities[city].keys())[0]
        chat_id = all_cities[city][first_avail_net][0]["chat_id"]
        base_price = get_price_for_chat(chat_id, days)
        
        if is_vip:
            base_price = int(base_price * 1.5)
            
        # Воспроизводим расчет скидки для "Всех сетей"
        active_subs_count = len(ad_subs_collection.distinct("network", {"user_id": call.from_user.id, "city": city, "end_date": {"$gt": now_ekb()}}))
        buying_now = len([n for n, d in all_cities[city].items() if d]) - active_subs_count
        total_nets = active_subs_count + buying_now
        
        discount_net = 0
        if total_nets == 3: discount_net = 10
        elif total_nets == 4: discount_net = 20
        elif total_nets >= 5: discount_net = 30
        
        final_price = int((base_price * buying_now) * (1 - discount_net / 100))
        amount = int(final_price * 1.2) if is_pin else final_price

    else:
        names = {"mk": "Мужской Клуб", "parni": "ПАРНИ 18+", "ns": "НС", "rainbow": "Радуга", "gayznak": "Гей Знакомства"}
        network = names.get(net_key, net_key)

        chat_id = all_cities[city][net_key][0]["chat_id"]
        base_price = get_price_for_chat(chat_id, days)
        
        if is_vip: base_price = int(base_price * 1.5)
        
        amount = int(base_price * 1.2) if is_pin else base_price
    
    # 👇 ВОТ ЭТОТ БЛОК МЫ СЛУЧАЙНО ЗАТЕРЛИ ПРОШЛЫЙ РАЗ 👇
    # --- Вшиваем VIP метку в payload ---
    payload_base = "ad_access_vip" if is_vip else "ad_access"
    payload = f"{payload_base}_discount_{days}_{net_key}_{city}_{temp_promo}" if temp_promo else f"{payload_base}_{days}_{net_key}_{city}"
    if is_pin: payload += "_pin"
    
    description_text = f"Сеть: {network}\nГород: {city}\nСрок: {days} дн."
    
    if temp_promo:
        promo_data = promocodes_collection.find_one({"_id": temp_promo})
        if promo_data:
            discount = promo_data["value"]
            amount = int(amount * (1 - discount / 100))
            description_text += f"\n🎁 Промокод: {temp_promo} (-{discount}%)"
            
        db['users'].update_one({"_id": call.from_user.id}, {"$unset": {"temp_promo": ""}})
    # 👆 КОНЕЦ ВОССТАНОВЛЕННОГО БЛОКА 👆

    # 👇 ГЕНЕРИРУЕМ КРИПТО-ССЫЛКИ 👇
    crypto_payload = f"{payload}___{call.from_user.id}"
    url_usdt = get_crypto_pay_url(crypto_payload, amount, f"Реклама: {network} ({city})", asset="USDT")
    url_ton = get_crypto_pay_url(crypto_payload, amount, f"Реклама: {network} ({city})", asset="TON")

    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton(text=f"⭐️ Оплатить {amount} Звезд", pay=True))
    
    if url_usdt:
        markup.add(types.InlineKeyboardButton("🟢 Оплатить через USDT (CryptoBot)", url=url_usdt))
    if url_ton:
        markup.add(types.InlineKeyboardButton("💎 Оплатить через TON (CryptoBot)", url=url_ton))

    # 👇 НОВАЯ КНОПКА 👇
    markup.add(types.InlineKeyboardButton("💳 Проблема с оплатой/Альтернатива", callback_data=f"ad_altpay_{amount}_{days}_{net_key}_{city}"))


    bot.send_invoice(
        call.message.chat.id, 
        title="Доступ + ЗАКРЕП 📌" if is_pin else "Доступ к публикации 📢", 
        description=description_text, 
        invoice_payload=payload, 
        provider_token="", 
        currency="XTR", 
        prices=[types.LabeledPrice(label="Рекламный доступ", amount=amount)],
        reply_markup=markup
    )

# 👇 ВСТАВЛЯЕМ СЮДА 👇
@bot.callback_query_handler(func=lambda call: call.data.startswith('ad_altpay_'))
def handle_alternative_payment(call):
    bot.answer_callback_query(call.id)
    parts = call.data.split('_')
    amount = int(parts[2])
    days = parts[3]
    net_key = parts[4]
    city = parts[5]
    
    names = {"mk": "Мужской Клуб", "parni": "ПАРНИ 18+", "ns": "НС", "rainbow": "Радуга", "gayznak": "Гей Знакомства", "all": "Все сети"}
    network = names.get(net_key, net_key)
    
    # 🔥 ТА САМАЯ МАФИОЗНАЯ ФОРМУЛА ИЗ СКАЙНЕТА 🔥
    rub_amount = int(round(amount * 1.65 * 1.1))
    
    text = (
        f"💳 <b>Запрос на альтернативную оплату (Карта / СБП)</b>\n\n"
        f"Оплатить рекламу можно на одноразовый технологический номер телефона. Сумма рассчитывается по формуле:\n"
        f"{amount} звезд * 1.65 (курс 1 звезды) + 10% комиссии банка за пополнение = <b>{rub_amount}₽</b>\n\n"
        f"<b>Ваш заказ:</b>\n"
        f"🌐 Сеть: <b>{network}</b>\n"
        f"📍 Город: <b>{city}</b>\n"
        f"⏳ Срок: <b>{days} дн.</b>\n\n"
        f"👇 <i>Пожалуйста, перешлите это сообщение (чек) в нашу Поддержку, и дежурный администратор выдаст вам реквизиты!</i>"
    )
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💬 Получить реквизиты в Поддержке", url="https://t.me/FAQMKBOT"))
    
    try: bot.delete_message(call.message.chat.id, call.message.message_id)
    except: pass
    bot.send_message(call.message.chat.id, text, parse_mode="HTML", reply_markup=markup)
# 👆 КОНЕЦ ВСТАВКИ 👆

# --- ФОНОВЫЕ ЗАДАЧИ ---
def check_expiring_subs():
    """Фоновая задача: ищет подписки, истекающие через 72, 24 и 3 часа"""
    reverse_names = {"Мужской Клуб": "mk", "ПАРНИ 18+": "parni", "НС": "ns", "Радуга": "rainbow", "Гей Знакомства": "gayznak"}

    while True:
        try:
            now = now_ekb()
            
            # --- Функция генерации кнопки продления ---
            def get_renew_markup(network_name, city):
                net_key = reverse_names.get(network_name, "mk")
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton("♻️ Продлить подписку", callback_data=f"renew_{net_key}_{city}"))
                return markup

            # 1. Проверка за 72 часа (3 дня)
            target_72 = now + timedelta(hours=72)
            expiring_72 = ad_subs_collection.find({"end_date": {"$gte": target_72 - timedelta(hours=1), "$lte": target_72 + timedelta(hours=1)}, "notified_72h": {"$ne": True}})
            for sub in expiring_72:
                try:
                    bot.send_message(sub['user_id'], f"⏳ <b>Мягкое напоминание:</b>\nВаша подписка на <b>{sub['network']}</b> ({sub['city']}) истекает через 3 дня.\nПодготовьтесь к продлению, чтобы не терять клиентов!", parse_mode="HTML", reply_markup=get_renew_markup(sub['network'], sub['city']))
                    ad_subs_collection.update_one({"_id": sub["_id"]}, {"$set": {"notified_72h": True}})
                except: pass

            # 2. Проверка за 24 часа
            target_24 = now + timedelta(hours=24)
            expiring_24 = ad_subs_collection.find({"end_date": {"$gte": target_24 - timedelta(hours=1), "$lte": target_24 + timedelta(hours=1)}, "notified_24h": {"$ne": True}})
            for sub in expiring_24:
                try:
                    bot.send_message(sub['user_id'], f"🚨 <b>Остались ровно 1 сутки!</b>\nВаш доступ к <b>{sub['network']}</b> ({sub['city']}) закончится через 24 часа!\n\nЖмите кнопку ниже, чтобы продлить доступ в пару кликов 👇", parse_mode="HTML", reply_markup=get_renew_markup(sub['network'], sub['city']))
                    ad_subs_collection.update_one({"_id": sub["_id"]}, {"$set": {"notified_24h": True}})
                except: pass

            # 3. НОВОЕ: Экстренная проверка за 3 часа (Для тех, кто брал на 24 часа)
            target_3 = now + timedelta(hours=3)
            expiring_3 = ad_subs_collection.find({"end_date": {"$gte": target_3 - timedelta(hours=1), "$lte": target_3 + timedelta(hours=1)}, "notified_3h": {"$ne": True}})
            for sub in expiring_3:
                try:
                    bot.send_message(sub['user_id'], f"🔥 <b>СГОРАЕТ ЧЕРЕЗ 3 ЧАСА!</b>\nДоступ к <b>{sub['network']}</b> ({sub['city']}) почти истек.\n\nПродлите сейчас, чтобы ваши посты не перестали публиковаться!", parse_mode="HTML", reply_markup=get_renew_markup(sub['network'], sub['city']))
                    ad_subs_collection.update_one({"_id": sub["_id"]}, {"$set": {"notified_3h": True}})
                except: pass

            time.sleep(3600)
        except Exception as e:
            time.sleep(60)

# Запускаем поток
threading.Thread(target=check_expiring_subs, daemon=True).start()

def process_autoposts_worker():
    """Фоновый поток: публикует отложенные посты каждые X часов"""
    while True:
        try:
            now = now_ekb()
            ready_posts = list(autopost_queue.find({"next_run": {"$lte": now}, "posts_left": {"$gt": 0}}))
            
            for post in ready_posts:
                user_id = post['user_id']
                network = post['network']
                city = post['city']
                
                # 1. Проверяем, активна ли еще подписка
                if not is_user_paid(user_id, network, city):
                    autopost_queue.delete_one({"_id": post["_id"]})
                    continue

                # 2. Проверяем лимит 3 поста на сегодня
                user_stats = get_user_statistics(user_id)
                city_stats = user_stats.get("details", {}).get(network, {}).get(city, {})
                if city_stats.get("remaining", 0) <= 0:
                    # Лимит исчерпан. Переносим попытку на завтра (на утро)
                    autopost_queue.update_one(
                        {"_id": post["_id"]},
                        {"$set": {"next_run": now.replace(hour=8, minute=0) + timedelta(days=1)}}
                    )
                    continue

                # 3. Публикация
                try:
                    user_info = bot.get_chat(user_id)
                    user_name = f'<b>{get_user_html_link(user_info)}</b>'
                except:
                    user_name = f'<b><a href="tg://user?id={user_id}">Пользователь</a></b>'
                    
                networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства"] if network == "Все сети" else [network]
                
                for net in networks:
                    net_key = normalize_network_key(net)
                    city_data = all_cities.get(city, {}).get(net_key)
                    if not city_data: continue
                    
                    signature = network_signatures.get(net, "")
                    full_text = f"📢 Объявление от {user_name}:\n\n{post['text']}\n\n{signature}"
                    reply_markup = types.InlineKeyboardMarkup()
                    reply_markup.add(types.InlineKeyboardButton(text="Напиши мне в ЛС", url=f"tg://user?id={user_id}", style="success", icon_custom_emoji_id="5470060791883374114"))
                    
                    for location in city_data:
                        chat_id = location["chat_id"]
                        try:
                            if post['media_type'] == "album":
                                media_list = []
                                for m in post.get('media_array', []):
                                    if m['type'] == 'photo': media_list.append(types.InputMediaPhoto(m['id']))
                                    else: media_list.append(types.InputMediaVideo(m['id']))
                                
                                bot.send_media_group(chat_id, media_list)
                                sent_msg = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)
                            elif post['media_type'] == "photo": 
                                sent_msg = bot.send_photo(chat_id, post['file_id'], caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                            elif post['media_type'] == "video": 
                                sent_msg = bot.send_video(chat_id, post['file_id'], caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                            else: 
                                sent_msg = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)
                            
                            add_post_to_history(user_id, "Автопост", net, location['name'], chat_id, sent_msg.message_id)
                            
                            # Проверяем закреп
                            sub = ad_subs_collection.find_one({"user_id": user_id, "city": city, "network": {"$in": ["Все сети", net]}, "end_date": {"$gt": now_ekb()}})
                            if sub and sub.get("has_pin"):
                                try: bot.pin_chat_message(chat_id, sent_msg.message_id, disable_notification=True)
                                except: pass
                                
                        except Exception as e: pass
                            
                # 4. Обновляем счетчик
                posts_left = post['posts_left'] - 1
                if posts_left > 0:
                    autopost_queue.update_one(
                        {"_id": post["_id"]}, 
                        {"$set": {"posts_left": posts_left, "next_run": now + timedelta(hours=post['interval_hours'])}}
                    )
                else:
                    autopost_queue.delete_one({"_id": post["_id"]})
                    try: bot.send_message(user_id, f"ℹ️ Серия автопубликаций для <b>{network} ({city})</b> завершена (3 из 3 постов вышли).", parse_mode="HTML")
                    except: pass
                    
        except Exception as e: pass
        time.sleep(60) # Проверка базы раз в минуту

# 👇 ЗАПУСКАЕМ НАШ НОВЫЙ ПОТОК (Вставить перед if __name__ == '__main__':)
threading.Thread(target=process_autoposts_worker, daemon=True).start()

if __name__ == '__main__':
    print("✅ Скайнет-Модуль mpserv запущен!")
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)