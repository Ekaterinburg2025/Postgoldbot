import os
import time
import pymongo
from pymongo import MongoClient
import threading
import re
import html
from bson.objectid import ObjectId
from urllib.parse import quote

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

admins_collection = db['admins'] # Новая коллекция для админов

# ADMIN ID (ваш ID)
ADMIN_CHAT_ID = 479938867  # Ваш ID

# 🔒 Вечные (статичные) админы
CORE_ADMINS = [479938867, 7235010425]

# ==================== БАЗА ДАННЫХ MONGODB ====================
MONGO_URI = os.getenv('MONGO_URI')
if not MONGO_URI:
    raise ValueError("❌ КРИТИЧЕСКАЯ ОШИБКА: Не задана переменная MONGO_URI на сервере!")

mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client['elite_bot_db'] # Подключаемся к ЕДИНОЙ базе Скайнета!

# Коллекции, которые нам понадобятся:
ad_subs_collection = db['ad_subscriptions'] # НОВАЯ: Подписки на рекламу
ad_posts_collection = db['ad_posts']        # НОВАЯ: Опубликованные посты
promocodes_collection = db['promocodes']    # СУЩЕСТВУЮЩАЯ ИЗ СКАЙНЕТА
admins_collection = db['admins']            # НОВАЯ: Список админов
# =============================================================

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
    """Динамический расчет стоимости в Звездах (XTR)"""
    # 1. Если это БИГ-чат
    if chat_id in BIG_CHATS:
        if days == 1: return 1095
        if days == 7: return 7656
        return None # В биг-чатах продаем только на 1 и 7 дней
        
    # 2. Узнаем размер обычного чата (если не удалось, считаем мелким)
    try:
        count = bot.get_chat_member_count(chat_id)
    except:
        count = 500 
        
    # 3. Выдаем цену по нашей матрице
    if count > 1000:
        prices = {1: 105, 7: 656, 15: 1288, 30: 1563}
    else:
        prices = {1: 105, 7: 490, 15: 720, 30: 938}
        
    return prices.get(days)

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

    # 💥 ЗАПИСЬ В MONGODB ВМЕСТО СТАРОГО СЛОВАРЯ
    ad_subs_collection.insert_one({
        "user_id": user_id,
        "network": network,
        "city": city,
        "end_date": expiry_date,
        "purchase_date": now_ekb()
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
        date_str = end_date.strftime("%d.%m.%Y %H:%M")

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
            end_date = sub['end_date'].strftime('%d.%m.%Y')
            
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
    
    bot.edit_message_text("⏳ Выберите, на сколько изменить срок выбранной подписки:", call.message.chat.id, call.message.message_id, reply_markup=markup)

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
        
        # Отчитываемся админу об успехе
        success_text = (f"✅ <b>Срок успешно изменён!</b>\n\n"
                        f"🌐 Сеть: <b>{escape_html(sub.get('network'))}</b>\n"
                        f"📍 Город: <b>{escape_html(sub.get('city'))}</b>\n"
                        f"⏳ Новая дата окончания: <b>{new_date.strftime('%d.%m.%Y %H:%M')}</b>")
        bot.edit_message_text(success_text, call.message.chat.id, call.message.message_id, parse_mode="HTML")

    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка при изменении срока.")

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
        
    if "Групп" in message.text:
        bot.send_message(
            message.chat.id, 
            "Эксклюзив! 🎪\nДля расчета стоимости рекламы сообществ, Youtube-каналов и мероприятий обратитесь к нашему менеджеру: @FAQMKBOT выберите раздел реклама", 
            reply_markup=get_main_keyboard()
        )
        return
        
    bot.send_message(message.chat.id, "Напишите текст объявления (без ссылок на другие каналы):", reply_markup=types.ReplyKeyboardRemove())
    bot.register_next_step_handler(message, process_text)

@bot.message_handler(func=lambda message: message.text == "Удалить объявление")
def handle_delete_post(message):
    if message.chat.type != "private": return
    
    # ИЩЕМ ПОСТЫ В MONGODB (только активные)
    user_id = message.from_user.id
    posts = list(ad_posts_collection.find({"user_id": user_id, "deleted": False}))
    
    if posts:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        for post in posts:
            time_formatted = format_time(post["time"])
            button_text = f"Удалить: {time_formatted}, {post['city']}, {post['network']}"
            markup.add(button_text)
        markup.add("Отмена")
        bot.send_message(message.chat.id, "Выберите объявление для удаления:", reply_markup=markup)
        bot.register_next_step_handler(message, process_delete_choice, posts)
    else:
        bot.send_message(message.chat.id, "❌ У вас нет активных опубликованных объявлений.")

def process_delete_choice(message, posts):
    if message.text == "Отмена":
        bot.send_message(message.chat.id, "Удаление отменено.", reply_markup=get_main_keyboard())
        return

    for post in posts:
        time_formatted = format_time(post["time"])
        if message.text == f"Удалить: {time_formatted}, {post['city']}, {post['network']}":
            try:
                bot.delete_message(post["chat_id"], post["message_id"])
            except: pass

            # Помечаем пост как удаленный в MONGODB
            ad_posts_collection.update_one(
                {"_id": post["_id"]}, 
                {"$set": {"deleted": True, "deleted_by": "Пользователь"}}
            )
            bot.send_message(message.chat.id, "✅ Объявление успешно удалено.", reply_markup=get_main_keyboard())
            return
            
    bot.send_message(message.chat.id, "❌ Ошибка! Пожалуйста, выберите объявление из списка.")

@bot.message_handler(func=lambda message: message.text == "Удалить все объявления")
def handle_delete_all_posts(message):
    if message.chat.type != "private": return
    
    user_id = message.from_user.id
    posts = list(ad_posts_collection.find({"user_id": user_id, "deleted": False}))
    
    if posts:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        markup.add("Да, удалить всё", "Нет, отменить")
        bot.send_message(message.chat.id, "Вы уверены, что хотите удалить все свои активные объявления?", reply_markup=markup)
        bot.register_next_step_handler(message, process_delete_all_choice)
    else:
        bot.send_message(message.chat.id, "❌ У вас нет опубликованных объявлений.")

def process_delete_all_choice(message):
    if message.text == "Да, удалить всё":
        user_id = message.from_user.id
        posts = list(ad_posts_collection.find({"user_id": user_id, "deleted": False}))
        
        for post in posts:
            try: bot.delete_message(post["chat_id"], post["message_id"])
            except: pass
            
            ad_posts_collection.update_one({"_id": post["_id"]}, {"$set": {"deleted": True, "deleted_by": "Пользователь"}})

        bot.send_message(message.chat.id, "✅ Все ваши объявления успешно удалены.", reply_markup=get_main_keyboard())
    else:
        bot.send_message(message.chat.id, "Удаление отменено.", reply_markup=get_main_keyboard())

def process_text(message):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Вы вернулись в главное меню.", reply_markup=get_main_keyboard())
        return

    if message.photo or message.video:
        if message.photo:
            media_type = "photo"
            file_id = message.photo[-1].file_id
            text = message.caption if message.caption else ""
        elif message.video:
            media_type = "video"
            file_id = message.video.file_id
            text = message.caption if message.caption else ""
    elif message.text:
        media_type = None
        file_id = None
        text = message.text
    else:
        bot.send_message(message.chat.id, "❌ Ошибка! Отправьте текст, фото или видео.")
        bot.register_next_step_handler(message, process_text)
        return

    confirm_text(message, text, media_type, file_id)

def confirm_text(message, text, media_type=None, file_id=None):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Да", "Нет")
    bot.send_message(message.chat.id, f"Ваш текст:\n{text}\n\nВсё верно?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_confirmation, text, media_type, file_id)

def handle_confirmation(message, text, media_type, file_id):
    if message.text.lower() == "да":
        bot.send_message(message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
    elif message.text.lower() == "нет":
        bot.send_message(message.chat.id, "Хорошо, напишите текст объявления заново:")
        bot.register_next_step_handler(message, process_text)
    else:
        bot.send_message(message.chat.id, "❌ Неверный ответ. Выберите 'Да' или 'Нет'.")
        bot.register_next_step_handler(message, handle_confirmation, text, media_type, file_id)

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
    if name == "Мужской Клуб":
        return "mk"
    elif name == "ПАРНИ 18+":
        return "parni"
    elif name in ["НС", "Знакомства 66", "Знакомства 74"]:
        return "ns"
    elif name == "Радуга":
        return "rainbow"
    elif name == "Гей Знакомства":
        return "gayznak"
    return None

def select_network(message, text, media_type, file_id):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Напишите текст объявления:")
        bot.register_next_step_handler(message, process_text)
        return

    selected_network = message.text.strip()
    valid_networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства", "Все сети"]

    if selected_network in valid_networks:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)

        if selected_network == "Все сети":
            # Только города, которые есть минимум в 2 сетях
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
        bot.register_next_step_handler(message, select_city_and_publish, text, selected_network, media_type, file_id)
    else:
        bot.send_message(
            message.chat.id,
            "❌ <b>Ошибка!</b> Пожалуйста, выберите одну из предложенных сетей.",
            parse_mode="HTML"
        )
        bot.register_next_step_handler(message, process_text)

def get_user_html_link(user):
    name = html.escape(user.first_name or "Без имени")
    if user.last_name:
        name += " " + html.escape(user.last_name)
    return f'<a href="tg://user?id={user.id}">{name}</a>'

def select_city_and_publish(message, text, selected_network, media_type, file_id):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
        return

    city = message.text
    if city == "Выбрать другую сеть":
        bot.send_message(message.chat.id, "📋 Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
        return

    user_id = message.from_user.id
    user_name = f'<b>{get_user_html_link(message.from_user)}</b>' # НЕ экранируем!
    text = escape_html(text) # Экранируем пользовательский текст

    networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Радуга", "Гей Знакомства",] if selected_network == "Все сети" else [selected_network]

    was_published = False

    for network in networks:
        net_key = normalize_network_key(network)
        city_data = all_cities.get(city, {}).get(net_key)

        if not city_data:
            continue

        if not is_user_paid(user_id, network, city):
            log_failed_attempt(user_id, network, city, "Нет доступа")
            
            chat_id = city_data[0]["chat_id"] # Берем ID чата для расчета цены
            
            markup = types.InlineKeyboardMarkup(row_width=1)
            markup.add(types.InlineKeyboardButton("🎫 У меня есть промокод", callback_data=f"ad_promo_{network}_{city}"))
            
            # --- УМНАЯ СКИДКА (НАКОПИТЕЛЬНАЯ + ОПТ) ---
            # 1. Считаем, сколько уникальных сетей в этом городе у юзера УЖЕ активно
            already_active_nets = len(ad_subs_collection.distinct("network", {
                "user_id": user_id, 
                "city": city, 
                "end_date": {"$gt": now_ekb()}
            }))
            
            # 2. Считаем, сколько сетей он покупает СЕЙЧАС
            if network == "Все сети":
                # Если берет всё оптом, считаем, сколько сетей доступно в этом городе
                nets_in_city = len([n for n, d in all_cities[city].items() if d])
                buying_now = nets_in_city - already_active_nets # Не продаем то, что уже куплено
                total_nets_count = nets_in_city
            else:
                buying_now = 1
                total_nets_count = already_active_nets + 1
            
            # 3. Определяем процент скидки
            discount_percent = 0
            if total_nets_count == 3:
                discount_percent = 10
            elif total_nets_count == 4:
                discount_percent = 20
            elif total_nets_count >= 5:
                discount_percent = 30

            # Генерируем кнопки с ценами на лету! (Обычная + Закреп)
            for days in [1, 7, 15, 30]:
                base_price = get_price_for_chat(chat_id, days)
                if base_price:
                    # Умножаем базовую цену одной сети на количество покупаемых сетей
                    total_base_price = base_price * buying_now
                    
                    # Применяем прогрессивную скидку
                    final_price = int(total_base_price * (1 - discount_percent / 100))
                    
                    # Цена с закрепом (+20% к финальной цене)
                    pin_price = int(final_price * 1.2) 
                    
                    # Формируем текст на кнопке (показываем скидку, если она есть)
                    btn_text = f"💳 {days} дн. ({final_price}⭐️)"
                    if discount_percent > 0:
                        btn_text = f"🔥 {days} дн. (-{discount_percent}% за {final_price}⭐️)"
                    
                    # Добавляем сразу две кнопки в один ряд!
                    markup.row(
                        types.InlineKeyboardButton(btn_text, callback_data=f"ad_pay_{days}_{network}_{city}"),
                        types.InlineKeyboardButton(f"📌 +Закреп ({pin_price}⭐️)", callback_data=f"ad_paypin_{days}_{network}_{city}")
                    )

            bot.send_message(
                message.chat.id,
                f"⛔ У вас нет доступа к публикации в <b>{escape_html(network)}</b> ({escape_html(city)}).\n\nПриобретите доступ за звезды Telegram:",
                reply_markup=markup,
                parse_mode="HTML"
            )
            continue # Идем к следующей сети, текст пока не публикуем

        # 🛡 АНТИ-ФРОД ФИЛЬТР (Проверяем текст на хитрые ссылки)
        has_links = bool(re.search(r'(t\.me/|@\w+|http)', text.lower()))
        if has_links:
            bot.send_message(message.chat.id, "❌ На тарифе «Встречи/Услуги» запрещена публикация ссылок (t.me, http) и @username. Уберите их из текста.")
            ask_for_new_post(message)
            return

        user_stats = get_user_statistics(user_id)
        city_stats = user_stats.get("details", {}).get(network, {}).get(city, {})

        if city_stats.get("remaining", 0) <= 0:
            bot.send_message(
                message.chat.id,
                f"⛔ Лимит публикаций исчерпан для <b>{escape_html(network)}</b>, город <b>{escape_html(city)}</b>",
                parse_mode="HTML"
            )
            log_failed_attempt(user_id, network, city, "Лимит исчерпан")
            continue

        signature = network_signatures.get(network, "") # Без escape_html
        full_text = f"📢 Объявление от {user_name}:\n\n{text}\n\n{signature}"

        # 💬 Кнопка "Напиши мне в ЛС" — ЗЕЛЁНАЯ + ТВОЙ премиум эмодзи
        reply_markup = types.InlineKeyboardMarkup()
        reply_markup.add(
            types.InlineKeyboardButton(
                text="Напиши мне в ЛС",
                url=f"tg://user?id={user_id}",
                style="success",                          # Зелёная кнопка
                icon_custom_emoji_id="5470060791883374114"   # ТВОЙ ID облачка
            )
        )

        for location in city_data:
            chat_id = location["chat_id"]
            try:
                if media_type == "photo":
                    sent_message = bot.send_photo(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                elif media_type == "video":
                    sent_message = bot.send_video(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                else:
                    sent_message = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)

                # 💥 ИСПРАВЛЕНИЕ: Обязательно пишем пост в БД, иначе лимиты не будут работать!
                add_post_to_history(
                    user_id=user_id,
                    user_name=message.from_user.first_name or "Без имени",
                    network=network,
                    city=location['name'],
                    chat_id=chat_id,
                    message_id=sent_message.message_id
                )

                bot.send_message(
                    message.chat.id,
                    f"✅ Объявление опубликовано в сети <b>{escape_html(network)}</b>, городе <b>{escape_html(location['name'])}</b>.",
                    parse_mode="HTML"
                )
                was_published = True

                # Проверяем, есть ли у юзера купленный закреп
                active_sub = ad_subs_collection.find_one({
                    "user_id": user_id,
                    "city": location['name'],
                    "network": {"$in": ["Все сети", network]},
                    "end_date": {"$gt": now_ekb()}
                })
                
                if active_sub and active_sub.get("has_pin", False):
                    try:
                        # Закрепляем сообщение (без звукового уведомления для всех участников)
                        bot.pin_chat_message(chat_id, sent_message.message_id, disable_notification=True)
                    except Exception as e:
                        pass # Если у бота нет прав на закреп, он просто пропустит

            except telebot.apihelper.ApiTelegramException as e:
                log_failed_attempt(user_id, network, city, f"Ошибка отправки: {e.description}")
                bot.send_message(message.chat.id, f"❌ <b>Ошибка:</b> {escape_html(e.description)}", parse_mode="HTML")

    if not was_published:
        markup = types.InlineKeyboardMarkup()
        url = "https://t.me/FAQMKBOT" if selected_network == "Мужской Клуб" else "https://t.me/FAQZNAKBOT"
        markup.add(
            types.InlineKeyboardButton(
                text="Купить рекламу",
                url=url,
                style="danger",                           # Красная кнопка
                icon_custom_emoji_id="5420315771991497307"   # ТВОЙ ID огонька
            )
        )
        bot.send_message(
            message.chat.id,
            "⛔ У вас нет прав на публикацию в этой сети/городе. Обратитесь к администратору.",
            reply_markup=markup
        )
    
    ask_for_new_post(message)

def ask_for_new_post(message):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Да", "Нет")
    bot.send_message(message.chat.id, "Хотите опубликовать ещё одно объявление?", reply_markup=markup)
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

@bot.callback_query_handler(func=lambda call: (call.data.startswith('ad_pay_') or call.data.startswith('ad_paypin_')) and not call.data.startswith('ad_pay_discount_'))
def handle_ad_checkout(call):
    is_pin = call.data.startswith('ad_paypin_')
    parts = call.data.split('_')
    
    days = int(parts[2])
    network = parts[3]
    city = parts[4]
    
    net_key = normalize_network_key(network)
    chat_id = all_cities[city][net_key][0]["chat_id"]
    base_price = get_price_for_chat(chat_id, days)
    
    amount = int(base_price * 1.2) if is_pin else base_price
    
    payload = f"ad_access_{days}_{network}_{city}"
    if is_pin:
        payload += "_pin" # Ставим скрытую метку закрепа
        title_text = "Доступ + ЗАКРЕП 📌"
    else:
        title_text = "Доступ к публикации 📢"
    
    bot.send_invoice(
        call.message.chat.id, 
        title=title_text, 
        description=f"Сеть: {network}\nГород: {city}\nСрок: {days} дн.", 
        invoice_payload=payload, 
        provider_token="", 
        currency="XTR", 
        prices=[types.LabeledPrice(label="Рекламный доступ", amount=amount)]
    )

@bot.pre_checkout_query_handler(func=lambda query: query.invoice_payload.startswith("ad_access_"))
def checkout_process(pre_checkout_query):
    bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@bot.message_handler(content_types=['successful_payment'])
def successful_payment(message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload

    if payload.startswith("ad_access_"):
        parts = payload.split('_')
        
        # Разбираем пейлоад (с промокодом или без)
        if "discount" in payload:
            days = int(parts[3])
            network = parts[4]
            city = parts[5]
            promo_code = parts[6]
            # Записываем активацию промокода в Монго (лимит уменьшается на 1)
            promocodes_collection.update_one({"_id": promo_code}, {"$inc": {"used_count": 1}})
        else:
            days = int(parts[2])
            network = parts[3]
            city = parts[4]

        end_date = now_ekb() + timedelta(days=days)
        has_pin = "_pin" in payload # Проверяем, купил ли юзер закреп

        # 💥 ЗАПИСЬ В БАЗУ ДАННЫХ
        ad_subs_collection.insert_one({
            "user_id": user_id,
            "network": network,
            "city": city,
            "end_date": end_date,
            "purchase_date": now_ekb(),
            "has_pin": has_pin # Сохраняем статус закрепа!
        })

        try:
            bot.send_message(
                user_id, 
                f"✅ **Оплата успешно получена!**\n\nДоступ к сети **{network}** ({city}) открыт на {days} дней.\nНажмите «Создать новое объявление».",
                parse_mode="Markdown"
            )
        except Exception as e:
            # Если юзер заблокировал бота сразу после оплаты, бот просто проигнорирует ошибку и не упадет
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('ad_promo_'))
def handle_ad_promo(call):
    parts = call.data.split('_')
    network = parts[2]
    city = parts[3]
    msg = bot.send_message(call.message.chat.id, "👇 **Введите ваш промокод ответом на это сообщение:**", parse_mode="Markdown")
    bot.register_next_step_handler(msg, process_ad_promo, network=network, city=city, call_msg=call.message)

def process_ad_promo(message, network, city, call_msg):
    promo_text = message.text.strip().upper()
    promo_data = promocodes_collection.find_one({"_id": promo_text})

    if not promo_data or not promo_data.get("is_active") or promo_data.get("used_count", 0) >= promo_data.get("usage_limit", 1):
        bot.send_message(message.chat.id, "❌ Промокод не найден, истек или исчерпал лимит активаций.")
        return

    # Разрешаем использовать промокоды, у которых цель "ads" (реклама) или "all" (все)
    if promo_data.get("target") not in ["all", "ads"]:
        bot.send_message(message.chat.id, "❌ Этот промокод нельзя применить к покупке рекламы.")
        return

    discount = promo_data["value"]
    is_percent = promo_data["type"] == "percent"

    net_key = normalize_network_key(network)
    chat_id = all_cities[city][net_key][0]["chat_id"]

    def calc_discount(price):
        if not price: return None
        if is_percent: return max(1, int(price * (1 - discount / 100)))
        return max(1, price - discount)

    markup = types.InlineKeyboardMarkup(row_width=1)
    for days in [1, 7, 15, 30]:
        original_price = get_price_for_chat(chat_id, days)
        if original_price:
            new_price = calc_discount(original_price)
            # Вшиваем промокод в кнопку оплаты
            markup.add(types.InlineKeyboardButton(
                f"💳 Купить {days} дн. ({new_price}⭐️ вместо {original_price})", 
                callback_data=f"ad_pay_discount_{days}_{network}_{city}_{promo_text}_{new_price}"
            ))

    bot.send_message(message.chat.id, f"✅ **Промокод применен!** Выберите тариф со скидкой:", reply_markup=markup, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: call.data.startswith('ad_pay_discount_'))
def handle_ad_discount_checkout(call):
    parts = call.data.split('_')
    days, network, city, promo_code, price = int(parts[3]), parts[4], parts[5], parts[6], int(parts[7])
    
    payload = f"ad_access_discount_{days}_{network}_{city}_{promo_code}"
    
    bot.send_invoice(
        call.message.chat.id, 
        title="Доступ к публикации (Скидка) 📢", 
        description=f"Сеть: {network}\nГород: {city}\nСрок: {days} дн.", 
        invoice_payload=payload, 
        provider_token="", 
        currency="XTR", 
        prices=[types.LabeledPrice(label="Рекламный доступ", amount=price)]
    )

# --- ФОНОВЫЕ ЗАДАЧИ ---
def check_expiring_subs():
    """Фоновая задача: ищет подписки, истекающие через 72 и 24 часа"""
    while True:
        try:
            now = now_ekb()
            
            # 1. Проверка за 72 часа (3 дня)
            target_72 = now + timedelta(hours=72)
            expiring_72 = ad_subs_collection.find({
                "end_date": {"$gte": target_72 - timedelta(hours=1), "$lte": target_72 + timedelta(hours=1)},
                "notified_72h": {"$ne": True}
            })
            for sub in expiring_72:
                try:
                    bot.send_message(sub['user_id'], f"⏳ <b>Мягкое напоминание:</b>\nВаша подписка на <b>{sub['network']}</b> ({sub['city']}) истекает через 3 дня.\nПодготовьтесь к продлению, чтобы не потерять клиентов!", parse_mode="HTML")
                    ad_subs_collection.update_one({"_id": sub["_id"]}, {"$set": {"notified_72h": True}})
                except: pass

            # 2. Проверка за 24 часа
            target_24 = now + timedelta(hours=24)
            expiring_24 = ad_subs_collection.find({
                "end_date": {"$gte": target_24 - timedelta(hours=1), "$lte": target_24 + timedelta(hours=1)},
                "notified_24h": {"$ne": True}
            })
            for sub in expiring_24:
                try:
                    bot.send_message(sub['user_id'], f"🚨 <b>Внимание! Последние сутки!</b>\nВаш доступ к <b>{sub['network']}</b> ({sub['city']}) закончится через 24 часа!\nПродлите подписку, чтобы ваши объявления продолжали публиковаться.", parse_mode="HTML")
                    ad_subs_collection.update_one({"_id": sub["_id"]}, {"$set": {"notified_24h": True}})
                except: pass

            time.sleep(3600) # Проверяем базу каждый час
        except Exception as e:
            time.sleep(60)

# Запускаем поток
threading.Thread(target=check_expiring_subs, daemon=True).start()

if __name__ == '__main__':
    print("✅ Скайнет-Модуль mpserv запущен!")
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)