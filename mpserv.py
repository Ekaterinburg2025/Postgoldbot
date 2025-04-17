import os
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from collections import defaultdict
import threading
import shutil
import re
import html
from urllib.parse import quote

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

ATTEMPTS_PER_PAGE = 10
POSTS_PER_PAGE = 10

import pytz
from pytz import timezone

def now_ekb():
    return datetime.now(timezone('Asia/Yekaterinburg'))

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

# ADMIN ID (ваш ID)
ADMIN_CHAT_ID = 479938867  # Ваш ID

# Глобальные переменные
paid_users = {}
user_posts = {}
user_daily_posts = {}
user_statistics = {}
admins = []
db_lock = threading.Lock()
user_failed_attempts = {}

# 🔒 Вечные (статичные) админы
CORE_ADMINS = [479938867, 7235010425]

# Инициализация базы данных
def init_db():
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()

            # Таблица оплативших пользователей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS paid_users (
                    user_id INTEGER,
                    network TEXT,
                    city TEXT,
                    end_date TEXT
                )
            """)

            # Таблица администраторов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS admin_users (
                    user_id INTEGER PRIMARY KEY
                )
            """)

            # Таблица опубликованных постов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_posts (
                    user_id INTEGER,
                    network TEXT,
                    city TEXT,
                    time TEXT,
                    chat_id INTEGER,
                    message_id INTEGER,
                    deleted INTEGER DEFAULT 0
                )
            """)

            # Таблица неудачных попыток публикации
            cur.execute("""
                CREATE TABLE IF NOT EXISTS failed_attempts (
                    user_id INTEGER,
                    network TEXT,
                    city TEXT,
                    time TEXT,
                    reason TEXT
                )
            """)

            # ⚠️ Если таблица уже существовала, добавим колонку deleted вручную (миграция)
            try:
                cur.execute("ALTER TABLE user_posts ADD COLUMN deleted INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass  # колонка уже есть — пропускаем

            # Таблица истории постов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS post_history (
                    user_id INTEGER,
                    user_name TEXT,
                    network TEXT,
                    city TEXT,
                    time TEXT,
                    chat_id INTEGER,
                    message_id INTEGER,
                    deleted INTEGER DEFAULT 0,
                    deleted_by INTEGER
                )
            """)

            conn.commit()

def log_failed_attempt(user_id, network, city, reason):
    """Логирует неудачную попытку публикации — и в память, и в базу."""

    # 💾 Сохраняем в память
    if user_id not in user_failed_attempts:
        user_failed_attempts[user_id] = []

    user_failed_attempts[user_id].append({
        "network": network,
        "city": city,
        "time": now_ekb(),
        "reason": reason
    })

    # 🧱 И дублируем в БД для надёжности
    try:
        with db_lock:
            with sqlite3.connect("bot_data.db") as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO failed_attempts (user_id, network, city, time, reason)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, network, city, now_ekb().isoformat(), reason))
                conn.commit()
        print(f"[FAILED] {user_id}, {repr(network)}, {repr(city)}, {repr(reason)}")
    except Exception as e:
        print(f"[ERROR] Ошибка при логировании неудачной попытки: {e}")

def add_post_to_history(user_id, user_name, network, city, chat_id, message_id, deleted=False, deleted_by=None):
    """
    Сохраняет пост в таблицу post_history.
    """
    post_time = now_ekb()
    post_data = {
        "user_id": user_id,
        "user_name": user_name,
        "network": network,
        "city": city,
        "time": post_time,
        "chat_id": chat_id,
        "message_id": message_id,
        "deleted": deleted,
        "deleted_by": deleted_by
    }

    # Сохраняем пост в post_history
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO post_history (user_id, user_name, network, city, time, chat_id, message_id, deleted, deleted_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                post_data["user_id"],
                post_data["user_name"],
                post_data["network"],
                post_data["city"],
                post_data["time"].isoformat(),
                post_data["chat_id"],
                post_data["message_id"],
                int(post_data["deleted"]),
                post_data["deleted_by"]
            ))
            conn.commit()

# Загрузка данных из базы данных
def load_data():
    with db_lock:
        try:
            with sqlite3.connect("bot_data.db") as conn:
                cur = conn.cursor()

                # Загружаем оплативших пользователей
                cur.execute("SELECT user_id, network, city, end_date FROM paid_users")
                local_paid_users = {}
                for user_id, network, city, end_date in cur.fetchall():
                    if user_id not in local_paid_users:
                        local_paid_users[user_id] = []
                    try:
                        parsed_date = datetime.fromisoformat(end_date)
                    except:
                        parsed_date = None
                    local_paid_users[user_id].append({
                        "network": network,
                        "city": city,
                        "end_date": parsed_date
                    })

                # Загружаем админов
                cur.execute("SELECT user_id FROM admin_users")
                local_admins = [row[0] for row in cur.fetchall()]

                # Загружаем посты
                cur.execute("SELECT user_id, network, city, time, chat_id, message_id, deleted FROM user_posts")
                local_user_posts = {}
                for user_id, network, city, time_str, chat_id, message_id, deleted in cur.fetchall():
                    if user_id not in local_user_posts:
                        local_user_posts[user_id] = []
                    try:
                        post_time = datetime.fromisoformat(time_str)
                    except:
                        post_time = now_ekb()
                    local_user_posts[user_id].append({
                        "message_id": message_id,
                        "chat_id": chat_id,
                        "time": post_time,
                        "city": city,
                        "network": network,
                        "deleted": bool(deleted)
                    })

                # Загружаем неудачные попытки
                cur.execute("SELECT user_id, network, city, time, reason FROM failed_attempts")
                local_failed_attempts = {}
                for user_id, network, city, time_str, reason in cur.fetchall():
                    try:
                        attempt_time = datetime.fromisoformat(time_str)
                    except:
                        attempt_time = now_ekb()
                    if user_id not in local_failed_attempts:
                        local_failed_attempts[user_id] = []
                    local_failed_attempts[user_id].append({
                        "network": network,
                        "city": city,
                        "time": attempt_time,
                        "reason": reason
                    })

                # Обновляем глобальные переменные
                global paid_users, admins, user_posts, user_failed_attempts, user_daily_posts
                paid_users = local_paid_users
                admins = local_admins
                user_posts = local_user_posts
                user_failed_attempts = local_failed_attempts

                # Восстановление user_daily_posts
                from collections import defaultdict
                user_daily_posts = {}

                source_posts = user_posts if user_posts else {}

                # Если user_posts пуст — пробуем взять посты из post_history
                if not source_posts:
                    print("[ℹ️] Восстанавливаем user_daily_posts из post_history")
                    cur.execute("SELECT user_id, network, city, time, deleted FROM post_history")
                    for user_id, network, city, time_str, deleted in cur.fetchall():
                        try:
                            post_time = datetime.fromisoformat(time_str)
                        except:
                            continue

                        if post_time.date() != now_ekb().date():
                            continue

                        if user_id not in source_posts:
                            source_posts[user_id] = []

                        source_posts[user_id].append({
                            "network": network,
                            "city": city,
                            "time": post_time,
                            "deleted": bool(deleted)
                        })

                # Сборка user_daily_posts
                for user_id, posts in source_posts.items():
                    for post in posts:
                        network = post["network"]
                        city = post["city"]
                        time = post["time"]
                        is_deleted = post.get("deleted", False)

                        if isinstance(time, str):
                            try:
                                time = datetime.fromisoformat(time)
                            except:
                                continue

                        if time.date() != now_ekb().date():
                            continue

                        if user_id not in user_daily_posts:
                            user_daily_posts[user_id] = defaultdict(lambda: defaultdict(lambda: {
                                "posts": [],
                                "deleted_posts": [],
                                "last_post_time": None
                            }))

                        user_daily_posts[user_id][network][city]["last_post_time"] = time

                        if is_deleted:
                            user_daily_posts[user_id][network][city]["deleted_posts"].append(time)
                        else:
                            user_daily_posts[user_id][network][city]["posts"].append(time)

                return paid_users, admins, user_posts

        except Exception as e:
            print(f"[ERROR] Ошибка при загрузке данных из базы: {e}")
            return {}, [], {}

# Инициализация базы данных
init_db()
paid_users, admins, user_posts = load_data()

# 🧠 Автогенерация all_cities на основе chat_ids_* и учёта особых случаев

# Старые словари:
chat_ids_mk = {
    "Екатеринбург": -1002210043742,
    "Челябинск": -1002238514762,
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
    "Иркутск": -1002210419274,
    "Кемерово": -1002147522863,
    "Москва": -1002208434096,
    "Санкт Петербург": -1002485776859,
    "Общая группа Юга": -1001814693664,
    "Общая группа Дальнего Востока": -1002161346845,
    "Общая группа Тюмень и Север": -1002210623988,
    "Тестовая группа 🛠️": -1002426733876
}

chat_ids_parni = {
    "Екатеринбург": -1002413948841,
    "Тюмень": -1002255622479,
    "Омск": -1002274367832,
    "Челябинск": -1002406302365,
    "Перми": -1002280860973,
    "Курган": -1002469285352,
    "ХМАО": -1002287709568,
    "Уфа": -1002448909000,
    "Новосибирск": -1002261777025,
    "ЯМАО": -1002371438340
}

chat_ids_ns = {
    "Курган": -1001465465654,
    "Новосибирск": -1001824149334,
    "Челябинск": -1002233108474,
    "Пермь": -1001753881279,
    "Уфа": -1001823390636,
    "Ямал": -1002145851794,
    "Москва": -1001938448310,
    "ХМАО": -1001442597049,
    "Знакомства 66": -1002169473861,
    "Знакомства 74": -1002193127380
}

# Нормализация названий (объединение Перми/Пермь, ЯМАО/Ямал и пр.)
def normalize_city_name(name):
    mapping = {
        "Перми": "Пермь",
        "ЯМАО": "Ямал",
        "Знакомства 66": "Екатеринбург",
        "Знакомства 74": "Челябинск"
    }
    return mapping.get(name, name)

# Автоматическая сборка all_cities
all_cities = {}

def insert_to_all(city, net, real_name, chat_id):
    norm = normalize_city_name(city)
    if norm not in all_cities:
        all_cities[norm] = {}
    if net not in all_cities[norm]:
        all_cities[norm][net] = []
    all_cities[norm][net].append({"name": real_name, "chat_id": chat_id})

for city, chat_id in chat_ids_mk.items():
    insert_to_all(city, "mk", city, chat_id)

for city, chat_id in chat_ids_parni.items():
    insert_to_all(city, "parni", city, chat_id)

for city, chat_id in chat_ids_ns.items():
    insert_to_all(city, "ns", city, chat_id)

# Добавим fallback-группу МК для Тюмени, Ямала и ХМАО если её там нет
fallback_mk = {"Тюмень", "Ямал", "ХМАО"}
for city in fallback_mk:
    if "mk" not in all_cities.get(city, {}):
        insert_to_all(city, "mk", "Общая группа Тюмень и Север", -1002210623988)

# Итог: all_cities готов
print(f"📦 Сформировано {len(all_cities)} городов")

# Статичные подписи для каждой сети
network_signatures = {
    "Мужской Клуб": "️ 🕸️Реклама. Согласовано с администрацей сети МК.",
    "ПАРНИ 18+": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫",
    "НС": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫"
}

# Словарь для хранения всех сообщений пользователей
user_posts = {}

def get_main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Создать новое объявление", "Удалить объявление", "Удалить все объявления", "📊 Моя статистика")
    return markup

def format_time(timestamp):
    tz = pytz.timezone('Asia/Yekaterinburg')
    local_time = timestamp.astimezone(tz)
    return local_time.strftime("%H:%M, %d %B %Y")

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

def add_paid_user(user_id, network, city, end_date):
    with db_lock:  
        if user_id not in paid_users:
            paid_users[user_id] = []
        paid_users[user_id].append({
            "network": network,
            "city": city,
            "end_date": end_date
        })
        save_data()

    bot.send_message(ADMIN_CHAT_ID, f"✅ Пользователь {user_id} добавлен в сеть «{network}», город {city} на {end_date.strftime('%Y-%m-%d')}.")
    bot.send_message(user_id, f"✅ Вы добавлены в сеть «{network}», город {city} на {end_date.strftime('%Y-%m-%d')}.")

def add_admin_user(user_id):
    with db_lock:
        if user_id not in admins:
            admins.append(user_id)
            save_data()

    bot.send_message(ADMIN_CHAT_ID, f"✅ Пользователь {user_id} добавлен как администратор.")
    bot.send_message(user_id, "✅ Вы добавлены как администратор.")

def load_admin_users():
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM admin_users")
            admin_users = [row[0] for row in cur.fetchall()]
            return admin_users  # Возвращаем список администраторов

def add_admin_user(user_id):
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
            cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (user_id,))
            conn.commit()

def is_admin(user_id):
    admin_users = load_admin_users()
    return user_id in admin_users or user_id in CORE_ADMINS

@bot.message_handler(commands=["backup"])
def handle_backup(message):
    if not is_admin(message.from_user.id):
        return
    try:
        save_data()  # 💾 обязательно сохраняем перед отправкой

        with open("bot_data.db", "rb") as f:
            bot.send_document(message.chat.id, f, caption="📦 Бэкап базы данных")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Не удалось отправить бэкап: {e}")

@bot.message_handler(commands=["restore"])
def handle_restore_command(message):
    if not is_admin(message.from_user.id):
        return
    bot.send_message(message.chat.id, "📥 Отправьте файл `bot_data.db` для восстановления.")
    bot.register_next_step_handler(message, handle_restore_file)

def handle_restore_file(message):
    if not message.document:
        bot.send_message(message.chat.id, "❌ Это не файл. Отправьте `bot_data.db`.")
        return

    try:
        file_info = bot.get_file(message.document.file_id)
        downloaded_file = bot.download_file(file_info.file_path)

        with open("bot_data.db", "wb") as f:
            f.write(downloaded_file)

        load_data()
        bot.send_message(message.chat.id, "✅ База данных успешно восстановлена!")

    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Ошибка при восстановлении: {e}")

@bot.message_handler(commands=["debug_users"])
def handle_debug_users(message):
    if not is_admin(message.from_user.id):
        return

    if not paid_users:
        bot.send_message(message.chat.id, "⚠️ paid_users пуст.")
        return

    text = f"🧠 В памяти {len(paid_users)} оплативших:\n"
    for uid, entries in paid_users.items():
        text += f"\n👤 ID: {uid}"
        for e in entries:
            end = e['end_date']
            text += f"\n• {e['network']} | {e['city']} → {end}"
    bot.send_message(message.chat.id, text)

# Вспомогательная функция для подсчёта уникальных комбинаций "сеть + город"
def count_unique_networks_cities(user_id):
    """Считает количество уникальных комбинаций сетей и городов для пользователя."""
    if user_id not in user_daily_posts:
        return 0

    unique_combinations = set()
    for network, cities in user_daily_posts[user_id].items():
        for city in cities:
            unique_combinations.add((network, city))

    return len(unique_combinations)

def is_new_day(last_post_time):
    """Проверяет, наступил ли новый день."""
    if last_post_time is None:
        return True
    return last_post_time.date() < now_ekb().date()

def is_today(post_time):
    """Проверяет, было ли время публикации сегодня."""
    return post_time.date() == now_ekb().date()

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

    if user_id not in paid_users:
        paid_users[user_id] = []

    paid_users[user_id].append({
        "end_date": expiry_date.isoformat(),
        "network": network,
        "city": city
    })
    save_data()

    # Получаем имя пользователя для админа
    try:
        user_info = bot.get_chat(user_id)
        user_name = f"{user_info.first_name or ''} {user_info.last_name or ''}".strip()
        if not user_name:
            user_name = user_info.username or "Имя не указано"
    except Exception as e:
        user_name = "Имя не найдено"

    # Уведомление назначившему админу
    if message.chat.id != ADMIN_CHAT_ID:
        bot.send_message(
            message.chat.id,
            f"✅ Пользователь {user_name} (ID: {user_id}) добавлен в сеть «{network}», город {city} на {days} дн.\n📅 Действует до: {expiry_date.strftime('%d.%m.%Y')}"
        )

    # Уведомление главному админу (если нужно)
    bot.send_message(
        ADMIN_CHAT_ID,
        f"👨‍💼 {get_user_name(message.from_user)} добавил пользователя {user_name} (ID: {user_id}) в сеть «{network}», город {city} на {days} дн.\n📅 До: {expiry_date.strftime('%d.%m.%Y')}"
    )

def get_user_statistics(user_id):
    stats = {"published": 0, "remaining": 0, "details": {}}
    limit_total = 0

    active_access = []
    for access in paid_users.get(user_id, []):
        end_date = access.get("end_date")
        if isinstance(end_date, str):
            try:
                end_date = datetime.fromisoformat(end_date)
            except:
                end_date = None

        if end_date and end_date >= now_ekb():
            if access["network"] == "Все сети":
                for net in ["Мужской Клуб", "ПАРНИ 18+", "НС"]:
                    active_access.append((net, access["city"]))
            else:
                active_access.append((access["network"], access["city"]))

    # Удаляем дубликаты, если был доступ к "Все сети" и отдельно к сети
    active_access = list(set(active_access))

    for network, city in active_access:
        total_posts = 0
        if user_id in user_daily_posts and network in user_daily_posts[user_id] and city in user_daily_posts[user_id][network]:
            post_data = user_daily_posts[user_id][network][city]
            active_posts = len([p for p in post_data.get("posts", []) if is_today(p)])
            deleted_posts = len([p for p in post_data.get("deleted_posts", []) if is_today(p)])
            total_posts = active_posts + deleted_posts

        limit_total += 3
        if network not in stats["details"]:
            stats["details"][network] = {}

        stats["details"][network][city] = {
            "published": total_posts,
            "remaining": max(0, 3 - total_posts)
        }
        stats["published"] += total_posts

    stats["remaining"] = max(0, limit_total - stats["published"])
    return stats

def is_today(timestamp):
    now = now_ekb()
    try:
        parsed_time = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
        return parsed_time.date() == now_ekb().date()
    except:
        return False

from datetime import datetime
import pytz

def now_ekb():
    return datetime.now(pytz.timezone('Asia/Yekaterinburg'))

def get_admin_statistics():
    statistics = {}

    for user_id, networks in user_daily_posts.items():
        stats = {
            "published": 0,
            "remaining": 0,
            "details": {},
            "links": []
        }
        limit_total = 0
        links = set()
        today = now_ekb().date()

        for network, cities in networks.items():
            stats["details"][network] = {}

            for city, post_data in cities.items():
                # Оставляем только сегодняшние посты
                today_posts = [p for p in post_data.get("posts", []) if isinstance(p, datetime) and p.date() == today]
                today_deleted = [p for p in post_data.get("deleted_posts", []) if isinstance(p, datetime) and p.date() == today]

                total_posts = len(today_posts) + len(today_deleted)
                limit_total += 3

                stats["details"][network][city] = {
                    "published": total_posts,
                    "remaining": max(0, 3 - total_posts)
                }

                stats["published"] += total_posts

                # Ссылки только на сегодняшние посты
                for user_post in user_posts.get(user_id, []):
                    if (
                        user_post["network"] == network and
                        user_post["city"] == city and
                        isinstance(user_post.get("time"), datetime) and
                        user_post["time"].astimezone(pytz.timezone('Asia/Yekaterinburg')).date() == today
                    ):
                        link = f"https://t.me/c/{str(user_post['chat_id'])[4:]}/{user_post['message_id']}"
                        links.add(link)

        stats["remaining"] = max(0, limit_total - stats["published"])
        stats["links"] = list(links)
        statistics[user_id] = stats

    return statistics

def check_payment(user_id, network, city):
    """Проверяет, оплатил ли пользователь доступ к сети и городу (с учетом all_cities и НС)."""
    user_id = str(user_id)  # на всякий случай
    if user_id not in paid_users:
        print(f"[DEBUG] Пользователь {user_id} не найден в оплативших.")
        return False

    # Получаем ключ сети
    net_map = {"Мужской Клуб": "mk", "ПАРНИ 18+": "parni", "НС": "ns"}
    net_key = net_map.get(network)

    for payment in paid_users[user_id]:
        expiry = payment.get("end_date")
        if isinstance(expiry, str):
            try:
                expiry = datetime.fromisoformat(expiry)
            except:
                continue

        if not isinstance(expiry, datetime) or expiry < now_ekb():
            print(f"[DEBUG] Срок оплаты истёк у {user_id}: {payment}")
            continue

        # ✅ Все сети — подходит если город совпадает
        if payment["network"] == "Все сети" and payment["city"] == city:
            print(f"[DEBUG] ✅ Все сети: доступ в {network} / {city}")
            return True

        # ✅ Конкретная сеть и город
        if payment["network"] == network and payment["city"] == city:
            print(f"[DEBUG] ✅ Сеть: {network} / {city}")
            return True

        # ✅ Особый случай: НС — подставной город
        if network == "НС" and payment["network"] == "Все сети":
            variants = [city, ns_city_substitution.get(city)]
            if payment["city"] in variants:
                print(f"[DEBUG] ✅ НС через подстановку: {network} / {city}")
                return True

    print(f"[DEBUG] ❌ Нет доступа у {user_id} к {network} / {city}")
    return False

# Сохранение данных в файл
def save_data(retries=3, delay=0.5):
    """Сохраняет данные в базу данных с повторной попыткой при блокировке."""
    if not paid_users and not user_posts:
        print("[⛔ SAVE] Сохранение прервано: paid_users и user_posts пустые.")
        bot.send_message(
            ADMIN_CHAT_ID,
            "⚠️ Сохранение базы прервано: нет данных (0 оплат, 0 постов).",
        )
        return

    print(f"[💾 SAVE] Оплативших: {len(paid_users)}, Постов: {len(user_posts)}, Админов: {len(admins)}")

    for attempt in range(retries):
        with db_lock:
            try:
                with sqlite3.connect("bot_data.db", timeout=5) as conn:
                    cur = conn.cursor()

                    # Очистка таблиц
                    cur.execute("DELETE FROM paid_users")
                    cur.execute("DELETE FROM admin_users")
                    cur.execute("DELETE FROM user_posts")
                    cur.execute("DELETE FROM failed_attempts")
                    cur.execute("DELETE FROM post_history")

                    # Сохраняем оплативших
                    for user_id, entries in paid_users.items():
                        for entry in entries:
                            end = entry.get("end_date", now_ekb())
                            if isinstance(end, str):
                                try:
                                    end = datetime.fromisoformat(end)
                                except:
                                    end = now_ekb()
                            cur.execute("""
                                INSERT INTO paid_users (user_id, network, city, end_date)
                                VALUES (?, ?, ?, ?)
                            """, (user_id, entry["network"], entry["city"], end.isoformat()))

                    # Сохраняем админов
                    for user_id in admins:
                        cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (user_id,))

                    # Сохраняем посты
                    for user_id, posts in user_posts.items():
                        for post in posts:
                            cur.execute("""
                                INSERT INTO user_posts (user_id, network, city, time, chat_id, message_id, deleted)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                            """, (
                                user_id,
                                post["network"],
                                post["city"],
                                post["time"],
                                post["chat_id"],
                                post["message_id"],
                                int(post.get("deleted", False))
                            ))

                            # Также сохраняем в post_history
                            cur.execute("""
                                INSERT INTO post_history (user_id, user_name, network, city, time, chat_id, message_id, deleted, deleted_by)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                user_id,
                                post.get("user_name", "неизвестен"),
                                post["network"],
                                post["city"],
                                post["time"],
                                post["chat_id"],
                                post["message_id"],
                                int(post.get("deleted", False)),
                                post.get("deleted_by", None)
                            ))

                    # Неудачные попытки
                    for user_id, attempts in user_failed_attempts.items():
                        for attempt in attempts:
                            cur.execute("""
                                INSERT INTO failed_attempts (user_id, network, city, time, reason)
                                VALUES (?, ?, ?, ?, ?)
                            """, (
                                user_id,
                                attempt["network"],
                                attempt["city"],
                                attempt["time"].isoformat(),
                                attempt["reason"]
                            ))

                    conn.commit()
                    print("[✅ SAVE] Успешно сохранено в bot_data.db")

                    bot.send_message(
                        ADMIN_CHAT_ID,
                        f"✅ *Сохранено в базу:*\n👤 Оплативших: *{len(paid_users)}*\n📬 Постов: *{len(user_posts)}*\n👮 Админов: *{len(admins)}*",
                        parse_mode="Markdown"
                    )
                    return

            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    print("[⏳ SAVE] База занята, пробуем снова...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"[❌ SAVE] SQLite ошибка: {e}")
                    break

            except Exception as ex:
                print(f"[❌ SAVE] Ошибка при сохранении: {ex}")
                break

@bot.message_handler(commands=['start'])
def start(message):
    try:
        if message.chat.type != "private":
            bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
            return

        if message.chat.id not in user_posts:
            user_posts[message.chat.id] = []

        bot.send_message(
            message.chat.id,
            "Привет! Я PostGoldBot. 👋\nВыберите действие:",
            reply_markup=get_main_keyboard()
        )
    except Exception as e:
        bot.send_message(ADMIN_CHAT_ID, f"Ошибка в /start: {e}")

from collections import defaultdict

def check_daily_limit(user_id, network, city):
    """Проверяет лимит публикаций для пользователя."""
    # Инициализация данных, если их нет
    if user_id not in user_daily_posts:
        user_daily_posts[user_id] = defaultdict(lambda: defaultdict(lambda: {
            "posts": [],
            "deleted_posts": [],
            "last_post_time": None
        }))

    # Проверяем, наступил ли новый день
    if is_new_day(user_daily_posts[user_id][network][city]["last_post_time"]):
        user_daily_posts[user_id][network][city]["posts"] = []
        user_daily_posts[user_id][network][city]["deleted_posts"] = []
        print(f"[DEBUG] Новый день для пользователя {user_id} в сети {network}, городе {city}.")

    # Считаем активные и удалённые публикации
    active_posts = sum(1 for post_time in user_daily_posts[user_id][network][city]["posts"] if is_today(post_time))
    deleted_posts = sum(1 for post_time in user_daily_posts[user_id][network][city]["deleted_posts"] if is_today(post_time))
    total_posts = active_posts + deleted_posts

    # Определяем лимит
    unique_combinations = count_unique_networks_cities(user_id)
    if unique_combinations == 0:
        return False  # Нет оплаченных направлений

    # 🔧 Новый расчёт лимита: без ограничения сверху
    limit = 3 * unique_combinations

    return total_posts < limit


def update_daily_posts(user_id, network, city, remove=False):
    """Обновляет статистику публикаций."""
    with db_lock:
        try:
            if user_id not in user_daily_posts:
                user_daily_posts[user_id] = {}

            if network not in user_daily_posts[user_id]:
                user_daily_posts[user_id][network] = {}

            if city not in user_daily_posts[user_id][network]:
                user_daily_posts[user_id][network][city] = {"posts": [], "deleted_posts": []}

            current_time = now_ekb()

            if remove:
                if user_daily_posts[user_id][network][city]["posts"]:
                    deleted_post = user_daily_posts[user_id][network][city]["posts"].pop()
                    user_daily_posts[user_id][network][city]["deleted_posts"].append(deleted_post)
                    print(f"[DEBUG] Удалено сообщение для пользователя {user_id} в сети {network}, городе {city}.")
            else:
                user_daily_posts[user_id][network][city]["posts"].append(current_time)
                print(f"[DEBUG] Добавлено сообщение для пользователя {user_id} в сети {network}, городе {city}.")

            save_data()
        except Exception as e:
            print(f"[ERROR] Ошибка при обновлении статистики: {e}")

def send_nightly_backup():
    try:
        # ⛔ Не отправляем, если в памяти ничего нет
        if len(paid_users) == 0 and len(user_posts) == 0:
            bot.send_message(ADMIN_CHAT_ID, "⚠️ Ночной бэкап отменён: в памяти нет данных (0 оплат, 0 постов).")
            print("[⛔ BACKUP] База пуста, пропускаем отправку.")
            return

        save_data()  # 💾 Обязательно сохраняем перед отправкой

        with open("bot_data.db", "rb") as f:
            bot.send_document(ADMIN_CHAT_ID, f, caption="🌙 Ночной бэкап базы данных")
            print("[✅ BACKUP] Ночной бэкап успешно отправлен")
    except Exception as e:
        print(f"[❌ BACKUP] Ошибка при отправке бэкапа: {e}")

def schedule_auto_backup():
    def check_and_backup():
        while True:
            now = now_ekb()
            if now.hour == 1 and now.minute == 0:  # 01:00 по Екатеринбургу
                send_nightly_backup()
                time.sleep(61)
            else:
                time.sleep(30)
    t = threading.Thread(target=check_and_backup)
    t.daemon = True
    t.start()

@bot.message_handler(commands=['my_stats'])
def show_user_statistics(message):
    if message.chat.type != "private":
        return

    try:
        stats = get_user_statistics(message.from_user.id)
        response = (
            f"Ваша статистика:\n"
            f"  - Опубликовано сегодня: {stats['published']}\n"
            f"  - Осталось публикаций: {stats['remaining']}\n"
        )
        if stats["details"]:
            response += "  - Детали по сетям:\n"
            for network, cities in stats["details"].items():
                for city, data in cities.items():
                    response += f"    - {network}, {city}: {data['published']} опубликовано, {data['remaining']} осталось\n"
        bot.send_message(message.chat.id, response)
    except Exception as e:
        print(f"Ошибка при получении статистики: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка при получении статистики. Попробуйте позже.")

# Проверка оплаты пользователя
def is_user_paid(user_id, network, city):
    """Проверяет, есть ли у пользователя доступ к выбранной сети и городу, включая 'Все сети'."""
    if isinstance(user_id, str):
        user_id = int(user_id)

    if user_id not in paid_users:
        print(f"[DEBUG] Пользователь {user_id} не найден в списке оплативших.")
        return False

    for entry in paid_users[user_id]:
        if entry["city"] != city:
            continue

        if entry["network"] == "Все сети" or entry["network"] == network:
            end_date = entry["end_date"]

            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except ValueError:
                    print(f"[WARN] Некорректный формат даты: {entry['end_date']}")
                    continue

            if isinstance(end_date, datetime) and now_ekb() < end_date:
                print(f"[DEBUG] Доступ разрешён: {entry}")
                return True
            else:
                print(f"[DEBUG] Срок доступа истёк: {entry}")

    print(f"[DEBUG] Нет активного доступа к {network}, {city}")
    return False

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

# Обработчик callback-запросов админ-панели
@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_"))
def handle_admin_callback(call):
    try:
        if call.data == "admin_add_paid_user":
            bot.send_message(call.message.chat.id, "Введите ID пользователя:")
            bot.register_next_step_handler(call.message, process_user_id_for_payment)
        elif call.data == "admin_list_paid_users":
            show_paid_users(call.message)
        elif call.data == "admin_change_duration":
            bot.send_message(call.message.chat.id, "Введите ID пользователя для изменения срока:")
            bot.register_next_step_handler(call.message, select_user_for_duration_change)
        elif call.data == "admin_add_admin":
            bot.send_message(call.message.chat.id, "Введите ID нового администратора:")
            bot.register_next_step_handler(call.message, add_admin_step)
        elif call.data == "admin_statistics":
            show_statistics_for_admin(call.message.chat.id)
        elif call.data == "admin_delete_user_posts":
            bot.send_message(call.message.chat.id, "🆔 Введите ID пользователя, чьи объявления нужно удалить:")
            bot.register_next_step_handler(call.message, delete_user_posts_step)
        elif call.data == "admin_post_history":  # 👈 ЭТО ДОБАВЬ
            show_post_history(call)              # 👈 И ЭТО
    except Exception as e:
        bot.send_message(call.message.chat.id, f"❌ Ошибка в admin_callback: {e}")

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
    if network not in ["Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети"]:
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

    # Разбираем номер страницы из callback_data
    try:
        parts = call.data.split(":")
        page = int(parts[1]) if len(parts) > 1 else 0
    except:
        page = 0

    try:
        with db_lock:
            with sqlite3.connect("bot_data.db") as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT user_id, network, city, time, reason
                    FROM failed_attempts
                    ORDER BY time DESC
                """)
                attempts = cur.fetchall()

        if not attempts:
            bot.answer_callback_query(call.id, "✅ Нет попыток без доступа.")
            return

        start = page * ATTEMPTS_PER_PAGE
        end = start + ATTEMPTS_PER_PAGE
        total_pages = (len(attempts) - 1) // ATTEMPTS_PER_PAGE + 1
        page_attempts = attempts[start:end]

        response = f"<b>📛 Попытки публикации без доступа (стр. {page+1} из {total_pages}):</b>\n\n"
        for user_id, network, city, time_str, reason in page_attempts:
            try:
                user = bot.get_chat(user_id)
                name = escape_html(user.first_name)
                user_link = f"<a href='https://t.me/{user.username}'>{name}</a>" if user.username else f"<a href='tg://user?id={user.id}'>{name}</a>"
            except:
                user_link = f"ID: <code>{user_id}</code>"

            try:
                time = datetime.fromisoformat(time_str)
                time_formatted = time.strftime('%d.%m.%Y %H:%M')
            except:
                time_formatted = "неизвестно"

            response += (
                f"👤 {user_link}\n"
                f"🌐 Сеть: <b>{escape_html(network)}</b>, Город: <b>{escape_html(city)}</b>\n"
                f"🕐 {time_formatted}\n"
                f"❌ Причина: <i>{escape_html(reason)}</i>\n\n"
            )

        # Кнопки «Назад» и «Вперёд»
        keyboard = InlineKeyboardMarkup()
        buttons = []
        if page > 0:
            buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"show_failed_attempts:{page - 1}"))
        if end < len(attempts):
            buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"show_failed_attempts:{page + 1}"))
        if buttons:
            keyboard.row(*buttons)

        bot.edit_message_text(response, chat_id=call.message.chat.id, message_id=call.message.message_id,
                              parse_mode="HTML", reply_markup=keyboard)
        bot.answer_callback_query(call.id)

    except Exception as e:
        print(f"Ошибка: {e}")
        bot.send_message(call.message.chat.id, f"❌ Ошибка при получении попыток: <code>{escape_html(str(e))}</code>", parse_mode="HTML")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка.")

@bot.callback_query_handler(func=lambda call: call.data.startswith("admin_post_history"))
def show_post_history(call):
    try:
        # 🔢 Получаем номер страницы
        parts = call.data.split(":")
        page = int(parts[1]) if len(parts) > 1 else 0

        with db_lock:
            with sqlite3.connect("bot_data.db") as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT user_id, user_name, network, city, time, chat_id, message_id, deleted, deleted_by
                    FROM post_history
                    ORDER BY time DESC
                """)
                posts = cur.fetchall()

        total_pages = (len(posts) - 1) // POSTS_PER_PAGE + 1
        page_posts = posts[page * POSTS_PER_PAGE: (page + 1) * POSTS_PER_PAGE]

        if not page_posts:
            bot.send_message(call.message.chat.id, "История постов пуста.")
            return

        report = f"<b>📜 История публикаций (стр. {page + 1} из {total_pages}):</b>\n\n"

        for post in page_posts:
            try:
                user_id, user_name, network, city, time_str, chat_id, message_id, deleted, deleted_by = post

                # 🕒 Парсинг времени с защитой
                try:
                    time = datetime.fromisoformat(time_str)
                    if time.tzinfo is not None:
                        time = time.replace(tzinfo=None)
                    formatted_time = time.strftime('%d.%m.%Y %H:%M')
                except Exception as time_parse_error:
                    print(f"[ERROR] Не удалось распарсить дату: {time_str} → {time_parse_error}")
                    formatted_time = time_str

                # 🔍 Попытка вытянуть имя, если неизвестно
                if not user_name or user_name.lower() == "неизвестен":
                    try:
                        user_info = bot.get_chat(user_id)
                        user_name = user_info.first_name or "неизвестен"
                    except:
                        user_name = "неизвестен"

                # Создаём кликабельное имя
                user_link = f"<a href='tg://user?id={user_id}'>{escape_html(user_name)}</a> (ID: <code>{user_id}</code>)"
                network = escape_html(network)
                city = escape_html(city)
                chat_id_short = str(chat_id).replace("-100", "")

                # 🗑 Обработка удаления
                if deleted:
                    deleted_by_display = escape_html(str(deleted_by)) if deleted_by else "неизвестно"
                    status_line = f"❌ <b>Удалён:</b> Да (кем: {deleted_by_display})"
                else:
                    status_line = "✅ <b>Статус:</b> Активен"

                report += (
                    f"👤 <b>Юзер:</b> {user_link}\n"
                    f"🌐 <b>Сеть/Группа:</b> {network} ({city})\n"
                    f"🕒 <b>Время:</b> {formatted_time}\n"
                    f"{status_line}\n"
                    f"🔗 <a href='https://t.me/c/{chat_id_short}/{message_id}'>Перейти к посту</a>\n\n"
                )

            except Exception as inner_e:
                print(f"[ERROR] Ошибка в записи истории: {inner_e}")
                report += f"⚠️ Ошибка в записи: <code>{escape_html(str(inner_e))}</code>\n\n"

        # 🧱 Ограничение на длину сообщения
        if len(report) > 4000:
            report = report[:3900] + "\n\n⚠️ Данные урезаны, слишком длинное сообщение."

        # Кнопки «назад/вперёд»
        keyboard = InlineKeyboardMarkup()
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"admin_post_history:{page - 1}"))
        if (page + 1) * POSTS_PER_PAGE < len(posts):
            nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"admin_post_history:{page + 1}"))
        if nav_buttons:
            keyboard.row(*nav_buttons)

        bot.edit_message_text(report, chat_id=call.message.chat.id, message_id=call.message.message_id,
                              parse_mode="HTML", reply_markup=keyboard)
        bot.answer_callback_query(call.id)

    except Exception as e:
        print(f"[ERROR] История постов: {e}")
        bot.send_message(call.message.chat.id, f"❌ Ошибка при отображении истории: <code>{escape_html(str(e))}</code>", parse_mode="HTML")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка.")

# Функция для добавления администратора
def add_admin_step(message):
    try:
        new_admin_id = int(message.text)
        admins.append(new_admin_id)
        save_data()
        bot.send_message(message.chat.id, f"✅ Пользователь {new_admin_id} добавлен как администратор.")
    except ValueError:
        bot.send_message(message.chat.id, " Ошибка: ID должен быть числом.")

# Функция для отображения списка оплативших
def show_paid_users(message):
    if not paid_users:
        bot.send_message(message.chat.id, "Нет данных об оплативших пользователях.")
        return

    response = "📋 <b>Список оплативших пользователей:</b>\n"
    for user_id, entries in paid_users.items():
        try:
            user_info = bot.get_chat(user_id)
            name = escape_html(user_info.first_name or "")
            username = user_info.username
            id_link = f"<a href='tg://user?id={user_id}'>{user_id}</a>"

            if username:
                full_name = f"{name} (@{username})"
            else:
                full_name = f"{name}"

            user_line = f"👤 <b>Пользователь:</b> {id_link} | {full_name}"
        except Exception:
            user_line = f"👤 <b>Пользователь:</b> <code>{user_id}</code>"

        response += f"\n{user_line}\n"

        for entry in entries:
            network = escape_html(entry.get("network"))
            city = escape_html(entry.get("city"))
            net_key = normalize_network_key(entry.get("network"))

            city_names = []
            if all_cities.get(city) and net_key in all_cities[city]:
                city_names = [escape_html(group["name"]) for group in all_cities[city][net_key]]
            else:
                city_names = [escape_html(city)]

            city_display = ", ".join(city_names)

            end_date = entry.get("end_date")
            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except:
                    end_date = None

            date_str = end_date.strftime("%d.%m.%Y %H:%M") if isinstance(end_date, datetime) else "неизвестно"

            response += f" - Сеть: {network}, Город: {city_display}, Срок: {date_str}\n"

    bot.send_message(message.chat.id, response, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith("change_duration_"))
def handle_duration_change(call):
    try:
        data = call.data.split("_")
        user_id = int(data[2])
        days = int(data[3])

        if user_id not in paid_users:
            bot.answer_callback_query(call.id, " Пользователь не найден в списке оплативших.")
            return

        for entry in paid_users[user_id]:
            end_date = entry.get("end_date")
            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except:
                    end_date = None

            if isinstance(end_date, datetime):
                entry["end_date"] = end_date + timedelta(days=days)

        save_data()
        bot.answer_callback_query(call.id, f"✅ Срок изменён на {days} дней.")
        show_paid_users(call.message)

    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")
        bot.answer_callback_query(call.id, "❌ Произошла ошибка при изменении срока.")

def clear_old_stats():
    """Очищает статистику старше суток."""
    now = datetime.now()
    for user_id, posts in user_posts.items():
        user_posts[user_id] = [post for post in posts if now - post["time"] < timedelta(days=1)]

@bot.message_handler(commands=['statistics'])
def show_statistics_for_admin(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "⛔ У вас нет прав для просмотра статистики.")
        return

    # Очистка старой статистики
    clear_old_stats()

    stats = get_admin_statistics()
    if not stats:
        bot.send_message(chat_id, "ℹ️ Нет данных о публикациях.")
        return

    response = "<b>📊 Статистика публикаций:</b>\n\n"

    for user_id, user_stats in stats.items():
        try:
            user_info = bot.get_chat(user_id)
            user_name = escape_html(user_info.first_name)
            user_link = f"<a href='https://t.me/{user_info.username}'>{user_name}</a>" if user_info.username else f"<a href='tg://user?id={user_info.id}'>{user_name}</a>"
        except Exception as e:
            print(f"DEBUG: Ошибка получения данных пользователя {user_id}: {e}")
            user_link = f"ID <code>{user_id}</code>"

        response += (
            f"👤 {user_link}\n"
            f"📨 Опубликовано: <b>{user_stats['published']}</b>\n"
            f"📉 Осталось: <b>{user_stats['remaining']}</b>\n"
        )

        if user_stats["details"]:
            response += "🧾 <b>Детали по сетям и городам:</b>\n"
            for network, cities in user_stats["details"].items():
                net_key = normalize_work_key(network)
                for city, data in cities.items():
                    expire_str = "(неизвестно)"
                    for paid in paid_users.get(user_id, []):
                        print(f"DEBUG: Проверка записи оплаты для user_id={user_id}: network={paid.get('network')}, city={paid.get('city')}")
                        if normalize_network_key(paid.get("network")) == net_key and paid.get("city") == city:
                            end_date = paid.get("end_date")
                            if isinstance(end_date, str):
                                try:
                                    end_date = datetime.fromisoformat(end_date)
                                except ValueError:
                                    print(f"DEBUG: Ошибка преобразования end_date: {end_date}")
                                    end_date = None
                            if isinstance(end_date, datetime):
                                try:
                                    if end_date >= now_ekb():  # <-- Исправлено здесь
                                        expire_str = f"⏳ до {end_date.strftime('%d.%m.%Y')}"
                                        print(f"DEBUG: Найден срок для {network}, {city}: {expire_str}")
                                    else:
                                        print(f"DEBUG: Срок истёк для {network}, {city}")
                                        expire_str = "(срок истёк)"
                                except TypeError as te:
                                    print(f"DEBUG: Ошибка сравнения дат: {te}")
                            break
                    else:
                        print(f"DEBUG: Запись оплаты для {network}, {city} не найдена для user_id={user_id}")

                    location_names = [loc["name"] for loc in all_cities.get(city, {}).get(net_key, [])]
                    location_str = ", ".join(location_names) if location_names else city

                    response += (
                        f"  └ 🧩 <b>{escape_html(network)}</b>, 📍<b>{escape_html(city)}</b> → "
                        f"{escape_html(location_str)} {expire_str}: "
                        f"<b>{data['published']} / {data['remaining']}</b>\n"
                    )

        if user_stats["links"]:
            unique_links = list(set(user_stats["links"]))
            response += "🔗 <b>Ссылки на публикации:</b>\n"
            for link in unique_links:
                response += f"  • <a href='{link}'>{link}</a>\n"

        response += "\n"

    try:
        bot.send_message(chat_id, response, parse_mode="HTML")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при отправке статистики: <code>{escape_html(str(e))}</code>", parse_mode="HTML")

# Функция для изменения срока оплаты
def select_user_for_duration_change(message):
    try:
        user_id = int(message.text)
        if user_id not in paid_users:
            bot.send_message(message.chat.id, " Пользователь не найден в списке оплативших.")
            return

        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("+1 день", callback_data=f"change_duration_{user_id}_1"))
        markup.add(types.InlineKeyboardButton("+1 неделя", callback_data=f"change_duration_{user_id}_7"))
        markup.add(types.InlineKeyboardButton("+1 месяц", callback_data=f"change_duration_{user_id}_30"))
        markup.add(types.InlineKeyboardButton("-1 день", callback_data=f"change_duration_{user_id}_-1"))
        markup.add(types.InlineKeyboardButton("-1 неделя", callback_data=f"change_duration_{user_id}_-7"))
        markup.add(types.InlineKeyboardButton("-1 месяц", callback_data=f"change_duration_{user_id}_-30"))
        bot.send_message(message.chat.id, "Выберите действие для изменения срока:", reply_markup=markup)
    except ValueError:
        bot.send_message(message.chat.id, " Ошибка: ID должен быть числом.")

# Обработчик изменения срока оплаты
@bot.callback_query_handler(func=lambda call: call.data.startswith("change_duration_"))
def handle_duration_change(call):
    try:
        # Разбираем данные из callback
        data = call.data.split("_")
        user_id = int(data[2])
        days = int(data[3])

        # Проверяем, есть ли пользователь в списке оплативших
        if user_id not in paid_users:
            bot.answer_callback_query(call.id, " Пользователь не найден в списке оплативших.")
            return

        # Обновляем срок оплаты для всех записей пользователя
        for entry in paid_users[user_id]:
            end_date = entry.get("end_date")
            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except:
                    end_date = None

            if isinstance(end_date, datetime):
                entry["end_date"] = end_date + timedelta(days=days)

        # Сохраняем изменения
        save_data()

        # Уведомляем администратора
        bot.answer_callback_query(call.id, f"✅ Срок изменён на {days} дней.")
        show_paid_users(call.message)
    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")
        bot.answer_callback_query(call.id, " Произошла ошибка при изменении срока.")

@bot.message_handler(func=lambda message: message.text == "Создать новое объявление")
def create_new_post(message):
    if message.chat.type != "private":
        bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
        return
    bot.send_message(message.chat.id, "Напишите текст объявления:")
    bot.register_next_step_handler(message, process_text)

@bot.message_handler(func=lambda message: message.text == "Удалить объявление")
def handle_delete_post(message):
    if message.chat.type != "private":
        bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
        return
    if message.chat.id in user_posts and user_posts[message.chat.id]:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        for post in user_posts[message.chat.id]:
            time_formatted = format_time(post["time"])
            button_text = f"Удалить: {time_formatted}, {post['city']}, {post['network']}"
            markup.add(button_text)
        markup.add("Отмена")
        bot.send_message(message.chat.id, "Выберите объявление для удаления:", reply_markup=markup)
        bot.register_next_step_handler(message, process_delete_choice)
    else:
        bot.send_message(message.chat.id, "❌ У вас нет опубликованных объявлений.")

@bot.message_handler(func=lambda message: message.text == "Удалить все объявления")
def handle_delete_all_posts(message):
    if message.chat.type != "private":
        bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
        return
    if message.chat.id in user_posts and user_posts[message.chat.id]:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
        markup.add("Да, удалить всё", "Нет, отменить")
        bot.send_message(message.chat.id, "Вы уверены, что хотите удалить все свои объявления?", reply_markup=markup)
        bot.register_next_step_handler(message, process_delete_all_choice)
    else:
        bot.send_message(message.chat.id, "❌ У вас нет опубликованных объявлений.")

def process_delete_choice(message):
    if message.text == "Отмена":
        bot.send_message(message.chat.id, "Удаление отменено.", reply_markup=get_main_keyboard())
    else:
        try:
            for post in user_posts[message.chat.id]:
                time_formatted = format_time(post["time"])
                if message.text == f"Удалить: {time_formatted}, {post['city']}, {post['network']}":
                    # Удаляем пост
                    try:
                        bot.delete_message(post["chat_id"], post["message_id"])
                    except Exception:
                        pass

                    # Сохраняем пост в историю
                    add_post_to_history(
                        user_id=message.chat.id,
                        user_name=get_user_name(message.from_user),
                        network=post["network"],
                        city=post["city"],
                        chat_id=post["chat_id"],
                        message_id=post["message_id"],
                        deleted=True,
                        deleted_by="Пользователь"  # Или ID администратора, если удаляет админ
                    )

                    # Удаляем пост из списка
                    user_posts[message.chat.id].remove(post)
                    bot.send_message(message.chat.id, "✅ Объявление успешно удалено.", reply_markup=get_main_keyboard())
                    return
            bot.send_message(message.chat.id, "❌ Объявление не найдено.")
        except (ValueError, IndexError):
            bot.send_message(message.chat.id, "❌ Ошибка! Пожалуйста, выберите объявление из списка.")

def process_delete_all_choice(message):
    if message.text == "Да, удалить всё":
        for post in user_posts[message.chat.id]:
            # Удаляем пост
            try:
                bot.delete_message(post["chat_id"], post["message_id"])
            except Exception:
                pass

            # Сохраняем пост в историю
            add_post_to_history(
                user_id=message.chat.id,
                user_name=get_user_name(message.from_user),
                network=post["network"],
                city=post["city"],
                chat_id=post["chat_id"],
                message_id=post["message_id"],
                deleted=True,
                deleted_by="Пользователь"  # Или ID администратора, если удаляет админ
            )

        # Очищаем список постов
        user_posts[message.chat.id] = []
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
    markup.add("Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети", "Назад")
    return markup

def normalize_network_key(name):
    """Приводим название сети к ключу all_cities: mk, parni, ns"""
    if name == "Мужской Клуб":
        return "mk"
    elif name == "ПАРНИ 18+":
        return "parni"
    elif name in ["НС", "Знакомства 66", "Знакомства 74"]:
        return "ns"
    return None

def select_network(message, text, media_type, file_id):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Напишите текст объявления:")
        bot.register_next_step_handler(message, process_text)
        return

    selected_network = message.text.strip()
    valid_networks = ["Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети"]

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
    display_name = escape_html(get_user_name(message.from_user))
    text = escape_html(text)
    networks = ["Мужской Клуб", "ПАРНИ 18+", "НС"] if selected_network == "Все сети" else [selected_network]

    # Создаем жирное и кликабельное имя пользователя, используя tg://user?id={user_id}
    user_name = f'<b><a href="tg://user?id={user_id}">{display_name}</a></b>'

    was_published = False

    for network in networks:
        net_key = normalize_network_key(network)
        city_data = all_cities.get(city, {}).get(net_key)

        if not city_data:
            continue

        if not is_user_paid(user_id, network, city):
            log_failed_attempt(user_id, network, city, "Нет доступа")
            continue

        user_stats = get_user_statistics(user_id)
        city_stats = user_stats.get("details", {}).get(network, {}).get(city, {})
        if city_stats.get("remaining", 0) <= 0:
            bot.send_message(message.chat.id, f"⛔ Лимит публикаций исчерпан для <b>{escape_html(network)}</b>, город <b>{escape_html(city)}</b>", parse_mode="HTML")
            log_failed_attempt(user_id, network, city, "Лимит исчерпан")
            continue

        signature = escape_html(network_signatures.get(network, ""))
        full_text = f"📢 Объявление от {user_name}:\n\n{text}\n\n{signature}"

        # Кнопка "Написать" больше не нужна, так как имя уже кликабельное
        reply_markup = None

        for location in city_data:
            chat_id = location["chat_id"]
            try:
                if media_type == "photo":
                    sent_message = bot.send_photo(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                elif media_type == "video":
                    sent_message = bot.send_video(chat_id, file_id, caption=full_text, parse_mode="HTML", reply_markup=reply_markup)
                else:
                    sent_message = bot.send_message(chat_id, full_text, parse_mode="HTML", reply_markup=reply_markup)

                # ✅ Добавляем в user_posts
                if user_id not in user_posts:
                    user_posts[user_id] = []

                user_posts[user_id].append({
                    "message_id": sent_message.message_id,
                    "chat_id": chat_id,
                    "time": now_ekb(),
                    "city": location["name"],
                    "network": network,
                    "user_name": display_name
                })

                # ✅ Добавляем в post_history
                add_post_to_history(
                    user_id=user_id,
                    user_name=display_name,
                    network=network,
                    city=location["name"],
                    chat_id=chat_id,
                    message_id=sent_message.message_id
                )

                # ✅ Обновляем лимиты
                if user_id not in user_daily_posts:
                    user_daily_posts[user_id] = {}
                if network not in user_daily_posts[user_id]:
                    user_daily_posts[user_id][network] = {}
                if city not in user_daily_posts[user_id][network]:
                    user_daily_posts[user_id][network][city] = {"posts": [], "deleted_posts": []}

                user_daily_posts[user_id][network][city]["posts"].append(now_ekb())

                bot.send_message(
                    message.chat.id,
                    f"✅ Объявление опубликовано в сети <b>{escape_html(network)}</b>, городе <b>{escape_html(location['name'])}</b>.",
                    parse_mode="HTML"
                )
                was_published = True

            except telebot.apihelper.ApiTelegramException as e:
                log_failed_attempt(user_id, network, city, f"Ошибка отправки: {e.description}")
                bot.send_message(message.chat.id, f"❌ <b>Ошибка:</b> {escape_html(e.description)}", parse_mode="HTML")

    if not was_published:
        markup = types.InlineKeyboardMarkup()
        url = "https://t.me/FAQMKBOT" if selected_network == "Мужской Клуб" else "https://t.me/FAQZNAKBOT"
        markup.add(types.InlineKeyboardButton("Купить рекламу", url=url))
        bot.send_message(
            message.chat.id,
            "⛔ У вас нет прав на публикацию в этой сети/городе. Обратитесь к администратору.",
            reply_markup=markup
        )

    save_data()
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

        response = (
            f"📊 <b>Ваша статистика на сегодня:</b>\n"
            f"📨 Опубликовано: <b>{stats['published']}</b>\n"
            f"📉 Осталось публикаций: <b>{stats['remaining']}</b>\n"
        )

        if stats["details"]:
            response += "\n🗂️ <b>Детали по сетям и городам:</b>\n"
            for network, cities in stats["details"].items():
                net_key = normalize_network_key(network)
                for city, data in cities.items():
                    expire_str = "⏳ неизвестно"

                    for paid in paid_users.get(user_id, []):
                        if normalize_network_key(paid["network"]) == net_key and paid["city"] == city:
                            end = paid.get("end_date")
                            if isinstance(end, str):
                                try:
                                    end = datetime.fromisoformat(end)
                                except:
                                    end = None
                            if isinstance(end, datetime):
                                expire_str = f"⏳ до {end.strftime('%d.%m.%Y')}"
                            break

                    location_names = [loc["name"] for loc in all_cities.get(city, {}).get(net_key, [])]
                    location_str = ", ".join(location_names) if location_names else city

                    response += (
                        f"  └ 🧩 <b>{network}</b>, 📍<b>{city}</b> → {location_str} {expire_str}:\n"
                        f"     • Опубликовано: <b>{data['published']}</b>, Осталось: <b>{data['remaining']}</b>\n"
                    )

        bot.send_message(message.chat.id, response, parse_mode="HTML")

    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Произошла ошибка при получении статистики: {e}")

def delete_user_posts_step(message):
    try:
        user_id = int(message.text)

        if user_id not in user_posts or not user_posts[user_id]:
            bot.send_message(message.chat.id, "❌ У пользователя нет объявлений.")
            return

        # Формируем список постов
        preview = f"📋 Найдено <b>{len(user_posts[user_id])}</b> объявлений у пользователя ID <code>{user_id}</code>:\n\n"
        for post in user_posts[user_id]:
            date_str = format_time(post["time"])
            preview += f"• 🧩 <b>{post['network']}</b> | 📍<b>{post['city']}</b> | 🕒 {date_str}\n"

        # Кнопки: подтвердить / отменить
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
    deleted = 0

    if user_id in user_posts:
        for post in user_posts[user_id]:
            try:
                bot.delete_message(post["chat_id"], post["message_id"])
                deleted += 1
            except Exception as e:
                print(f"[WARN] Не удалось удалить сообщение: {e}")

            post["deleted"] = True  # ✅ Просто помечаем как удалённый

        save_data()

    bot.edit_message_text(
        f"✅ Удалено {deleted} объявлений пользователя ID: <code>{user_id}</code>.",
        call.message.chat.id,
        call.message.message_id,
        parse_mode="HTML"
    )

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode('utf-8'))
    bot.process_new_updates([update])
    return 'ok', 200

@app.route('/')
def index():
    return '✅ Бот запущен и работает!'

if __name__ == '__main__':
    init_db()
    paid_users, admins, user_posts = load_data()

    print(f"[📂 LOAD] Загружено: {len(paid_users)} оплат, {len(user_posts)} постов, {len(admins)} админов")

    # 🔁 Добавляем вечных админов
    for core_admin in CORE_ADMINS:
        if core_admin not in admins:
            admins.append(core_admin)

    # 💾 Сохраняем, только если есть что
    if paid_users or user_posts:
        save_data()
    else:
        print("[⚠️ INIT] Пропускаем save_data(): нет данных для сохранения.")

    schedule_auto_backup()

    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)