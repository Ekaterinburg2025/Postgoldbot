# Импорты
import os
import json
import sqlite3
import logging
import threading
from datetime import datetime, timedelta
from collections import defaultdict
import pytz
import telebot
from telebot import types
from flask import Flask, request, abort
from telebot.apihelper import ApiTelegramException
from pytz import timezone
ekaterinburg_tz = pytz.timezone('Asia/Yekaterinburg')

def get_current_time():
    return datetime.now(tz).isoformat()

def parse_time(iso_str):
    return datetime.fromisoformat(iso_str).astimezone(tz)

def is_today(iso_str):
    try:
        dt = parse_time(iso_str)
        now = datetime.now(tz)
        return dt.date() == now.date()
    except Exception as e:
        print(f"[ERROR] Ошибка в is_today: {e}")
        return False

# Создаём Flask-приложение
app = Flask(__name__)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="bot.log"
)

# Токен бота
TOKEN = os.getenv("BOT_TOKEN")  # Используем переменную окружения для токена
bot = telebot.TeleBot(TOKEN)

def safe_send_message(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        print(f"[Ошибка отправки сообщения] chat_id={chat_id} — {e}")
        return None

# URL вебхука
WEBHOOK_URL = "https://postgoldbot.onrender.com/webhook"

# Админ ID (ваш ID)
ADMIN_CHAT_ID = 479938867  # Замените на ваш ID

# Глобальные переменные
paid_users = {}
user_posts = {}
user_daily_posts = {}
user_statistics = {}
admins = [ADMIN_CHAT_ID]
db_lock = threading.Lock()

# Инициализация базы данных
def init_db():
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paid_users (
            user_id INTEGER,
            network TEXT,
            city TEXT,
            end_date TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_users (
            user_id INTEGER PRIMARY KEY
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_posts (
            user_id INTEGER,
            network TEXT,
            city TEXT,
            time TEXT,
            chat_id INTEGER,
            message_id INTEGER
        )
    """)
    conn.commit()
    conn.close()

# Загрузка данных при старте
def load_data():
    with db_lock:  # Блокируем доступ к данным
        try:
            with sqlite3.connect("bot_data.db") as conn:  # Открываем соединение с базой данных
                cur = conn.cursor()

                # Загружаем оплативших пользователей
                cur.execute("SELECT user_id, network, city, end_date FROM paid_users")
                global paid_users
                paid_users = {}
                for user_id, network, city, end_date in cur.fetchall():
                    if user_id not in paid_users:
                        paid_users[user_id] = []
                    paid_users[user_id].append({
                        "network": network,
                        "city": city,
                        "end_date": datetime.fromisoformat(end_date)
                    })

                # Загружаем админов
                cur.execute("SELECT user_id FROM admin_users")
                admin_users = [row[0] for row in cur.fetchall()]

                # Загружаем публикации
                cur.execute("SELECT user_id, network, city, time, chat_id, message_id FROM user_posts")
                user_posts = {}
                for user_id, network, city, time, chat_id, message_id in cur.fetchall():
                    if user_id not in user_posts:
                        user_posts[user_id] = []
                    user_posts[user_id].append({
                        "network": network,
                        "city": city,
                        "time": time,
                        "chat_id": chat_id,
                        "message_id": message_id
                    })

                return paid_users, admin_users, user_posts  # Возвращаем данные

        except Exception as e:
            print(f"[ERROR] Ошибка при загрузке данных из базы: {e}")
            return {}, [], {}  # Возвращаем пустые данные в случае ошибки

# Инициализация базы данных
init_db()

# Добавляем блокировку для синхронизации доступа к базе данных
db_lock = threading.Lock()

# Загрузка данных при старте
paid_users, admin_users, user_posts = load_data()

# Списки chat_id для каждой сети и города
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
    "Санкт-Петербург": -1002485776859,
    "Общая группа Юга": -1001814693664,
    "Общая группа Дальнего Востока": -1002161346845,
    "Общая группа Тюмень и Север": -1002210623988,
    "Тестовая группа ️": -1002426733876
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

# Словарь для замены названий городов для сети НС
ns_city_substitution = {
    "Екатеринбург": "Знакомства 66",
    "Челябинск": "Знакомства 74"
}

# Словарь для хранения всех сообщений пользователей
user_posts = {}

# Словарь для хранения оплативших пользователей
paid_users = {}

# Словарь для учёта публикаций за сутки
user_daily_posts = {}

# Статичные подписи для каждой сети
network_signatures = {
    "Мужской Клуб": "️ 🕸️Реклама. Согласовано с администрацей сети МК.",
    "ПАРНИ 18+": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫",
    "НС": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫"
}

# Вызов при старте бота
init_db()

def load_paid_users():
    with db_lock:  # Блокируем доступ к данным
        with sqlite3.connect("bot_data.db") as conn:  # Открываем соединение с базой данных
            cur = conn.cursor()

            # Выполняем SQL-запрос
            cur.execute("SELECT user_id, network, city, end_date FROM paid_users")
            paid_users = {}

            # Обрабатываем результаты запроса
            for user_id, network, city, end_date in cur.fetchall():
                if user_id not in paid_users:
                    paid_users[user_id] = []
                paid_users[user_id].append({
                    "network": network,
                    "city": city,
                    "end_date": datetime.fromisoformat(end_date)
                })

            return paid_users  # Возвращаем данные

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

def remove_paid_user(user_id, network, city):
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM paid_users WHERE user_id = ?", (user_id,))
            conn.commit()
            # Закрытие соединения не обязательно, так как `with` автоматически закроет его

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

def remove_admin_user(user_id):
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM admin_users WHERE user_id = ?", (user_id,))
            conn.commit()

def is_admin(user_id):
    admin_users = load_admin_users()  # Загружаем список администраторов
    return user_id in admin_users  # Проверяем, есть ли пользователь в списке

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
    return last_post_time.date() < datetime.now().date()

def is_today(post_time):
    """Проверяет, было ли время публикации сегодня."""
    return post_time.date() == datetime.now().date()

# Установите ваш часовой пояс
your_timezone = pytz.timezone("Asia/Yekaterinburg")

# Пример использования
now = datetime.now(your_timezone)
print(now)

# Загружаем данные при запуске
load_data()

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

    expiry_date = datetime.now() + timedelta(days=days)

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

    # Уведомление админу
    bot.send_message(
        ADMIN_CHAT_ID,
        f"✅ Пользователь {user_name} (ID: {user_id}) добавлен в сеть «{network}», город {city} на {days} дн.\n📅 Действует до: {expiry_date.strftime('%d.%m.%Y')}"
    )

    # Уведомление текущему администратору в боте
    bot.send_message(
        message.chat.id,
        f"✅ Пользователь {user_id} добавлен в сеть «{network}», город {city} на {days} дн.\n📅 Действует до: {expiry_date.strftime('%d.%m.%Y')}"
    )

def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} is not serializable")

def deserialize_datetime(d):
    for key, value in d.items():
        if isinstance(value, str) and key in ['expiry_date', 'last_post_time', 'time']:
            try:
                d[key] = datetime.fromisoformat(value)
            except ValueError:
                pass
    return d

def is_new_day(last_post_time):
    if last_post_time is None:
        return True

    if isinstance(last_post_time, str):
        last_post_time = datetime.fromisoformat(last_post_time)

    current_time = datetime.now(ekaterinburg_tz)
    print(current_time)
    return current_time.date() > last_post_time.date()

def get_user_statistics(user_id):
    stats = {"published": 0, "remaining": 9, "details": {}}
    if user_id in user_daily_posts:
        for network, cities in user_daily_posts[user_id].items():
            stats["details"][network] = {}
            for city, post_data in cities.items():
                active_posts = len(post_data["posts"])
                deleted_posts = len(post_data["deleted_posts"])
                total_posts = active_posts + deleted_posts
                stats["details"][network][city] = {
                    "published": total_posts,
                    "remaining": max(0, 3 - total_posts)
                }
                stats["published"] += total_posts
        stats["remaining"] = max(0, 9 - stats["published"])
    return stats

def is_today(timestamp):
    """Проверяет, относится ли временная метка к сегодняшнему дню."""
    now = datetime.now()
    return datetime.fromisoformat(timestamp).date() == now.date()

def check_payment(user_id, network, city):
    """Проверяет, оплатил ли пользователь доступ к сети и городу."""
    if str(user_id) not in paid_users:
        print(f"[DEBUG] Пользователь {user_id} не найден в оплативших.")
        return False

    for payment in paid_users[str(user_id)]:
        # Проверяем, не истёк ли срок оплаты
        if payment["expiry_date"] < datetime.now():
            print(f"[DEBUG] Срок оплаты истёк для пользователя {user_id}: {payment}")
            continue  # Пропускаем истёкшие платежи

        # Если оплачен доступ ко всем сетям для этого города
        if payment["network"] == "Все сети" and payment["city"] == city:
            print(f"[DEBUG] Пользователь {user_id} оплатил доступ ко всем сетям для города {city}.")
            return True

        # Если оплачен доступ к конкретной сети и городу
        if payment["network"] == network and payment["city"] == city:
            print(f"[DEBUG] Пользователь {user_id} оплатил доступ к сети {network} для города {city}.")
            return True

    print(f"[DEBUG] Пользователь {user_id} не оплатил доступ к сети {network} для города {city}.")
    return False

def validate_text_length(text):
    """Проверяет, что текст объявления не превышает 1000 символов."""
    return len(text) <= 1000

# Сохранение данных в файл
def save_data(retries=3, delay=0.5):
    """Сохраняет данные в базу данных с повторной попыткой при блокировке."""
    for attempt in range(retries):
        with db_lock:  # Гарантируем одиночный доступ
            try:
                with sqlite3.connect("bot_data.db", timeout=5) as conn:  # timeout поможет тоже
                    cur = conn.cursor()

                    # Очищаем таблицы
                    cur.execute("DELETE FROM paid_users")
                    cur.execute("DELETE FROM admin_users")
                    cur.execute("DELETE FROM user_posts")

                    # Сохраняем оплативших пользователей
                    for user_id, entries in paid_users.items():
                        for entry in entries:
                            cur.execute("""
                                INSERT INTO paid_users (user_id, network, city, end_date)
                                VALUES (?, ?, ?, ?)
                            """, (user_id, entry["network"], entry["city"], entry["end_date"].isoformat()))

                    # Сохраняем админов
                    for user_id in admin_users:
                        cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (user_id,))

                    # Сохраняем публикации
                    for user_id, posts in user_posts.items():
                        for post in posts:
                            cur.execute("""
                                INSERT INTO user_posts (user_id, network, city, time, chat_id, message_id)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                user_id, post["network"], post["city"],
                                post["time"], post["chat_id"], post["message_id"]
                            ))

                    conn.commit()
                    print("[DEBUG] Данные успешно сохранены.")
                    return  # Успешно, выходим
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    print(f"[WARN] Попытка {attempt+1} — БД заблокирована, пробуем снова через {delay} сек...")
                    time.sleep(delay)
                else:
                    print(f"[ERROR] Ошибка при сохранении данных: {e}")
                    break
            except Exception as e:
                print(f"[ERROR] Ошибка при сохранении данных: {e}")
                break
    else:
        print("[ERROR] Не удалось сохранить данные после нескольких попыток.")

# Клавиатура выбора сети
def get_network_markup():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети", "Назад")
    return markup

# Основная клавиатура
def get_main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Создать новое объявление", "Удалить объявление")
    markup.add("Удалить все объявления", "📊 Моя статистика")
    return markup

# Форматирование времени
def format_time(timestamp):
    tz = pytz.timezone('Asia/Yekaterinburg')
    local_time = timestamp.astimezone(tz)
    return local_time.strftime("%H:%M, %d %B %Y")

def is_new_day(last_post_time):
    if last_post_time is None:
        return True

    if isinstance(last_post_time, str):
        last_post_time = datetime.fromisoformat(last_post_time)

    current_time = datetime.now(ekaterinburg_tz)
    print(current_time)
    return current_time.date() > last_post_time.date()

# Получение имени пользователя
def get_user_name(user):
    name = user.first_name
    if user.username:
        return f"@{user.username}"
    else:
        return f"{name} (ID: {user.id})"

# Проверка, является ли пользователь администратором
def is_admin(user_id):
    return user_id in admins

# Команда /start
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
            "Привет! Я PostGoldBot. \nВыберите действие:",
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
        user_daily_posts[user_id][network][city]["posts"] = []  # Сбрасываем активные публикации
        user_daily_posts[user_id][network][city]["deleted_posts"] = []  # Сбрасываем удалённые публикации
        print(f"[DEBUG] Новый день для пользователя {user_id} в сети {network}, городе {city}.")

    # Считаем активные и удалённые публикации
    active_posts = sum(1 for post_time in user_daily_posts[user_id][network][city]["posts"] if is_today(post_time))
    deleted_posts = sum(1 for post_time in user_daily_posts[user_id][network][city]["deleted_posts"] if is_today(post_time))
    total_posts = active_posts + deleted_posts

    # Определяем лимит
    unique_combinations = count_unique_networks_cities(user_id)
    if unique_combinations == 0:
        return False  # Пользователь не оплатил ни одного города

    # Лимит = 3 * количество уникальных комбинаций, но не более 9
    limit = min(3 * unique_combinations, 9)

    # Проверяем лимит
    if network == "Все сети":
        # Общий лимит для всех сетей (9 публикаций)
        return total_posts < 9
    else:
        # Лимит для конкретной сети (3 публикации)
        return total_posts < limit

def update_daily_posts(user_id, network, city, remove=False):
    """Обновляет статистику публикаций."""
    with db_lock:  # Используем блокировку
        try:
            if user_id not in user_daily_posts:
                user_daily_posts[user_id] = {}

            if network not in user_daily_posts[user_id]:
                user_daily_posts[user_id][network] = {}

            if city not in user_daily_posts[user_id][network]:
                user_daily_posts[user_id][network][city] = {"posts": [], "deleted_posts": []}

            current_time = datetime.now()

            if remove:
                if user_daily_posts[user_id][network][city]["posts"]:
                    deleted_post = user_daily_posts[user_id][network][city]["posts"].pop()
                    user_daily_posts[user_id][network][city]["deleted_posts"].append(deleted_post)
                    print(f"[DEBUG] Удалено сообщение для пользователя {user_id} в сети {network}, городе {city}.")
            else:
                user_daily_posts[user_id][network][city]["posts"].append(current_time)
                print(f"[DEBUG] Добавлено сообщение для пользователя {user_id} в сети {network}, городе {city}.")

            save_data()  # Сохраняем данные
        except Exception as e:
            print(f"[ERROR] Ошибка при обновлении статистики: {e}")

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
    if isinstance(user_id, str):
        user_id = int(user_id)  # Приводим к int, как в основной логике

    print(f"[DEBUG] Проверяем оплату: user_id={user_id}, network={network}, city={city}")

    if user_id not in paid_users:
        print("[DEBUG] Пользователь не найден в словаре оплаченных.")
        return False

    for entry in paid_users[user_id]:
        if entry["network"] == network and entry["city"] == city:
            end_date = entry["end_date"]
            print(f"[DEBUG] Найдено совпадение. Срок оплаты: {end_date}")

            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except ValueError:
                    print("[DEBUG] Ошибка формата даты.")
                    return False

            if not isinstance(end_date, datetime):
                print(f"[DEBUG] end_date неправильного типа: {type(end_date)}")
                return False

            if datetime.now() < end_date:
                print("[DEBUG] Оплата актуальна.")
                return True
            else:
                print("[DEBUG] Срок подписки истёк.")

    print("[DEBUG] Не найдено подходящего тарифа или срок закончился.")
    return False

# Админ-панель
@bot.message_handler(commands=['admin'])
def admin_panel(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, " У вас нет прав для выполнения этой команда.")
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Добавить оплатившего", callback_data="admin_add_paid_user"))
    markup.add(types.InlineKeyboardButton("Список оплативших", callback_data="admin_list_paid_users"))
    markup.add(types.InlineKeyboardButton("Изменить срок оплаты", callback_data="admin_change_duration"))
    markup.add(types.InlineKeyboardButton("Добавить администратора", callback_data="admin_add_admin"))
    markup.add(types.InlineKeyboardButton("Статистика публикаций", callback_data="admin_statistics"))
    bot.send_message(message.chat.id, "Админ-панель:", reply_markup=markup)

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
            show_statistics(call.message)
    except Exception as e:
        print(f"Ошибка в handle_admin_callback: {e}")

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
    if network in ["Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети"]:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)
        if network == "Мужской Клуб":
            cities = list(chat_ids_mk.keys())
        elif network == "ПАРНИ 18+":
            cities = list(chat_ids_parni.keys())
        elif network == "НС":
            cities = list(chat_ids_ns.keys())
        else:
            cities = list(set(list(chat_ids_mk.keys()) + list(chat_ids_parni.keys()) + list(chat_ids_ns.keys())))
        for city in cities:
            markup.add(city)
        markup.add("Назад")
        bot.send_message(message.chat.id, "📍 Выберите город для добавления пользователя:", reply_markup=markup)
        bot.register_next_step_handler(message, lambda m: select_city_for_payment(m, user_id, network))
    else:
        bot.send_message(message.chat.id, " Ошибка! Выберите правильную сеть.")
        bot.register_next_step_handler(message, lambda m: select_network_for_payment(m, user_id))

# Функция для выбора города при добавления оплатившего
def select_city_for_payment(message, user_id, network):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "️ Выберите сеть для добавления пользователя:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, lambda m: select_network_for_payment(m, user_id))
        return

    city = message.text
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("День", "Неделя", "Месяц")
    bot.send_message(message.chat.id, " Выберите срок оплаты:", reply_markup=markup)
    bot.register_next_step_handler(message, lambda m: select_duration_for_payment(m, user_id, network, city))

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

    response = "Список оплативших пользователей:\n"
    for user_id, entries in paid_users.items():
        try:
            user_info = bot.get_chat(user_id)
            user_name = get_user_name(user_info)
        except Exception:
            user_name = f"(ID: {user_id})"
        
        response += f"Пользователь {user_name}:\n"
        for entry in entries:
            end_date = entry["end_date"]
            if isinstance(end_date, str):
                end_date = datetime.fromisoformat(end_date)
            response += f" - Сеть: {entry['network']}, Город: {entry['city']}, " + \
                       f"Срок: {end_date.strftime('%d.%m.%Y %H:%M')}\n"
    
    bot.send_message(message.chat.id, response)

def handle_duration_change(call):
    try:
        data = call.data.split("_")
        user_id = int(data[2])
        days = int(data[3])

        if user_id not in paid_users:
            bot.answer_callback_query(call.id, " Пользователь не найден в списке оплативших.")
            return

        for entry in paid_users[user_id]:
            end_date = entry["end_date"]
            if isinstance(end_date, str):  # Если дата в формате строки
                end_date = datetime.fromisoformat(end_date)
            entry["end_date"] = end_date + timedelta(days=days)

        save_data()
        bot.answer_callback_query(call.id, f"✅ Срок изменён на {days} дней.")
        show_paid_users(call.message)
    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")

def show_statistics(message):
    if not user_statistics:
        bot.send_message(message.chat.id, "Нет данных о публикациях.")
        return

    response = "Статистика публикаций:\n"
    for user_id, stats in user_statistics.items():
        user_info = bot.get_chat(user_id)
        user_name = get_user_name(user_info)
        response += f"Пользователь {user_name}: {stats['count']} публикаций\n"
    bot.send_message(message.chat.id, response)

def get_admin_statistics():
    statistics = {}

    for user_id, user_data in user_daily_posts.items():
        stats = {"published": 0, "remaining": 9, "links": [], "details": {}}

        for network, cities in user_data.items():
            stats["details"][network] = {}
            for city, post_data in cities.items():
                active_posts = len(post_data["posts"])
                deleted_posts = len(post_data["deleted_posts"])
                total_posts = active_posts + deleted_posts

                stats["details"][network][city] = {
                    "published": total_posts,
                    "remaining": max(0, 3 - total_posts)
                }

                stats["published"] += total_posts

        stats["remaining"] = max(0, 9 - stats["published"])

        # Ссылки на активные посты (если нужны)
        if user_id in user_posts:
            for post in user_posts[user_id]:
                if is_today(post["time"]):
                    stats["links"].append(f"https://t.me/c/{str(post['chat_id'])[4:]}/{post['message_id']}")

        statistics[user_id] = stats

    print(f"[DEBUG] Статистика для админа: {statistics}")
    return statistics

@bot.message_handler(commands=['statistics'])
def show_statistics(message):
    if message.chat.id not in admins:
        return

    stats = get_admin_statistics()
    if not stats:
        bot.send_message(message.chat.id, "Нет данных о публикациях.")
        return

    response = "Статистика публикаций:\n"
    for user_id, user_stats in stats.items():
        response += (
            f"Пользователь {user_id}:\n"
            f"  - Опубликовано: {user_stats['published']}\n"
            f"  - Осталось: {user_stats['remaining']}\n"
            f"  - Ссылки: {', '.join(user_stats['links'])}\n"
        )
        if user_stats["details"]:
            response += "  - Детали по сетям:\n"
            for network, cities in user_stats["details"].items():
                for city, data in cities.items():
                    response += f"    - {network}, {city}: {data['published']} опубликовано, {data['remaining']} осталось\n"
    bot.send_message(message.chat.id, response)

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
            end_date = entry["end_date"]
            if isinstance(end_date, str):  # Если дата в формате строки
                end_date = datetime.fromisoformat(end_date)
            entry["end_date"] = end_date + timedelta(days=days)

        # Сохраняем изменения
        save_data()

        # Уведомляем администратора
        bot.answer_callback_query(call.id, f"✅ Срок изменён на {days} дней.")
        show_paid_users(call.message)
    except Exception as e:
        print(f"Ошибка в handle_duration_change: {e}")
        bot.answer_callback_query(call.id, " Произошла ошибка при изменении срока.")

# Основная логика публикации объявлений
@bot.message_handler(func=lambda message: message.text == "Создать новое объявление")
def create_new_post(message):
    if message.chat.type != "private":
        bot.send_message(message.chat.id, "Пожалуйста, используйте ЛС для работы с ботом.")
        return

    # Запрашиваем текст объявления
    bot.send_message(message.chat.id, "Напишите текст объявления:")
    bot.register_next_step_handler(message, process_text)

def process_text(message):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Вы вернулись в главное меню.", reply_markup=get_main_keyboard())
        return

    # Обработка текста или медиа
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
        bot.send_message(message.chat.id, " Ошибка! Отправьте текст, фото или видео.")
        bot.register_next_step_handler(message, process_text)
        return

    # Проверка длины текста
    if not validate_text_length(text):
        bot.send_message(message.chat.id, " Ошибка! Текст объявления не должен превышать 1000 символов.")
        bot.register_next_step_handler(message, process_text)
        return

    # Подтверждение текста
    confirm_text(message, text, media_type, file_id)

def select_city_and_publish_webhook(message, state_data):
    text = state_data['text']
    selected_network = state_data['selected_network']
    media_type = state_data['media_type']
    file_id = state_data['file_id']

    city = message.text

    if city == "Назад":
        markup = get_network_markup()
        safe_send_message(message.chat.id, "Выберите сеть для публикации:", reply_markup=markup)
        user_state[message.from_user.id] = {
            'step': 'select_network',
            'data': {
                'text': text,
                'media_type': media_type,
                'file_id': file_id
            }
        }
        return

    if city == "Выбрать другую сеть":
        markup = get_network_markup()
        safe_send_message(message.chat.id, "Выберите сеть для публикации:", reply_markup=markup)
        user_state[message.from_user.id] = {
            'step': 'select_network',
            'data': {
                'text': text,
                'media_type': media_type,
                'file_id': file_id
            }
        }
        return

    if is_user_paid(message.from_user.id, selected_network, city):
        user_name = get_user_name(message.from_user)
        user_id = message.from_user.id

        if selected_network == "Все сети":
            networks = ["Мужской Клуб", "ПАРНИ 18+", "НС"]
        else:
            networks = [selected_network]

        for network in networks:
            if network == "Мужской Клуб":
                chat_dict = chat_ids_mk
            elif network == "ПАРНИ 18+":
                chat_dict = chat_ids_parni
            elif network == "НС":
                chat_dict = chat_ids_ns
            else:
                continue

            city_for_network = ns_city_substitution.get(city, city) if network == "НС" else city

            if city_for_network in chat_dict:
                chat_id = chat_dict[city_for_network]
                if not check_daily_limit(user_id, network, city_for_network):
                    safe_send_message(message.chat.id, f"⛔ Вы превысили лимит публикаций (3 в сутки) для сети «{network}», города {city_for_network}. Попробуйте завтра.")
                    continue

                sent_message = publish_post(chat_id, text, user_name, user_id, media_type, file_id)
                if sent_message:
                    safe_send_message(user_id, f"✅ Ваше объявление опубликовано в сети «{network}», городе {city_for_network}.")
            else:
                safe_send_message(message.chat.id, f"❗ Ошибка: город '{city}' не найден в сети «{network}».")
        ask_for_new_post(message)
    else:
        markup = types.InlineKeyboardMarkup()
        if selected_network == "Мужской Клуб":
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQMKBOT"))
        else:
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQZNAKBOT"))
        safe_send_message(message.chat.id, "⛔ У вас нет прав на публикацию в этой сети/городе. Обратитесь к администратору для оплаты.", reply_markup=markup)

def handle_delete_post(message):
    try:
        user_id = message.chat.id
        if user_id not in user_posts or not user_posts[user_id]:
            bot.send_message(user_id, "У вас нет опубликованных объявлений.")
            return

        for post in list(user_posts[user_id]):
            if f"Удалить объявление в {post['city']} ({post['network']})" == message.text:
                try:
                    # Попытка удалить сообщение
                    try:
                        bot.delete_message(post["chat_id"], post["message_id"])
                    except Exception as e:
                        print(f"[DEBUG] Ошибка при удалении сообщения (возможно, уже удалено): {e}")

                    # Удаляем только из user_posts, но лимиты и статистику НЕ трогаем
                    user_posts[user_id].remove(post)
                    save_data()

                    bot.send_message(user_id, "✅ Объявление успешно удалено.")
                    return
                except Exception as e:
                    bot.send_message(user_id, f"⚠️ Ошибка при удалении объявления: {e}")
                    return

        bot.send_message(user_id, "Объявление не найдено.")
    except Exception as e:
        print(f"[ERROR] Ошибка при обработке удаления: {e}")
        bot.send_message(user_id, f"❌ Ошибка при удалении объявления: {e}")

def select_city_and_publish(message, text, selected_network, media_type, file_id):
    if message.text == "Назад":
        safe_send_message(message.chat.id, "Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
        return

    city = message.text
    if city == "Выбрать другую сеть":
        safe_send_message(message.chat.id, "Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
        return

    user_id = message.from_user.id
    user_name = get_user_name(message.from_user)

    if selected_network == "Все сети":
        networks = ["Мужской Клуб", "ПАРНИ 18+", "НС"]
    else:
        networks = [selected_network]

    any_success = False  # Флаг успешной публикации

    for network in networks:
        if network == "Мужской Клуб":
            chat_dict = chat_ids_mk
        elif network == "ПАРНИ 18+":
            chat_dict = chat_ids_parni
        elif network == "НС":
            chat_dict = chat_ids_ns
        else:
            continue

        target_city = ns_city_substitution[city] if (network == "НС" and city in ns_city_substitution) else city

        if target_city not in chat_dict:
            safe_send_message(message.chat.id, f"❌ Ошибка! Город '{target_city}' не найден в сети «{network}».")
            continue

        if not is_user_paid(user_id, network, city):
            print(f"[DEBUG] Нет доступа: {network} / {city}")
            continue  # Просто пропускаем

        chat_id = chat_dict[target_city]

        sent_message = publish_post(chat_id, text, user_name, user_id, media_type, file_id)
        if sent_message:
            any_success = True
            # Обновление статистики
            if message.chat.id not in user_posts:
                user_posts[message.chat.id] = []
            user_posts[message.chat.id].append({
                "message_id": sent_message.message_id,
                "chat_id": chat_id,
                "time": datetime.now(),
                "city": city,
                "network": network
            })
            update_daily_posts(user_id, network, city)
            if user_id not in user_statistics:
                user_statistics[user_id] = {"count": 0}
            user_statistics[user_id]["count"] += 1
            save_data()

    if any_success:
        safe_send_message(message.chat.id, f"✅ Ваше объявление успешно опубликовано.")
    else:
        # Ни в одной сети не получилось
        markup = types.InlineKeyboardMarkup()
        if selected_network == "Мужской Клуб":
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQMKBOT"))
        else:
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQZNAKBOT"))
        safe_send_message(message.chat.id, "⛔ У вас нет прав на публикацию в выбранной сети/городе.", reply_markup=markup)

    ask_for_new_post(message)

@bot.message_handler(func=lambda message: message.text == "📊 Моя статистика")
def handle_stats_button(message):
    try:
        stats = get_user_statistics(message.from_user.id)
        response = (
            f"📊 Ваша статистика:\n"
            f"• Опубликовано сегодня: {stats['published']}\n"
            f"• Осталось публикаций: {stats['remaining']}\n"
        )
        if stats["details"]:
            response += "\n📍 Детали по сетям:\n"
            for network, cities in stats["details"].items():
                for city, data in cities.items():
                    response += f"  └ {network}, {city}: {data['published']} опубликовано, {data['remaining']} осталось\n"
        bot.send_message(message.chat.id, response)
    except Exception as e:
        print(f"[ERROR] Ошибка при показе статистики: {e}")
        bot.send_message(message.chat.id, "Произошла ошибка при получении статистики.")

# Добавляем маршрут для проверки работоспособности сервиса (если зайти по корневому URL)
@app.route('/')
def index():
    return 'Bot is running!'

# Эндпоинт для вебхука, куда будут приходить обновления от Telegram
@app.route('/webhook', methods=['POST'])
def webhook():
    app.logger.info("Получен запрос на вебхук")
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        app.logger.info(f"Получено обновление: {json_string}")
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return '', 200
    else:
        abort(403)

# Определение функции set_webhook
def set_webhook():
    try:
        bot.remove_webhook()
        result = bot.set_webhook(url=WEBHOOK_URL)
        app.logger.info(f"Вебхук установлен на {WEBHOOK_URL}: {result}")
    except Exception as e:
        app.logger.error(f"Ошибка при установке вебхука: {e}")

# Запуск Flask
if __name__ == '__main__':
    set_webhook()  # Устанавливаем вебхук перед запуском
    app.run(host='0.0.0.0', port=8080)
