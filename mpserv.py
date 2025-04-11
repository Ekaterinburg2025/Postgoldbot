import os
import time
import sqlite3
import threading
from datetime import datetime, timedelta
from collections import defaultdict

import pytz
from pytz import timezone

import telebot
from telebot import types
from telebot.apihelper import ApiTelegramException

from flask import Flask, request, Response

# Устанавливаем часовой пояс для Екатеринбурга
ekaterinburg_tz = timezone('Asia/Yekaterinburg')

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


# Функции для работы с временем с учетом часового пояса
def get_current_time():
    """Возвращает текущее время в часовом поясе Екатеринбурга."""
    return datetime.now(ekaterinburg_tz)


def format_time(dt):
    """Форматирует время для вывода пользователю."""
    return dt.strftime("%d.%m.%Y %H:%M")


# Инициализация базы данных
def init_db():
    with db_lock:
        with sqlite3.connect("bot_data.db") as conn:
            cur = conn.cursor()
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

                    # Приводим end_date к datetime
                    if isinstance(end_date, str):
                        try:
                            parsed_date = datetime.fromisoformat(end_date).astimezone(ekaterinburg_tz)
                        except:
                            parsed_date = None
                    else:
                        parsed_date = None

                    local_paid_users[user_id].append({
                        "network": network,
                        "city": city,
                        "end_date": parsed_date
                    })

                # Загружаем админов
                cur.execute("SELECT user_id FROM admin_users")
                local_admins = [row[0] for row in cur.fetchall()]

                # Загружаем публикации
                cur.execute("SELECT user_id, network, city, time, chat_id, message_id FROM user_posts")
                local_user_posts = {}
                for user_id, network, city, time_str, chat_id, message_id in cur.fetchall():
                    if user_id not in local_user_posts:
                        local_user_posts[user_id] = []
                    try:
                        post_time = datetime.fromisoformat(time_str).astimezone(ekaterinburg_tz)
                    except:
                        post_time = get_current_time()
                    local_user_posts[user_id].append({
                        "message_id": message_id,
                        "chat_id": chat_id,
                        "time": post_time,
                        "city": city,
                        "network": network
                    })

                # Заменяем глобальные переменные
                global paid_users, admins, user_posts
                paid_users = local_paid_users
                admins = local_admins
                user_posts = local_user_posts

                return paid_users, admins, user_posts

        except Exception as e:
            print(f"[ERROR] Ошибка при загрузке данных из базы: {e}")
            return {}, [], {}


# Инициализация базы данных
init_db()
paid_users, admins, user_posts = load_data()

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
    "Новосибирск": -1002261777025,  # Обновленный ID для группы "Парни Новосибирск"
    "ЯМАО": -1002371438340
}

# ДОБАВЛЯЕМ новую сеть НС с нужными группами
chat_ids_ns = {
    "Курган": -1001465465654,
    "Новосибирск": -1001824149334,
    "Челябинск": -1002233108474,
    "Пермь": -1001753881279,
    "Уфа": -1001823390636,
    "Ямал": -1002145851794,
    "Москва": -1001938448310,
    "ХМАО": -1001442597049,
    "Знакомства 66": -1002169473861,   # Привязано к Екатеринбургу
    "Знакомства 74": -1002193127380    # Привязано к Челябинску
}

# Словарь для замены названий городов для сети НС
ns_city_substitution = {
    "Екатеринбург": "Знакомства 66",
    "Челябинск": "Знакомства 74"
}

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
    return dt.strftime("%d.%m %H:%M")

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

def is_today(dt):
    return dt.date() == datetime.now().date()

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

        if end_date and end_date >= datetime.now():
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
    now = datetime.now()
    try:
        parsed_time = datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
        return parsed_time.date() == now.date()
    except:
        return False

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

# Сохранение данных в файл
def save_data(retries=3, delay=0.5):
    """Сохраняет данные в базу данных с повторной попыткой при блокировке."""
    for attempt in range(retries):
        with db_lock:
            try:
                with sqlite3.connect("bot_data.db", timeout=5) as conn:
                    cur = conn.cursor()

                    # Очистка таблиц
                    cur.execute("DELETE FROM paid_users")
                    cur.execute("DELETE FROM admin_users")
                    cur.execute("DELETE FROM user_posts")

                    # Сохранение оплативших пользователей
                    for user_id, entries in paid_users.items():
                        for entry in entries:
                            end_date = entry.get("end_date")
                            if isinstance(end_date, datetime):
                                end_date_str = end_date.isoformat()
                            else:
                                end_date_str = None

                            cur.execute("""
                                INSERT INTO paid_users (user_id, network, city, end_date)
                                VALUES (?, ?, ?, ?)
                            """, (user_id, entry["network"], entry["city"], end_date_str))

                    # Сохранение админов
                    for user_id in admins:
                        cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (user_id,))

                    # Сохранение публикаций
                    for user_id, posts in user_posts.items():
                        for post in posts:
                            time_str = post["time"].isoformat() if isinstance(post["time"], datetime) else None
                            cur.execute("""
                                INSERT INTO user_posts (user_id, network, city, time, chat_id, message_id)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (
                                user_id, post["network"], post["city"],
                                time_str, post["chat_id"], post["message_id"]
                            ))

                    conn.commit()
                    return
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    time.sleep(delay)
                    continue
                else:
                    break
            except Exception:
                break
    # Не удалось сохранить после всех попыток

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

            current_time = datetime.now()

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

            if isinstance(end_date, datetime) and datetime.now() < end_date:
                print(f"[DEBUG] Доступ разрешён: {entry}")
                return True
            else:
                print(f"[DEBUG] Срок доступа истёк: {entry}")

    print(f"[DEBUG] Нет активного доступа к {network}, {city}")
    return False

# Админ-панель
@bot.message_handler(commands=['admin'])
def admin_panel(message):
    if not is_admin(message.chat.id):
        bot.send_message(message.chat.id, " У вас нет прав для выполнения этой команды.")
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Добавить оплатившего", callback_data="admin_add_paid_user"))
    markup.add(types.InlineKeyboardButton("Список оплативших", callback_data="admin_list_paid_users"))
    markup.add(types.InlineKeyboardButton("Изменить срок оплаты", callback_data="admin_change_duration"))
    markup.add(types.InlineKeyboardButton("Добавить администратора", callback_data="admin_add_admin"))
    markup.add(types.InlineKeyboardButton("📊 Статистика публикаций", callback_data="admin_statistics"))
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
            show_statistics_for_admin(call.message.chat.id)
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
            end_date = entry.get("end_date")
            if isinstance(end_date, str):
                try:
                    end_date = datetime.fromisoformat(end_date)
                except:
                    end_date = None

            if isinstance(end_date, datetime):
                date_str = end_date.strftime('%d.%m.%Y %H:%M')
            else:
                date_str = "неизвестно"

            response += f" - Сеть: {entry['network']}, Город: {entry['city']}, Срок: {date_str}\n"

    bot.send_message(message.chat.id, response)

def get_all_cities_for_network(network):
    if network == "Мужской Клуб":
        return list(chat_ids_mk.keys())
    elif network == "ПАРНИ 18+":
        return list(chat_ids_parni.keys())
    elif network == "НС":
        return list(chat_ids_ns.keys())
    elif network == "Все сети":
        return list(set(chat_ids_mk.keys()) | set(chat_ids_parni.keys()) | set(chat_ids_ns.keys()))
    return []

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
        bot.answer_callback_query(call.id, " Произошла ошибка при изменении срока.")

def get_admin_statistics():
    statistics = {}

    for user_id, networks in user_daily_posts.items():
        stats = {"published": 0, "remaining": 0, "details": {}, "links": []}
        limit_total = 0

        for network, cities in networks.items():
            stats["details"][network] = {}
            for city, post_data in cities.items():
                active_posts = len(post_data.get("posts", []))
                deleted_posts = len(post_data.get("deleted_posts", []))
                total_posts = active_posts + deleted_posts

                limit_total += 3

                stats["details"][network][city] = {
                    "published": total_posts,
                    "remaining": max(0, 3 - total_posts)
                }

                stats["published"] += total_posts

                for post in post_data.get("posts", []):
                    if isinstance(post, datetime) and is_today(post):
                        for user_post in user_posts.get(user_id, []):
                            if user_post["network"] == network and user_post["city"] == city:
                                stats.setdefault("links", []).append(
                                    f"https://t.me/c/{str(user_post['chat_id'])[4:]}/{user_post['message_id']}"
                                )

        stats["remaining"] = max(0, limit_total - stats["published"])
        statistics[user_id] = stats

    return statistics

@bot.message_handler(commands=['statistics'])
def show_statistics_for_admin(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "У вас нет прав для просмотра статистики.")
        return

    stats = get_admin_statistics()
    if not stats:
        bot.send_message(chat_id, "Нет данных о публикациях.")
        return

    response = "📊 Статистика публикаций:\n"
    for user_id, user_stats in stats.items():
        user_name = f"ID {user_id}"

        response += (
            f"👤 {user_name}:\n"
            f"• Опубликовано: {user_stats['published']}\n"
            f"• Осталось: {user_stats['remaining']}\n"
        )

        if user_stats["details"]:
            response += "  • Детали:\n"
            for network, cities in user_stats["details"].items():
                for city, data in cities.items():
                    end_date = None
                    for paid in paid_users.get(user_id, []):
                        if (
                            (paid.get("network") == network and paid.get("city") == city) or
                            (paid.get("network") == "Все сети" and paid.get("city") == city)
                        ):
                            end_date = paid.get("end_date")
                            break

                    if isinstance(end_date, str):
                        try:
                            end_date = datetime.fromisoformat(end_date)
                        except:
                            end_date = None

                    expire_str = f"(до {end_date.strftime('%d.%m.%Y')})" if isinstance(end_date, datetime) else "(неизвестно)"
                    response += f"    - {network}, {city} {expire_str}: {data['published']} / {data['remaining']}\n"

        if user_stats["links"]:
            response += "  • Ссылки:\n"
            for link in user_stats["links"]:
                response += f"    - {link}\n"

        response += "\n"

    try:
        bot.send_message(chat_id, response)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при отправке статистики: {e}")

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
                    try:
                        bot.delete_message(post["chat_id"], post["message_id"])
                    except Exception:
                        pass
                    user_posts[message.chat.id].remove(post)
                    bot.send_message(message.chat.id, "✅ Объявление успешно удалено.", reply_markup=get_main_keyboard())
                    return
            bot.send_message(message.chat.id, "❌ Объявление не найдено.")
        except (ValueError, IndexError):
            bot.send_message(message.chat.id, "❌ Ошибка! Пожалуйста, выберите объявление из списка.")

def process_delete_all_choice(message):
    if message.text == "Да, удалить всё":
        for post in user_posts[message.chat.id]:
            try:
                bot.delete_message(post["chat_id"], post["message_id"])
            except Exception:
                pass
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

def select_network(message, text, media_type, file_id):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Напишите текст объявления:")
        bot.register_next_step_handler(message, process_text)
        return

    selected_network = message.text
    if selected_network in ["Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети"]:
        markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True, row_width=2)
        if selected_network == "Мужской Клуб":
            cities = list(chat_ids_mk.keys())
        elif selected_network == "ПАРНИ 18+":
            cities = list(chat_ids_parni.keys())
        elif selected_network == "НС":
            cities = list(chat_ids_ns.keys())
        elif selected_network == "Все сети":
            cities = list(set(list(chat_ids_mk.keys()) + list(chat_ids_parni.keys()) + list(chat_ids_ns.keys())))
        for city in cities:
            markup.add(city)
        markup.add("Выбрать другую сеть", "Назад")
        bot.send_message(message.chat.id, "📍 Выберите город для публикации или нажмите 'Выбрать другую сеть':", reply_markup=markup)
        bot.register_next_step_handler(message, select_city_and_publish, text, selected_network, media_type, file_id)
    else:
        bot.send_message(message.chat.id, "❌ Ошибка! Выберите правильную сеть.")
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
    user_name = get_user_name(message.from_user)

    # Проверка доступа
    if not is_user_paid(user_id, selected_network, city):
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQMKBOT" if selected_network == "Мужской Клуб" else "https://t.me/FAQZNAKBOT"))
        bot.send_message(message.chat.id, "⛔ У вас нет прав на публикацию в этой сети/городе.", reply_markup=markup)
        return

    networks = ["Мужской Клуб", "ПАРНИ 18+", "НС"] if selected_network == "Все сети" else [selected_network]

    for network in networks:
        # Пропускаем, если нет оплаты по конкретной сети
        if not is_user_paid(user_id, network, city):
            continue

        # Проверка лимита по конкретной сети и городу
        user_stats = get_user_statistics(user_id)
        city_stats = user_stats.get("details", {}).get(network, {}).get(city, {})
        if city_stats.get("remaining", 0) <= 0:
            bot.send_message(message.chat.id, f"⛔ Лимит публикаций исчерпан для {network}, город {city}")
            continue

        # Подпись для сети
        signature = network_signatures.get(network, "")
        full_text = f"📢 Объявление от {user_name}:\n\n{text}\n\n{signature}"

        if network == "Мужской Клуб":
            chat_dict = chat_ids_mk
        elif network == "ПАРНИ 18+":
            chat_dict = chat_ids_parni
        elif network == "НС":
            chat_dict = chat_ids_ns
        else:
            continue

        # Обработка города
        if network == "НС" and city not in chat_dict and city in ns_city_substitution:
            substitute_city = ns_city_substitution[city]
            chat_id = chat_dict.get(substitute_city)
        else:
            chat_id = chat_dict.get(city)

        if not chat_id:
            bot.send_message(message.chat.id, f"❌ Город '{city}' не найден в сети «{network}».")
            continue

        try:
            if media_type == "photo":
                sent_message = bot.send_photo(chat_id, file_id, caption=full_text, parse_mode="Markdown")
            elif media_type == "video":
                sent_message = bot.send_video(chat_id, file_id, caption=full_text, parse_mode="Markdown")
            else:
                sent_message = bot.send_message(chat_id, full_text, parse_mode="Markdown")

            # user_posts
            if user_id not in user_posts:
                user_posts[user_id] = []
            user_posts[user_id].append({
                "message_id": sent_message.message_id,
                "chat_id": chat_id,
                "time": datetime.now(),
                "city": city,
                "network": network
            })

            # user_daily_posts
            if user_id not in user_daily_posts:
                user_daily_posts[user_id] = {}
            if network not in user_daily_posts[user_id]:
                user_daily_posts[user_id][network] = {}
            if city not in user_daily_posts[user_id][network]:
                user_daily_posts[user_id][network][city] = {"posts": [], "deleted_posts": []}

            user_daily_posts[user_id][network][city]["posts"].append(datetime.now())

            bot.send_message(message.chat.id, f"✅ Объявление опубликовано в сети «{network}», городе {city}.")

        except telebot.apihelper.ApiTelegramException as e:
            bot.send_message(message.chat.id, f"❌ Ошибка: {e.description}")

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
                    end_date = None
                    for paid in paid_users.get(message.from_user.id, []):
                        if (
                            (paid["network"] == network and paid["city"] == city) or
                            (paid["network"] == "Все сети" and paid["city"] == city)
                        ):
                            end_date = paid.get("end_date")
                            break

                    if isinstance(end_date, str):
                        try:
                            end_date = datetime.fromisoformat(end_date)
                        except:
                            end_date = None

                    expire_str = f"(до {end_date.strftime('%d.%m.%Y')})" if isinstance(end_date, datetime) else "(неизвестно)"
                    response += (
                        f"  └ {network}, {city} {expire_str}: "
                        f"{data['published']} опубликовано, {data['remaining']} осталось\n"
                    )

        bot.send_message(message.chat.id, response)

    except Exception as e:
        bot.send_message(message.chat.id, f"Произошла ошибка при получении статистики: {e}")

def show_statistics_for_admin(chat_id):
    if not is_admin(chat_id):
        bot.send_message(chat_id, "У вас нет прав для просмотра статистики.")
        return

    stats = get_admin_statistics()
    if not stats:
        bot.send_message(chat_id, "Нет данных о публикациях.")
        return

    response = "📊 Статистика публикаций:\n"
    for user_id, user_stats in stats.items():
        user_name = f"ID {user_id}"

        response += (
            f"👤 {user_name}:\n"
            f"• Опубликовано: {user_stats['published']}\n"
            f"• Осталось: {user_stats['remaining']}\n"
        )

        if user_stats["details"]:
            response += "  • Детали:\n"
            for network, cities in user_stats["details"].items():
                for city, data in cities.items():
                    end_date = None
                    for paid in paid_users.get(user_id, []):
                        if (
                            (paid.get("network") == network and paid.get("city") == city) or
                            (paid.get("network") == "Все сети" and paid.get("city") == city)
                        ):
                            end_date = paid.get("end_date")
                            break

                    if isinstance(end_date, str):
                        try:
                            end_date = datetime.fromisoformat(end_date)
                        except:
                            end_date = None

                    expire_str = f"(до {end_date.strftime('%d.%m.%Y')})" if end_date else "(неизвестно)"
                    response += f"    - {network}, {city} {expire_str}: {data['published']} / {data['remaining']}\n"

        if user_stats["links"]:
            response += "  • Ссылки:\n"
            for link in user_stats["links"]:
                response += f"    - {link}\n"

        response += "\n"

    try:
        bot.send_message(chat_id, response)
    except Exception as e:
        bot.send_message(chat_id, f"❌ Ошибка при отправке статистики: {e}")

@app.route('/webhook', methods=['POST'])
def webhook():
    update = telebot.types.Update.de_json(request.stream.read().decode('utf-8'))
    bot.process_new_updates([update])
    return 'ok', 200

@app.route('/')
def index():
    return '✅ Бот запущен и работает!'

if __name__ == '__main__':
    add_admin_user(479938867)  # Только один раз!
    add_admin_user(7235010425)  # Только один раз!
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
