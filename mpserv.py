import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
import pytz
import telebot
from telebot import types
from flask import Flask, request

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

# Админ ID (ваш ID)
ADMIN_CHAT_ID = 479938867  # Замените на ваш ID

# Глобальные переменные
paid_users = {}
user_posts = {}
user_daily_posts = {}
user_statistics = {}
admins = [ADMIN_CHAT_ID]

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
    "Мужской Клуб": "️ 🕸️Реклама. Согласовано с администрация сети МК.🕸️",
    "ПАРНИ 18+": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫",
    "НС": "🟥🟦🟩🟨🟧🟪⬛️⬜️🟫"
}

# Словарь для хранения статистики публикаций
user_statistics = {}

# Список администраторов бота
admins = [ADMIN_CHAT_ID]

# Функция для сохранения данных в SQLite
def save_data():
    conn = sqlite3.connect("bot_data.db")
    cur = conn.cursor()
    data = {
        "paid_users": paid_users,
        "user_posts": user_posts,
        "user_daily_posts": user_daily_posts,
        "user_statistics": user_statistics,
        "admins": admins
    }
    cur.execute("INSERT OR REPLACE INTO bot_data (id, data) VALUES (1, ?)", (json.dumps(data),))
    conn.commit()
    cur.close()
    conn.close()

# Функция для загрузки данных из SQLite
def load_data():
    conn = sqlite3.connect("bot_data.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS bot_data (id INTEGER PRIMARY KEY, data TEXT)")
    cur.execute("SELECT data FROM bot_data WHERE id = 1")
    result = cur.fetchone()
    if result:
        data = json.loads(result[0])
        global paid_users, user_posts, user_daily_posts, user_statistics, admins
        paid_users = data.get("paid_users", {})
        user_posts = data.get("user_posts", {})
        user_daily_posts = data.get("user_daily_posts", {})
        user_statistics = data.get("user_statistics", {})
        admins = data.get("admins", [ADMIN_CHAT_ID])
    cur.close()
    conn.close()

# Загружаем данные при запуске
load_data()

# Вебхук endpoint
@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return ''
    else:
        abort(403)

# Установка вебхука
def set_webhook():
    webhook_url = "https://postgoldbot.onrender.com/webhook"  # Ваш URL на Render
    bot.remove_webhook()
    bot.set_webhook(url=webhook_url)
    logging.info(f"Вебхук установлен на {webhook_url}")

# Запуск Flask
@app.route('/')
def index():
    return 'Bot is running!'

if __name__ == '__main__':
    set_webhook()
    app.run(host='0.0.0.0', port=8080)

    # Запускаем Flask
    run_flask()

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
        bot.send_message(message.chat.id, " Ошибка! Выберите правильный срок.")
        bot.register_next_step_handler(message, lambda m: select_duration_for_payment(m, user_id, network, city))
        return

    expiry_date = datetime.now() + timedelta(days=days)

    if user_id not in paid_users:
        paid_users[user_id] = []  # Инициализируем список, если он отсутствует

    # Добавляем данные с ключом 'end_date'
    paid_users[user_id].append({
        "end_date": expiry_date.isoformat(),  # Используем isoformat для сериализации
        "network": network,
        "city": city
    })
    save_data()  # Сохраняем данные
    bot.send_message(message.chat.id, f" ✅ Пользователь {user_id} добавлен в сеть «{network}», город {city} на {days} дней. Срок действия: {expiry_date.strftime('%Y-%m-%d')}.")

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

    current_time = datetime.now()
    return current_time.date() > last_post_time.date()

def get_user_statistics(user_id):
    """Возвращает статистику публикаций для пользователя."""
    stats = {
        "published": 0,
        "remaining": 3,
        "details": {}
    }

    if user_id in user_daily_posts:
        for network in user_daily_posts[user_id]:
            stats["details"][network] = {}
            for city in user_daily_posts[user_id][network]:
                posts_today = len([
                    post_time for post_time in user_daily_posts[user_id][network][city]["posts"]
                    if is_today(post_time)
                ])
                stats["details"][network][city] = {
                    "published": posts_today,
                    "remaining": max(0, 3 - posts_today)
                }
                stats["published"] += posts_today

        # Общий лимит для режима "Все сети"
        if "Все сети" in stats["details"]:
            total_published = sum(
                details["published"]
                for network in stats["details"]
                for city in stats["details"][network]
            )
            stats["remaining"] = max(0, 9 - total_published)
        else:
            stats["remaining"] = max(0, 3 - stats["published"])

    return stats

def is_today(timestamp):
    """Проверяет, что временная метка относится к текущему дню."""
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    return timestamp.date() == datetime.now().date()

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
def save_data():
    conn = sqlite3.connect("bot_data.db")
    cur = conn.cursor()
    # Преобразуем datetime в строки
    data = {
        "paid_users": {
            user_id: [
                {
                    "end_date": entry["end_date"],  # Убедитесь, что 'end_date' существует
                    "network": entry["network"],    # Ключ 'network'
                    "city": entry["city"]           # Ключ 'city'
                }
                for entry in entries
            ]
            for user_id, entries in paid_users.items()
        },
        "user_posts": user_posts,
        "user_daily_posts": user_daily_posts,
        "user_statistics": user_statistics,
        "admins": admins
    }
    cur.execute(
        "INSERT OR REPLACE INTO bot_data (id, data) VALUES (1, ?)",
        (json.dumps(data, default=str),)  # Используем default=str для сериализации datetime
    )
    conn.commit()
    cur.close()
    conn.close()

def load_data():
    conn = sqlite3.connect("bot_data.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS bot_data (id INTEGER PRIMARY KEY, data TEXT)")
    cur.execute("SELECT data FROM bot_data WHERE id = 1")
    result = cur.fetchone()
    if result:
        data = json.loads(result[0])
        global paid_users, user_posts, user_daily_posts, user_statistics, admins
        paid_users = {
            user_id: [
                {
                    "end_date": datetime.fromisoformat(entry["end_date"]) if isinstance(entry["end_date"], str) else entry["end_date"],
                    "network": entry["network"],
                    "city": entry["city"]
                }
                for entry in entries
            ]
            for user_id, entries in data.get("paid_users", {}).items()
        }
        user_posts = data.get("user_posts", {})
        user_daily_posts = data.get("user_daily_posts", {})
        user_statistics = data.get("user_statistics", {})
        admins = data.get("admins", [ADMIN_CHAT_ID])
        print("Данные загружены:", data)  # Логирование
    cur.close()
    conn.close()

# Клавиатура выбора сети
def get_network_markup():
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Мужской Клуб", "ПАРНИ 18+", "НС", "Все сети", "Назад")
    return markup

# Основная клавиатура
def get_main_keyboard():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.add("Создать новое объявление", "Удалить объявление", "Удалить все объявления")
    return markup

# Форматирование времени
def format_time(timestamp):
    tz = pytz.timezone('Asia/Yekaterinburg')
    local_time = timestamp.astimezone(tz)
    return local_time.strftime("%H:%M, %d %B %Y")

def is_new_day(last_post_time):
    """Проверяет, наступил ли новый день."""
    if last_post_time is None:
        return True  # Если last_post_time отсутствует, считаем, что новый день
    current_time = datetime.now()
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

# Ограничение публикаций (3 в сутки)
def check_daily_limit(user_id, network, city):
    """Проверяет лимит публикаций для пользователя."""
    if user_id not in user_daily_posts:
        user_daily_posts[user_id] = {}

    if network not in user_daily_posts[user_id]:
        user_daily_posts[user_id][network] = {}

    if city not in user_daily_posts[user_id][network]:
        user_daily_posts[user_id][network][city] = {"posts": [], "last_post_time": None}

    # Проверяем, наступил ли новый день
    if is_new_day(user_daily_posts[user_id][network][city]["last_post_time"]):
        user_daily_posts[user_id][network][city]["posts"] = []
        print(f"[DEBUG] Новый день для пользователя {user_id} в сети {network}, городе {city}.")

    # Проверяем лимит публикаций
    if network == "Все сети":
        # Общий лимит для всех сетей (9 публикаций)
        total_posts = 0
        for net in ["Мужской Клуб", "ПАРНИ 18+", "НС"]:
            if net in user_daily_posts[user_id] and city in user_daily_posts[user_id][net]:
                total_posts += len(user_daily_posts[user_id][net][city]["posts"])
        return total_posts < 9
    else:
        # Лимит для конкретной сети (3 публикации)
        return len(user_daily_posts[user_id][network][city]["posts"]) < 3

def update_daily_posts(user_id, network, city, remove=False):
    """Обновляет данные о публикациях пользователя."""
    if user_id not in user_daily_posts:
        user_daily_posts[user_id] = {}

    if network not in user_daily_posts[user_id]:
        user_daily_posts[user_id][network] = {}

    if city not in user_daily_posts[user_id][network]:
        user_daily_posts[user_id][network][city] = {"posts": [], "last_post_time": None}

    if remove:
        # Удаляем последнюю публикацию
        if user_daily_posts[user_id][network][city]["posts"]:
            user_daily_posts[user_id][network][city]["posts"].pop()
    else:
        # Добавляем временную метку публикации
        post_time = datetime.now()
        user_daily_posts[user_id][network][city]["posts"].append(post_time.isoformat())
        user_daily_posts[user_id][network][city]["last_post_time"] = post_time

    # Логирование
    print(f"[DEBUG] Обновление данных о публикациях для пользователя {user_id} в сети {network}, городе {city}.")

    # Сохраняем данные
    save_data()

    # Обновляем общий счётчик публикаций
    if user_id not in user_statistics:
        user_statistics[user_id] = {"count": 0}
    user_statistics[user_id]["count"] += 1

    # Сохраняем данные
    save_data()
    print(f"[DEBUG] Данные сохранены для пользователя {user_id}.")

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
    if isinstance(user_id, str):  # Если ID передаётся как строка
        user_id = int(user_id)

    print(f"Проверяем оплату для пользователя {user_id}, сеть {network}, город {city}")  # Логирование
    if user_id in paid_users:
        print(f"Пользователь {user_id} найден в оплативших: {paid_users[user_id]}")  # Логирование
        for entry in paid_users[user_id]:
            if entry["network"] == network and entry["city"] == city:
                end_date = entry["end_date"]  # Используем ключ 'end_date'
                print(f"Срок оплаты: {end_date}, тип: {type(end_date)}")  # Логирование
                if isinstance(end_date, str):  # Если дата в формате строки
                    end_date = datetime.fromisoformat(end_date)
                elif isinstance(end_date, datetime):  # Если это уже объект datetime
                    pass  # Ничего не делаем
                else:
                    print(f"Некорректный тип end_date: {type(end_date)}")
                    return False
                if datetime.now() < end_date:
                    print("Срок оплаты актуален.")  # Логирование
                    return True
    print("Пользователь не оплачен или срок истёк.")  # Логирование
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
        bot.send_message(message.chat.id, " Ошибка! Выберите правильный срок.")
        bot.register_next_step_handler(message, lambda m: select_duration_for_payment(m, user_id, network, city))
        return

    expiry_date = datetime.now() + timedelta(days=days)

    if user_id not in paid_users:
        paid_users[user_id] = []  # Инициализируем список, если он отсутствует

    # Добавляем данные с ключом 'end_date'
    paid_users[user_id].append({
        "end_date": expiry_date.isoformat(),  # Используем isoformat для сериализации
        "network": network,
        "city": city
    })
    save_data()  # Сохраняем данные
    bot.send_message(message.chat.id, f" ✅ Пользователь {user_id} добавлен в сеть «{network}», город {city} на {days} дней. Срок действия: {expiry_date.strftime('%Y-%m-%d')}.")

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
    for user_id, posts in user_posts.items():
        # Получаем количество публикаций за сегодня
        published_today = 0
        links = []
        details = {}

        for post in posts:
            if is_today(post["time"]):
                published_today += 1
                links.append(f"https://t.me/c/{str(post['chat_id'])[4:]}/{post['message_id']}")

                # Детализация по сетям и городам
                network = post["network"]
                city = post["city"]
                if network not in details:
                    details[network] = {}
                if city not in details[network]:
                    details[network][city] = {"published": 0, "remaining": 3}
                details[network][city]["published"] += 1

        # Общий лимит для режима "Все сети"
        if "Все сети" in details:
            total_published = sum(
                data["published"]
                for network in details
                for city in details[network]
            )
            remaining = max(0, 9 - total_published)
        else:
            remaining = max(0, 3 - published_today)

        # Добавляем данные в статистику
        statistics[user_id] = {
            "published": published_today,
            "remaining": remaining,
            "links": links,
            "details": details
        }
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

def confirm_text(message, text, media_type=None, file_id=None):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Да", "Нет")
    bot.send_message(message.chat.id, f"Ваш текст:\n{text}\n\nВсё верно?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_confirmation, text, media_type, file_id)

def handle_confirmation(message, text, media_type, file_id):
    if message.text.lower() == "да":
        bot.send_message(message.chat.id, " Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
    elif message.text.lower() == "нет":
        bot.send_message(message.chat.id, "Хорошо, напишите текст объявления заново:")
        bot.register_next_step_handler(message, process_text)
    else:
        bot.send_message(message.chat.id, " Неверный ответ. Выберите 'Да' или 'Нет'.")
        bot.register_next_step_handler(message, handle_confirmation, text, media_type, file_id)

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
        bot.send_message(message.chat.id, " Ошибка! Выберите правильную сеть.")
        bot.register_next_step_handler(message, process_text)

def select_city_and_publish(message, text, selected_network, media_type, file_id):
    if message.text == "Назад":
        bot.send_message(message.chat.id, " Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
        return

    city = message.text
    if city == "Выбрать другую сеть":
        bot.send_message(message.chat.id, " Выберите сеть для публикации:", reply_markup=get_network_markup())
        bot.register_next_step_handler(message, select_network, text, media_type, file_id)
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
                continue  # Если сеть не найдена, пропускаем

            if network == "НС" and city in ns_city_substitution:
                city = ns_city_substitution[city]

            if city in chat_dict:
                chat_id = chat_dict[city]
                if not check_daily_limit(user_id, network, city):
                    bot.send_message(message.chat.id, f" Вы превысили лимит публикаций (3 в сутки) для сети «{network}», города {city}. Попробуйте завтра.")
                    continue

                sent_message = publish_post(chat_id, text, user_name, user_id, media_type, file_id)
                if sent_message:
                    if message.chat.id not in user_posts:
                        user_posts[message.chat.id] = []
                    user_posts[message.chat.id].append({
                        "message_id": sent_message.message_id,
                        "chat_id": chat_id,
                        "time": datetime.now(),
                        "city": city,
                        "network": network
                    })
                    # Обновляем количество публикаций
                    update_daily_posts(user_id, network, city)
                    # Обновляем статистику
                    if user_id not in user_statistics:
                        user_statistics[user_id] = {"count": 0}
                    user_statistics[user_id]["count"] += 1
                    save_data()
                    bot.send_message(message.chat.id, f"✅ Ваше объявление опубликовано в сети «{network}», городе {city}.")
            else:
                bot.send_message(message.chat.id, f" Ошибка! Город '{city}' не найден в сети «{network}».")
        ask_for_new_post(message)
    else:
        markup = types.InlineKeyboardMarkup()
        if selected_network == "Мужской Клуб":
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQMKBOT"))
        else:
            markup.add(types.InlineKeyboardButton("Купить рекламу", url="https://t.me/FAQZNAKBOT"))
        bot.send_message(message.chat.id, " У вас нет прав на публикацию в этой сети/городе. Обратитесь к администратору для оплаты.", reply_markup=markup)

# Запрос на новое объявление
def ask_for_new_post(message):
    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    markup.add("Да", "Нет")
    bot.send_message(message.chat.id, "Хотите опубликовать ещё одно объявление?", reply_markup=markup)
    bot.register_next_step_handler(message, handle_new_post_choice)

# Обработка выбора нового объявления
def handle_new_post_choice(message):
    if message.text.lower() == "да":
        bot.send_message(message.chat.id, "Напишите текст объявления:")
        bot.register_next_step_handler(message, process_text)
    else:
        bot.send_message(
            message.chat.id,
            "Спасибо за использование бота! \nЕсли хотите создать новое объявление, нажмите кнопку ниже.",
            reply_markup=get_main_keyboard()
        )

# Удаление одного объявления
@bot.message_handler(func=lambda message: message.text == "Удалить объявление")
def delete_post(message):
    if message.chat.id not in user_posts or not user_posts[message.chat.id]:
        bot.send_message(message.chat.id, "У вас нет опубликованных объявлений.")
        return

    markup = types.ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    for post in user_posts[message.chat.id]:
        markup.add(f"Удалить объявление в {post['city']} ({post['network']})")
    markup.add("Назад")
    bot.send_message(message.chat.id, "Выберите объявление для удаления:", reply_markup=markup)
    bot.register_next_step_handler(message, handle_delete_post)

def handle_delete_post(message):
    if message.text == "Назад":
        bot.send_message(message.chat.id, "Вы вернулись в главное меню.", reply_markup=get_main_keyboard())
        return

    # Проверяем, есть ли у пользователя опубликованные сообщения
    if message.chat.id not in user_posts or not user_posts[message.chat.id]:
        bot.send_message(message.chat.id, "У вас нет опубликованных объявлений.")
        return

    # Ищем выбранное сообщение для удаления
    for post in user_posts[message.chat.id]:
        if f"Удалить объявление в {post['city']} ({post['network']})" == message.text:
            try:
                # Удаляем сообщение из группы
                bot.delete_message(post["chat_id"], post["message_id"])
                # Удаляем запись о сообщении из user_posts
                user_posts[message.chat.id].remove(post)
                # Обновляем данные о публикациях
                update_daily_posts(message.chat.id, post["network"], post["city"])
                # Сохраняем изменения
                save_data()
                bot.send_message(message.chat.id, "✅ Объявление успешно удалено.")
                return
            except Exception as e:
                bot.send_message(message.chat.id, f" Ошибка при удалении объявления: {e}")
                return

    # Если сообщение не найдено
    bot.send_message(message.chat.id, " Объявление не найдено.")

@bot.message_handler(func=lambda message: message.text == "Удалить все объявления")
def delete_all_posts(message):
    user_id = message.chat.id

    # Проверяем, есть ли у пользователя опубликованные сообщения
    if user_id not in user_posts or not user_posts[user_id]:
        bot.send_message(user_id, "У вас нет опубликованных объявлений.")
        return

    # Удаляем все сообщения из групп
    for post in user_posts[user_id]:
        try:
            bot.delete_message(post["chat_id"], post["message_id"])
        except Exception as e:
            bot.send_message(user_id, f" Ошибка при удалении объявления: {e}")

    # Очищаем список объявлений пользователя
    user_posts[user_id] = []

    # Сохраняем изменения
    save_data()

    bot.send_message(user_id, "✅ Все объявления успешно удалены.")

# Функция для публикации объявления
def publish_post(chat_id, text, user_name, user_id, media_type=None, file_id=None):
    try:
        # Определяем сеть и город на основе chat_id
        network = None
        city = None

        if chat_id in chat_ids_mk.values():
            network = "Мужской Клуб"
            for city_name, city_chat_id in chat_ids_mk.items():
                if city_chat_id == chat_id:
                    city = city_name
                    break
        elif chat_id in chat_ids_parni.values():
            network = "ПАРНИ 18+"
            for city_name, city_chat_id in chat_ids_parni.items():
                if city_chat_id == chat_id:
                    city = city_name
                    break
        elif chat_id in chat_ids_ns.values():
            network = "НС"
            for city_name, city_chat_id in chat_ids_ns.items():
                if city_chat_id == chat_id:
                    city = city_name
                    break

        # Проверка лимита публикаций
        if not check_daily_limit(user_id, network, city):
            bot.send_message(user_id, f" Вы превысили лимит публикаций (3 в сутки) для сети «{network}», города {city}. Попробуйте завтра.")
            return None  # Завершаем процесс

        # Формируем текст объявления
        signature = network_signatures.get(network, "")
        full_text = f" Объявление от {user_name}:\n\n{text}\n\n{signature}"

        # Создаём клавиатуру с кнопкой "Написать"
        markup = types.InlineKeyboardMarkup()
        if not user_name.startswith("@"):  # Если нет username, добавляем кнопку "Написать"
            markup.add(types.InlineKeyboardButton("Написать", url=f"https://t.me/user?id={user_id}"))

        # Публикация объявления
        if media_type == "photo":
            sent_message = bot.send_photo(chat_id, file_id, caption=full_text, reply_markup=markup)
        elif media_type == "video":
            sent_message = bot.send_video(chat_id, file_id, caption=full_text, reply_markup=markup)
        else:
            sent_message = bot.send_message(chat_id, full_text, reply_markup=markup)

        # Обновляем данные о публикациях
        update_daily_posts(user_id, network, city)
        save_data()

        return sent_message
    except Exception as e:
        print(f"Ошибка при публикации объявления: {e}")
        return None

# Функция для проверки подключения к Telegram API
def check_telegram_connection():
    try:
        response = requests.get("https://api.telegram.org")
        if response.status_code == 200:
            return True
        else:
            return False
    except Exception as e:
        print(f"Ошибка при проверке подключения к Telegram API: {e}")
        return False

# Запуск Flask в отдельном потоке
def run_flask():
    try:
        app.run(host='0.0.0.0', port=8080)
    except Exception as e:
        print(f"Ошибка в Flask: {e}")
        traceback.print_exc()

# Запуск бота в основном потоке
def run_bot():
    print("Бот запущен...")
    while True:
        try:
            bot.polling(none_stop=True, timeout=60)
        except Exception as e:
            print(f"Ошибка в bot.polling: {e}")
            traceback.print_exc()  # Вывод полного стека ошибки
            print("Перезапуск бота через 10 секунд...")
            time.sleep(10)

if __name__ == '__main__':
    if check_telegram_connection():
        # Запуск Flask в отдельном потоке
        flask_thread = threading.Thread(target=run_flask)
        flask_thread.start()

        # Запуск бота в основном потоке
        run_bot()
    else:
        print("Нет подключения к Telegram API. Проверьте интернет-соединение.")