import os
import json
from typing import Dict, List
import uuid
import logging
from io import BytesIO
from urllib.parse import urlparse

import qrcode
import requests

from telegram import (
    Update,
    ReplyKeyboardMarkup,
    InputMediaPhoto,
    InlineKeyboardButton,
    InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    ConversationHandler,
    CallbackQueryHandler,
    filters
)

from config import load_app_config, ServerConnectionConfig

from x3uiClient import X3UIClient

# --- Configure logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- States for ConversationHandler ---
SECRET, ADD_USERNAME, ADD_CONTACT = range(3)

# --- Global storage ---
AUTH_DB = 'data/auth_users.json'
USERS_DB = 'data/users.json'

# --- Load servers & sessions on startup ---
CONFIG_PATH = os.getenv('CONFIG_PATH', 'data/config.yaml')


def get_servers():
    config = load_app_config(CONFIG_PATH)
    return config.servers


def get_sessions() -> Dict[str, X3UIClient]:
    servers = get_servers()

    sessions: Dict[str, X3UIClient] = {}
    for srv in servers:
        try:
            session = X3UIClient(srv)
            sessions[srv.name] = session
            logger.info(f"Logged in to {srv.name}")
        except Exception as e:
            logger.error(f"Failed to login to {srv.name}: {e}")

    return sessions


# --- Helpers ---

def load_authenticated_users():
    if not os.path.exists(AUTH_DB):
        return {"secret": "", "users": []}
    with open(AUTH_DB, 'r') as f:
        return json.load(f)


def save_authenticated_users(data):
    with open(AUTH_DB, 'w') as f:
        json.dump(data, f, indent=2)


def save_users(db_path, users):
    with open(db_path, 'w') as f:
        json.dump(users, f, indent=2)


def load_users(db_path):
    if not os.path.exists(db_path):
        return []
    with open(db_path, 'r') as f:
        return json.load(f)


def build_vless_url(session: X3UIClient, client_id, sub_id):
    """
    Fetch inbound details and construct a VLESS URL.
    Для ожидаемого URL:
      • Берём host из base_url без порта.
      • Используем порт из inbound (по умолчанию 443).
      • Устанавливаем 'sid' в пустую строку.
      • Используем custom tag из конфигурации.
    """
    server_config = session.config
    inbound = server_config.inbound_id

    # Get inbound details from the server
    obj = session.get_inbound(inbound)

    # Assume the inbound object содержит поле 'port' (по умолчанию 443)
    port = obj.get('port', 443)

    # Извлекаем hostname из base_url
    parsed = urlparse(server_config.base_url)
    host = parsed.hostname

    # Находим email клиента по его client_id
    clients = json.loads(obj["settings"])["clients"]
    for client in clients:
        if client['id'] == client_id:
            break
    email = client.get('email', client_id)

    # Парсим streamSettings
    ss = json.loads(obj['streamSettings'])
    rs = ss.get('realitySettings', {})
    settings = rs.get('settings', {})

    # Параметры URL
    params = {
        'type': ss.get('network', 'tcp'),
        'security': ss.get('security', 'none'),
        'pbk': settings.get('publicKey', ''),
        'fp': settings.get('fingerprint', ''),
        'sni': rs.get('serverNames', [''])[0],
        'sid': '',  # deliberately empty
        'spx': obj.get('spiderX', '/')
    }
    qs = '&'.join(
        f"{k}={requests.utils.quote(str(v), safe='')}"
        for k, v in params.items()
    )

    # Используем remark или имя сервера в качестве тега
    tag = f"{obj.get('remark', server_config.name)}-{email}"
    url = f"vless://{client_id}@{host}:{port}?{qs}#{tag}"
    return url


def generate_qr(url):
    img = qrcode.make(url)
    bio = BytesIO()
    bio.name = 'qr.png'
    img.save(bio)
    bio.seek(0)
    return bio


# Загружаем сохранённые данные аутентификации
auth_data = load_authenticated_users()
authenticated_users = set(auth_data.get("users", []))
stored_secret = auth_data.get("secret", "")


# --- Функции-обработчики ---

# Создаём основное меню (ReplyKeyboardMarkup), чтобы пользователю не пришлось помнить текстовые команды
MAIN_MENU = ReplyKeyboardMarkup([
    ["➕ Добавить пользователя", "👥 Список пользователей"],
    ["🔄 Синхронизировать клиентов"]
], resize_keyboard=True, one_time_keyboard=False)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Начало диалога. Спрашиваем секрет, чтобы пользователь ввёл его.
    """
    await update.message.reply_text(
        "Добро пожаловать! Пожалуйста, введите секрет, чтобы продолжить:"
    )
    return SECRET


async def secret_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатываем введённый секрет. Если верно, аутентифицируем и показываем главное меню.
    """
    global stored_secret, authenticated_users

    config = load_app_config(CONFIG_PATH)

    # Если секрет в конфиге изменился, сбрасываем параллельно список ранее аутентифицированных
    if config.bot.secret != stored_secret:
        authenticated_users.clear()
        stored_secret = config.bot.secret

    text = update.message.text.strip()
    user_id = update.effective_user.id
    if text == config.bot.secret:
        authenticated_users.add(user_id)

        # Сохраняем обновлённый список аутентифицированных
        save_authenticated_users({
            "secret": stored_secret,
            "users": list(authenticated_users)
        })

        # Показываем главное меню
        await update.message.reply_text(
            "✅ Аутентификация успешна! Выберите действие из меню ниже:",
            reply_markup=MAIN_MENU
        )
        return ConversationHandler.END
    else:
        await update.message.reply_text(
            "❌ Неверный секрет. Попробуйте ещё раз:"
        )
        return SECRET


async def add_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Шаг 1: начинаем диалог добавления пользователя, спрашиваем username.
    """
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text(
            "Пожалуйста, /start и введите секрет, чтобы продолжить."
        )
        return ConversationHandler.END

    # Убираем ReplyKeyboard (необязательно), чтобы дальше можно было вводить свободный текст
    await update.message.reply_text(
        "Введите системное имя (username) для нового клиента:",
        reply_markup=ReplyKeyboardMarkup([], resize_keyboard=True)
    )
    return ADD_USERNAME


async def add_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Шаг 2: сохраняем введённый username, просим переслать контакт
    """
    context.user_data['new_user'] = {
        'username': update.message.text.strip()
    }
    # Теперь просим админа переслать контакт целевого пользователя (или ввести текст),
    # а также даём подсказку, что можно отменить ввод.
    await update.message.reply_text(
        "Пожалуйста, **перешлите контакт** пользователя (или напрямую введите его Telegram ID/username, например «@username» или «id:7370682957»).\n"
        "Чтобы отменить операцию и вернуться в главное меню, отправьте «Отмена» или «/cancel».",
        reply_markup=ReplyKeyboardMarkup([], resize_keyboard=True),
        parse_mode="Markdown"
    )
    return ADD_CONTACT


async def add_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Шаг 3: получаем пересланный контакт (или вводимый текст), формируем запись клиента,
    рассылаем QR и URL по всем серверам.

    Также обрабатываем команду отмены (текст «Отмена» или «/cancel»).
    """
    text = update.message.text.strip() if update.message.text else ""

    # Если админ ввёл «Отмена» или «/cancel» — завершаем диалог и возвращаем главное меню
    if text.lower() in ('отмена', '/cancel'):
        await update.message.reply_text(
            "❗️Добавление пользователя отменено. Возврат в главное меню.",
            reply_markup=MAIN_MENU
        )
        return ConversationHandler.END

    # Если в сообщении есть contact — значит админ переслал контакт клиента
    if update.message.contact:
        user_id_peer = update.message.contact.user_id
        # Сохраняем как строку формата "id:<peer_id>"
        contact_str = f"id:{user_id_peer}"
    else:
        # Иначе берём чистый текст, который админ ввёл вручную
        # Здесь ожидаем, что он уже в формате "@username" или "id:7370682957"
        contact_str = text

    # Сохраняем в user_data
    nu = context.user_data['new_user']
    nu['telegram_contact'] = contact_str

    # Создаём записи для нового клиента
    client_id = str(uuid.uuid4())
    sub_id = str(uuid.uuid4())
    client = {
        'id': client_id,
        'email': f"{nu['username']}|{nu['telegram_contact']}",
        'enable': True,
        'expiryTime': 0,
        'flow': '',
        'limitIp': 0,
        'totalGB': 0,
        'tgId': '',
        'subId': sub_id,
        'reset': 0
    }

    results = []
    sessions = get_sessions()
    for server_name, session in sessions.items():
        try:
            inbound_id = session.config.inbound_id
            session.add_client_to_inbound(inbound_id, client)
            results.append((server_name, True))
        except Exception as e:
            logger.error(f"Error adding client on {server_name}: {e}")
            results.append((server_name, False))

    # Отправляем QR-коды и URL
    for srv, ok in results:
        session = sessions[srv]
        if not ok:
            await update.message.reply_text(f"⚠️ Не удалось добавить на сервер {srv}")
            continue
        url = build_vless_url(session, client_id, sub_id)
        qr = generate_qr(url)
        await update.message.reply_photo(qr, caption=f"Вот ваши данные подключения для {srv}:")
        await update.message.reply_text(
            f"```plain\n{url}```",
            parse_mode="Markdown"
        )

    # Сохраняем нового пользователя в USERS_DB
    users = load_users(USERS_DB)
    servers = get_servers()
    nu['clients'] = {
        srv.name: {'id': client_id, 'subId': sub_id}
        for srv in servers
    }
    # Здесь nu['telegram_contact'] уже хранит строку "id:<peer_id>" или "@username"
    nu['telegram_id'] = contact_str
    users.append(nu)
    save_users(USERS_DB, users)

    # После окончания возвращаем главное меню
    await update.message.reply_text(
        "✅ Пользователь добавлен и данные подключения отправлены. Выберите следующее действие:",
        reply_markup=MAIN_MENU
    )
    return ConversationHandler.END


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатываем нажатие «👥 Список пользователей»: показываем inline-кнопки
    с каждым username, чтобы можно было отправить им соединения заново.
    """
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text(
            "Пожалуйста, /start и введите секрет, чтобы продолжить."
        )
        return

    users = load_users(USERS_DB)
    if not users:
        await update.message.reply_text("Пользователей нет.", reply_markup=MAIN_MENU)
        return

    buttons = [
        [InlineKeyboardButton(u['username'], callback_data=u['username'])]
        for u in users
    ]
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(
        "Выберите пользователя, чтобы повторно отправить данные подключения:",
        reply_markup=kb
    )


async def user_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обрабатываем выбор пользователя из списка (inline-кнопка).
    Снова рассылаем QR и URL по всем серверам.
    """
    query = update.callback_query
    await query.answer()
    username = query.data

    users = load_users(USERS_DB)
    user = next((u for u in users if u['username'] == username), None)
    if not user:
        await query.edit_message_text("Пользователь не найден.", reply_markup=MAIN_MENU)
        return

    sessions = get_sessions()

    for server_name, session in sessions.items():
        srv_cfg = session.config
        info = user['clients'].get(srv_cfg.name)
        if not info:
            continue
        client_id = info['id']
        sub_id = info['subId']
        url = build_vless_url(session, client_id, sub_id)
        qr = generate_qr(url)
        await query.message.reply_photo(qr, caption=f"Данные подключения для {srv_cfg.name}:")
        await query.message.reply_text(
            f"```plain\n{url}```",
            parse_mode="Markdown"
        )

    await query.edit_message_text(
        f"✅ Данные подключения для `{username}` были повторно отправлены.",
        parse_mode="Markdown",
        reply_markup=MAIN_MENU
    )


async def sync_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Синхронизация клиентов между всеми серверами и обновление USERS_DB.
    Логика:
    1. Собрать списки клиентов со всех серверов.
    2. Добавить отсутствующих клиентов на те сервера, где их нет.
    3. Обновить USERS_DB (добавить новых, обновить существующих).
    4. Удалить из USERS_DB тех, кто исчез со всех серверов.
    """
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text(
            "Пожалуйста, /start и введите секрет, чтобы продолжить."
        )
        return

    await update.message.reply_text("🔄 Начинаю синхронизацию клиентов…")

    sessions: Dict[str, X3UIClient] = get_sessions()
    clients_per_server: Dict[str, List[Dict]] = {}

    # 1. Собираем клиентов со всех серверов
    for srv_name, session in sessions.items():
        try:
            inbound_id = session.config.inbound_id
            inbound_obj = session.get_inbound(inbound_id)
            settings = json.loads(inbound_obj["settings"])
            clients_list = settings.get("clients", [])
            clients_per_server[srv_name] = clients_list
        except Exception as e:
            logger.error(f"Не удалось получить inbound с сервера {srv_name}: {e}")
            clients_per_server[srv_name] = []

    # Структуры для объединения по email
    email_to_servers: Dict[str, set] = {}
    email_to_example_client: Dict[str, Dict] = {}
    for srv_name, clients in clients_per_server.items():
        for client in clients:
            email = client.get("email")
            if not email:
                continue
            email_to_servers.setdefault(email, set()).add(srv_name)
            if email not in email_to_example_client:
                email_to_example_client[email] = client

    all_emails = set(email_to_servers.keys())

    # 2. Добавляем клиентов на те сервера, где их нет
    for email in all_emails:
        present_on = email_to_servers[email]
        missing_on = set(sessions.keys()) - present_on
        if not missing_on:
            continue

        template = email_to_example_client[email]
        for missing_srv in missing_on:
            session = sessions[missing_srv]
            inbound_id = session.config.inbound_id

            # Копируем поля клиента, но создаём новый id и subId
            new_client = template.copy()
            new_client["email"] = email

            try:
                session.add_client_to_inbound(inbound_id, new_client)
                await update.message.reply_text(f"✅ Клиент `{email}` добавлен на сервер `{missing_srv}`.")
            except Exception as e:
                logger.error(f"Ошибка при добавлении клиента {email} на {missing_srv}: {e}")
                await update.message.reply_text(f"⚠️ Не удалось добавить клиента `{email}` на `{missing_srv}`.")

    # 3. Обновляем USERS_DB
    users_db_path = USERS_DB
    existing_users = load_users(users_db_path)
    updated_users: List[Dict] = []
    existing_emails_in_db = set()

    # 3.1. Сначала собираем email’ы из USERS_DB
    for user in existing_users:
        username = user.get("username")
        contact = user.get("telegram_contact", user.get("telegram_id", ""))
        if contact:
            reconstructed_email = f"{username}|{contact}"
        else:
            reconstructed_email = username
        existing_emails_in_db.add(reconstructed_email)

    # 3.2. Если email есть на серверах, но нет в USERS_DB → добавляем нового пользователя
    for email in all_emails:
        if email not in existing_emails_in_db:
            if "|" in email:
                username_part, contact_part = email.split("|", 1)
            else:
                username_part, contact_part = email, ""
            clients_map: Dict[str, Dict] = {}
            for srv_name, clients in clients_per_server.items():
                for client in clients:
                    if client.get("email") == email:
                        clients_map[srv_name] = {
                            "id": client.get("id"),
                            "subId": client.get("subId")
                        }
            new_user_entry = {
                "username": username_part,
                "telegram_contact": contact_part,
                "telegram_id": contact_part,
                "clients": clients_map
            }
            updated_users.append(new_user_entry)
            await update.message.reply_text(f"➕ Пользователь `{username_part}` (email `{email}`) добавлен в базу.")

    # 3.3. Обрабатываем уже существующих пользователей: обновляем clients или удаляем, если клиента нет ни на одном сервере
    for user in existing_users:
        username = user.get("username")
        contact = user.get("telegram_contact", user.get("telegram_id", ""))
        if contact:
            user_email = f"{username}|{contact}"
        else:
            user_email = username

        if user_email in all_emails:
            # Обновляем clients_map
            clients_map: Dict[str, Dict] = {}
            for srv_name, clients in clients_per_server.items():
                for client in clients:
                    if client.get("email") == user_email:
                        clients_map[srv_name] = {
                            "id": client.get("id"),
                            "subId": client.get("subId")
                        }
            user["clients"] = clients_map
            updated_users.append(user)
        else:
            # Клиент исчез со всех серверов → удаляем из базы
            await update.message.reply_text(f"🗑️ Пользователь `{username}` удалён из базы.")

    # Сохраняем обновлённый USERS_DB
    save_users(users_db_path, updated_users)

    await update.message.reply_text(
        "✅ Синхронизация клиентов завершена.",
        reply_markup=MAIN_MENU
    )


# Обработчик «пункта меню»: если пользователь нажал «➕ Добавить пользователя»
async def menu_add_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await add_user_cmd(update, context)


# Обработчик «пункта меню»: «👥 Список пользователей»
async def menu_list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await list_users(update, context)


# Обработчик «пункта меню»: «🔄 Синхронизировать клиентов»
async def menu_sync_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await sync_clients(update, context)


def main():
    cfg = load_app_config(CONFIG_PATH)
    app = ApplicationBuilder().token(cfg.bot.token).build()

    # ConversationHandler для ввода секрета
    secret_conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, secret_handler)]
        },
        fallbacks=[]
    )
    app.add_handler(secret_conv)

    # ConversationHandler для добавления пользователя
    add_conv = ConversationHandler(
        entry_points=[
            # кроме кнопки/команды, можно по старому /add_user
            CommandHandler('add_user', add_user_cmd),
            MessageHandler(filters.Regex(r'^➕ Добавить пользователя$'), menu_add_user)
        ],
        states={
            ADD_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_username)],
            # В ADD_CONTACT обрабатываем и contact, и текст (в том числе «Отмена»)
            ADD_CONTACT: [MessageHandler(filters.CONTACT | (filters.TEXT & ~filters.COMMAND), add_contact)],
        },
        fallbacks=[]
    )
    app.add_handler(add_conv)

    # Обработчики для остальных пунктов меню
    # Команда /list_users или кнопка «👥 Список пользователей»
    app.add_handler(CommandHandler('list_users', list_users))
    app.add_handler(MessageHandler(filters.Regex(r'^👥 Список пользователей$'), menu_list_users))
    app.add_handler(CallbackQueryHandler(user_selected))

    # Команда /sync_clients или кнопка «🔄 Синхронизировать клиентов»
    app.add_handler(CommandHandler('sync_clients', sync_clients))
    app.add_handler(MessageHandler(filters.Regex(r'^🔄 Синхронизировать клиентов$'), menu_sync_clients))

    logger.info("Bot is polling...")
    app.run_polling()


if __name__ == '__main__':
    main()
