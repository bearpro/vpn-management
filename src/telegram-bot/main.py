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
    Update, ReplyKeyboardMarkup, KeyboardButton, InputMediaPhoto, InlineKeyboardButton, InlineKeyboardMarkup
)
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes,
    ConversationHandler, CallbackQueryHandler, filters
)

from config import load_app_config, ServerConnectionConfig

from x3uiClient import X3UIClient

# --- Configure logging ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
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

    sessions = {}
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
    For the expected URL, we:
      • Use the host from the base URL without its port.
      • Use the port from the inbound details (defaulting to 443).
      • Set the 'sid' parameter to an empty string.
      • Use a custom tag from the server configuration.
    """
    server_config = session.config
    inbound = server_config.inbound_id

    # Get inbound details from the server
    obj = session.get_inbound(inbound)
    
    # Assume the inbound object contains a 'port' field (default to 443 if missing)
    port = obj.get('port', 443)
    
    # Parse the base_url to extract just the hostname
    parsed = urlparse(server_config.base_url)
    host = parsed.hostname

    # find client email
    clients = json.loads(obj["settings"])["clients"]
    for client in clients:
        if (client['id'] == client_id):
            break
    email = client.get('email', client_id)
    
    # Parse the streamSettings
    ss = json.loads(obj['streamSettings'])
    rs = ss.get('realitySettings', {})
    settings = rs.get('settings', {})

    # Set the URL query parameters.
    # Note: We now set 'sid' to an empty string per the expected output.
    params = {
        'type': ss.get('network', 'tcp'),
        'security': ss.get('security', 'none'),
        'pbk': settings.get('publicKey', ''),
        'fp': settings.get('fingerprint', ''),
        'sni': rs.get('serverNames', [''])[0],
        'sid': '',  # deliberately empty
        'spx': obj.get('spiderX', '/')
    }
    qs = '&'.join(f"{k}={requests.utils.quote(str(v), safe='')}" for k, v in params.items())

    # Use a custom tag if available; otherwise fall back to the server_config name.
    tag = f"{obj.get('remark', server_config.name)}-{email}"
    # tag = getattr(server_config, 'custom_tag', server_config.name)
    url = f"vless://{client_id}@{host}:{port}?{qs}#{tag}"
    return url


def generate_qr(url):
    img = qrcode.make(url)
    bio = BytesIO()
    bio.name = 'qr.png'
    img.save(bio)
    bio.seek(0)
    return bio

auth_data = load_authenticated_users()
authenticated_users = set(auth_data.get("users", []))
stored_secret = auth_data.get("secret", "")

# --- Handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Welcome! Please provide the secret to continue:")
    return SECRET


async def secret_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global stored_secret, authenticated_users

    config = load_app_config(CONFIG_PATH)

    if config.bot.secret != stored_secret:
        authenticated_users.clear()
        stored_secret = config.bot.secret

    text = update.message.text.strip()
    user_id = update.effective_user.id
    if text == config.bot.secret:
        authenticated_users.add(user_id)
        
        # Persist updated auth info
        save_authenticated_users({"secret": stored_secret, "users": list(authenticated_users)})
        
        await update.message.reply_text("✅ Authenticated! You can now use /add_user or /list_users.")
        return ConversationHandler.END
    else:
        await update.message.reply_text("❌ Incorrect secret. Try again:")
        return SECRET


async def add_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text("Please /start and enter the secret first.")
        return ConversationHandler.END
    await update.message.reply_text("Enter the system username for the new client:")
    return ADD_USERNAME


async def add_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_user'] = {'username': update.message.text.strip()}
    # ask for contact
    kb = ReplyKeyboardMarkup([[KeyboardButton("Share Contact", request_contact=True)]], one_time_keyboard=True)
    await update.message.reply_text("Please share your Telegram contact or enter your Telegram ID/username:", reply_markup=kb)
    return ADD_CONTACT


async def add_contact(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # extract contact info
    if update.message.contact:
        contact = update.message.contact.user_id
    else:
        contact = update.message.text.strip()
    nu = context.user_data['new_user']
    nu['telegram_contact'] = contact

    # prepare client record
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
    # call add_client_to_inbound on each server
    sessions = get_sessions()
    for server_name in sessions:
        session = sessions[server_name]
        try:
            inbound_id = session.config.inbound_id
            session.add_client_to_inbound(inbound_id, client)
            results.append((server_name, True))
        except Exception as e:
            logger.error(f"Error adding client on {server_name}: {e}")
            results.append((server_name, False))

    # send QR codes and URLs
    for srv, ok in results:
        session = sessions[srv]
        if not ok:
            await update.message.reply_text(f"⚠️ Failed on server {srv}")
            continue
        url = build_vless_url(session, client_id, sub_id)
        qr = generate_qr(url)
        await update.message.reply_photo(qr, caption=f"Here is your connection for {srv}:")
        await update.message.reply_text(f"```plain\n{url}```", parse_mode="Markdown")

    # save user to DB
    users = load_users(USERS_DB)
    servers = get_servers()
    nu['clients'] = {srv.name: {'id': client_id, 'subId': sub_id} for srv in servers}
    nu['telegram_id'] = contact
    users.append(nu)
    save_users(USERS_DB, users)

    await update.message.reply_text("✅ User added and connection details sent.")
    return ConversationHandler.END


async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text("Please /start and enter the secret first.")
        return
    users = load_users(USERS_DB)
    if not users:
        await update.message.reply_text("No users found.")
        return
    buttons = [[InlineKeyboardButton(u['username'], callback_data=u['username'])] for u in users]
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text("Select a user to resend their connection details:", reply_markup=kb)


async def user_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    username = query.data
    users = load_users(USERS_DB)
    user = next((u for u in users if u['username'] == username), None)
    if not user:
        await query.edit_message_text("User not found.")
        return
    
    servers = get_sessions()

    for server_name in servers:
        srv = servers[server_name]
        srv_cfg = srv.config

        info = user['clients'].get(srv_cfg.name)
        if not info:
            continue
        client_id = info['id']
        sub_id = info['subId']
        url = build_vless_url(srv, client_id, sub_id)
        qr = generate_qr(url)
        await query.message.reply_photo(qr, caption=f"Connection for {srv_cfg.name}:")
        await query.message.reply_text(f"```plain\n{url}```", parse_mode="Markdown")

    await query.edit_message_text(f"✅ Resent details for {username}.")


async def sync_clients(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    1. Загрузить со всех серверов inbound и извлечь из него список клиентов
    2. Если на каком-то сервере клиент есть, а на другом нет - нужно добавить этого клиента
       на тот сервер, где он отсутствует. Ключ – поле email (id могут отличаться).
    3. Если клиент есть на сервере, но отсутствует в USERS_DB, то добавить его в USERS_DB
    4. Если клиент есть в USERS_DB, но отсутствует на всех серверах, то удалить его из USERS_DB
    """
    user_id = update.effective_user.id
    if user_id not in authenticated_users:
        await update.message.reply_text("Пожалуйста, /start и введите секрет, чтобы продолжить.")
        return

    await update.message.reply_text("🔄 Начинаю синхронизацию клиентов…")

    # 1. Собираем со всех серверов список клиентов
    sessions: Dict[str, X3UIClient] = get_sessions()
    # clients_per_server: server_name -> [ {client_dict}, … ]
    clients_per_server: Dict[str, List[Dict]] = {}

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

    # Построим вспомогательные структуры:
    # email_to_servers: email -> set of серверов, где он есть
    # email_to_example_client: email -> один из client_dict (для копирования при добавлении)
    email_to_servers: Dict[str, set] = {}
    email_to_example_client: Dict[str, Dict] = {}

    for srv_name, clients in clients_per_server.items():
        for client in clients:
            email = client.get("email")
            if not email:
                continue
            email_to_servers.setdefault(email, set()).add(srv_name)
            # сохраним первый встретившийся client_dict по каждому email
            if email not in email_to_example_client:
                email_to_example_client[email] = client

    all_emails = set(email_to_servers.keys())

    # 2. Пройти по каждому email и если он отсутствует на каком-то сервере – добавить
    for email in all_emails:
        present_on = email_to_servers[email]
        missing_on = set(sessions.keys()) - present_on
        if not missing_on:
            continue

        # Берём шаблон client_dict с одного из серверов, где он уже есть
        template = email_to_example_client[email]

        for missing_srv in missing_on:
            session = sessions[missing_srv]
            inbound_id = session.config.inbound_id
            # Копируем все поля клиента, но даём новый id и new subId
            new_client = template.copy()

            new_client["email"] = email  # оставляем точно такое же email

            try:
                session.add_client_to_inbound(inbound_id, new_client)
                await update.message.reply_text(f"✅ Клиент `{email}` добавлен на сервер `{missing_srv}`.")
            except Exception as e:
                logger.error(f"Ошибка при добавлении клиента {email} на {missing_srv}: {e}")
                await update.message.reply_text(f"⚠️ Не удалось добавить клиента `{email}` на `{missing_srv}`.")

    # 3. и 4. Обновляем файл USERS_DB
    users_db_path = USERS_DB
    existing_users = load_users(users_db_path)  # это список словарей-юзеров
    updated_users: List[Dict] = []
    existing_emails_in_db = set()

    # Сначала найдём, какие email уже в USERS_DB
    for user in existing_users:
        username = user.get("username")
        contact = user.get("telegram_contact", user.get("telegram_id", ""))
        if contact:
            reconstructed_email = f"{username}|{contact}"
        else:
            reconstructed_email = username
        existing_emails_in_db.add(reconstructed_email)

    # 3.1: Если на сервере есть email, а в USERS_DB его нет → добавить нового пользователя
    for email in all_emails:
        if email not in existing_emails_in_db:
            # разбор email: "{username}|{telegram_contact}" или просто "username"
            if "|" in email:
                username_part, contact_part = email.split("|", 1)
            else:
                username_part, contact_part = email, ""
            # Собираем clients‐mapping: для каждого сервера, где есть этот email,
            # запомним id и subId
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
            await update.message.reply_text(f"➕ Пользователь `{username_part}` (email `{email}`) добавлен в USERS_DB.")

    # 3.2 + 4: Обрабатываем всех уже существующих пользователей и либо обновляем, либо удаляем
    for user in existing_users:
        username = user.get("username")
        contact = user.get("telegram_contact", user.get("telegram_id", ""))
        if contact:
            user_email = f"{username}|{contact}"
        else:
            user_email = username

        if user_email in all_emails:
            # Обновляем mapping текущего пользователя:
            clients_map: Dict[str, Dict] = {}
            for srv_name, clients in clients_per_server.items():
                for client in clients:
                    if client.get("email") == user_email:
                        clients_map[srv_name] = {
                            "id": client.get("id"),
                            "subId": client.get("subId")
                        }
            # Сохраняем обновлённый словарь (с новыми серверами, если клиент был добавлен)
            user["clients"] = clients_map
            updated_users.append(user)
        else:
            # 4. Если в USERS_DB есть пользователь, но его email нет ни на одном сервере → удалить
            await update.message.reply_text(f"🗑️ Пользователь `{username}` (email `{user_email}`) удалён из USERS_DB.")

    # Перезаписываем USERS_DB новым списком
    save_users(users_db_path, updated_users)

    await update.message.reply_text("✅ Синхронизация клиентов завершена.")

    return

# --- Set up the Application ---

def main():
    cfg = load_app_config(CONFIG_PATH)
    app = ApplicationBuilder().token(cfg.bot.token).build()

    # secret conversation
    secret_conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={SECRET: [MessageHandler(filters.TEXT & ~filters.COMMAND, secret_handler)]},
        fallbacks=[]
    )
    app.add_handler(secret_conv)

    # add_user conversation
    add_conv = ConversationHandler(
        entry_points=[CommandHandler('add_user', add_user_cmd)],
        states={
            ADD_USERNAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_username)],
            
            ADD_CONTACT: [MessageHandler(filters.CONTACT | (filters.TEXT & ~filters.COMMAND), add_contact)],
        },
        fallbacks=[]
    )
    app.add_handler(add_conv)

    # list users
    app.add_handler(CommandHandler('list_users', list_users))
    app.add_handler(CallbackQueryHandler(user_selected))
    
    # sync clients
    app.add_handler(CommandHandler('sync_clients', sync_clients))

    logger.info("Bot is polling...")
    app.run_polling()


if __name__ == '__main__':
    main()
