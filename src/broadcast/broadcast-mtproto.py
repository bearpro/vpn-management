import asyncio
from typing import Any, Iterable, List
from dotenv import load_dotenv
from telethon import TelegramClient
import json
import os
import re
import sys
import qrcode
from dataclasses import dataclass
import pathlib

load_dotenv()

@dataclass
class DataToSend:
    tg_peer: str | int
    url: str
    qr_path: str

def build_qr_path(qr_key: str) -> str:
    safe_qr_key = re.sub(r"[^A-Za-z0-9_.-]", "_", qr_key)
    return f"./data/tmp/{safe_qr_key}.jpg"

def create_qr(url: str, qr_key: str) -> str:
    qr_path = build_qr_path(qr_key)
    qr_file = pathlib.Path(qr_path)
    if qr_file.exists():
        qr_file.unlink()

    qr = qrcode.make(url)
    qr.save(qr_path)
    return qr_path

def extract_tg_username(description: str | None) -> str | None:
    if not description:
        return None

    match = re.search(r"@([A-Za-z0-9_]{5,})", description)
    if match is None:
        return None

    return f"@{match.group(1)}"

def get_recipient(user: dict[str, Any]) -> str | int:
    telegram_id = user.get("telegramId")
    if telegram_id is not None:
        return int(telegram_id)

    tg_username = extract_tg_username(user.get("description"))
    if tg_username is not None:
        return tg_username

    raise ValueError("missing telegramId and @username in description")

def prepare_json_data(users: Iterable[dict[str, Any]]) -> List[DataToSend]:
    data_to_send: List[DataToSend] = []

    for index, user in enumerate(users, start=1):
        username = user.get("username", f"user-{index}")
        url = user.get("subscriptionUrl")
        if not url:
            print(f"Skipping {username}: missing subscriptionUrl")
            continue

        try:
            recipient = get_recipient(user)
        except ValueError as exc:
            print(f"Skipping {username}: {exc}")
            continue

        qr_key = str(user.get("telegramId") or username or user.get("id") or index)
        qr_path = create_qr(url, qr_key)
        data_to_send.append(DataToSend(recipient, url, qr_path))

    return data_to_send

def prepare_list(path: str) -> List[DataToSend]:
    pathlib.Path("./data/tmp/").mkdir(parents=True, exist_ok=True)

    with open(path, "r") as file:
        raw_content = file.read()

    payload = json.loads(raw_content)
    users = payload.get("response", {}).get("users", [])
    return prepare_json_data(users)

def get_templates() -> List[str]:
    with open("msg_template.txt", "r") as f:
        lines = f.readlines()
        whole = "".join(lines)
        templates = whole.split("----msg-br")
        return templates


async def send_messages(data_list: List[DataToSend]):
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")

    if not api_id or not api_hash:
        raise RuntimeError("TG_API_ID and TG_API_HASH must be set")

    client = TelegramClient("data/tg_main", api_id, api_hash)
    await client.start()

    try:
        templates = get_templates()

        for receiver in data_list:
            try:
                for template in templates[:-1]:
                    message = template.replace("{{url}}", receiver.url)
                    await client.send_message(receiver.tg_peer, message)

                last_message = templates[-1].replace("{{url}}", receiver.url)
                await client.send_file(
                    receiver.tg_peer,
                    receiver.qr_path,
                    caption=last_message,
                )
                print(f"Sent to {receiver.tg_peer}")
            except Exception as exc:
                print(f"Failed to send to {receiver.tg_peer}: {exc}")
    finally:
        await client.disconnect()

async def main():
    list_path = sys.argv[1]
    to_send_data_list = prepare_list(list_path)
    await send_messages(to_send_data_list)

if __name__ == "__main__":
    asyncio.run(main())
