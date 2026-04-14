import asyncio
import argparse
from typing import Any, Iterable, List
from dotenv import load_dotenv
from telethon import TelegramClient
import json
import os
import re
import qrcode
from dataclasses import dataclass
import pathlib

load_dotenv()

@dataclass
class DataToSend:
    username: str
    telegram_id: int | None
    description_username: str | None
    url: str
    qr_path: str

    def log_identity(self) -> str:
        parts = [f"username={self.username}"]
        if self.telegram_id is not None:
            parts.append(f"peer={self.telegram_id}")
        if self.description_username is not None:
            parts.append(f"description_username={self.description_username}")
        return " ".join(parts)

    def recipient_candidates(self, recipient_source: str) -> List[str | int]:
        candidates: List[str | int] = []

        if recipient_source == "id":
            ordered = [self.telegram_id, self.description_username]
        elif recipient_source == "username":
            ordered = [self.description_username, self.telegram_id]
        else:
            ordered = [self.telegram_id, self.description_username]

        for candidate in ordered:
            if candidate is None:
                continue
            if candidate in candidates:
                continue
            candidates.append(candidate)

        return candidates

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

def prepare_json_data(users: Iterable[dict[str, Any]]) -> List[DataToSend]:
    data_to_send: List[DataToSend] = []

    for index, user in enumerate(users, start=1):
        username = user.get("username", f"user-{index}")
        url = user.get("subscriptionUrl")
        if not url:
            print(f"Skipping {username}: missing subscriptionUrl")
            continue

        telegram_id = user.get("telegramId")
        if telegram_id is not None:
            telegram_id = int(telegram_id)

        description_username = extract_tg_username(user.get("description"))
        if telegram_id is None and description_username is None:
            print(f"Skipping {username}: missing telegramId and @username in description")
            continue

        qr_key = str(telegram_id or username or user.get("id") or index)
        qr_path = create_qr(url, qr_key)
        data_to_send.append(
            DataToSend(
                username=username,
                telegram_id=telegram_id,
                description_username=description_username,
                url=url,
                qr_path=qr_path,
            )
        )

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("list_path")
    parser.add_argument(
        "--recipient-source",
        choices=["id", "username", "auto"],
        default="id",
        help="How to resolve Telegram recipient. 'id' preserves current behavior, "
        "'username' prefers @username from description, 'auto' tries id and then @username.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print how recipients would be resolved without sending messages.",
    )
    return parser.parse_args()

async def resolve_recipient(
    client: TelegramClient,
    receiver: DataToSend,
    recipient_source: str,
) -> tuple[str | int, Any]:
    last_error: Exception | None = None
    last_candidate: str | int | None = None

    for candidate in receiver.recipient_candidates(recipient_source):
        last_candidate = candidate
        try:
            entity = await client.get_input_entity(candidate)
            return candidate, entity
        except Exception as exc:
            last_error = exc
            if recipient_source != "auto":
                break

    if last_error is None:
        raise RuntimeError("no recipient candidates available")

    raise RuntimeError(f"via={last_candidate}: {last_error}") from last_error

async def send_messages(
    data_list: List[DataToSend],
    recipient_source: str,
    dry_run: bool,
):
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")

    if dry_run:
        for receiver in data_list:
            print(
                f"Dry run {receiver.log_identity()} "
                f"candidates={receiver.recipient_candidates(recipient_source)}"
            )
        return

    if not api_id or not api_hash:
        raise RuntimeError("TG_API_ID and TG_API_HASH must be set")

    client = TelegramClient("data/tg_main", api_id, api_hash)
    await client.start()

    try:
        templates = get_templates()

        for receiver in data_list:
            try:
                chosen_candidate, entity = await resolve_recipient(
                    client,
                    receiver,
                    recipient_source,
                )

                for template in templates[:-1]:
                    message = template.replace("{{url}}", receiver.url)
                    await client.send_message(entity, message)

                last_message = templates[-1].replace("{{url}}", receiver.url)
                await client.send_file(
                    entity,
                    receiver.qr_path,
                    caption=last_message,
                )
                print(f"Sent {receiver.log_identity()} via={chosen_candidate}")
            except Exception as exc:
                print(f"Failed {receiver.log_identity()}: {exc}")
    finally:
        await client.disconnect()

async def main():
    args = parse_args()
    to_send_data_list = prepare_list(args.list_path)
    await send_messages(
        to_send_data_list,
        recipient_source=args.recipient_source,
        dry_run=args.dry_run,
    )

if __name__ == "__main__":
    asyncio.run(main())
