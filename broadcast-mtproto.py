from typing import Generator, List
from dotenv import load_dotenv
from telethon import TelegramClient, events, sync
from urllib.parse import unquote
import os
import sys
import qrcode
from dataclasses import dataclass
import pathlib

load_dotenv()

@dataclass
class DataToSend:
    tg_username: str
    url: str
    qr_path: str

def prepare_list(path:str)-> List[DataToSend]:
    pathlib.Path("./data/tmp/").mkdir(parents=True, exist_ok=True)

    def get_lines():
        with open(path, 'r') as file:
            for line in file:
                yield unquote(line).strip()

    def prepare_data(raw_urls):
        for i, url in enumerate(raw_urls):
            d = url.split('|')
            if len(d) == 2:
                (url, tg_username) = d
                qr_key = tg_username[1:]
                qr_path = f"./data/tmp/{qr_key}.jpg"
                if pathlib.Path(qr_path).exists():
                    os.remove(qr_path)
            
                qr = qrcode.make(url)
                qr.save(qr_path)
                yield DataToSend(tg_username, url, qr_path)
            else:
                print(f"Invalid format at line {i+1}")
    urls = get_lines()
    data = prepare_data(urls)
    return list(data)

def get_templates() -> str:
    with open("msg_template.txt", 'r') as f:
        lines = f.readlines()
        whole = "".join(lines)
        templates = whole.split("----msg-br")
        return templates


def send_messages(data_list: List[DataToSend]):
    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")

    client = TelegramClient('data/tg_main', api_id, api_hash)
    client.start()

    templates = get_templates()

    for receiver in data_list:
        for template in templates:
            message = template.replace("{{url}}", receiver.url)
            client.send_message(receiver.tg_username, message)
        
        client.send_file(receiver.tg_username, receiver.qr_path)
        print(f"Sent to {receiver.tg_username}")

def main():
    list_path = sys.argv[1]
    to_send_data_list = prepare_list(list_path)
    send_messages(to_send_data_list)

if __name__ == "__main__":
    main()
