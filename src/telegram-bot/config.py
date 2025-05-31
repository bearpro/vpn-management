from dataclasses import dataclass
from typing import Any, Dict, List
import yaml

@dataclass
class ServerConnectionConfig:
    name: str
    base_url: str
    username: str
    password: str
    inbound_id: int

@dataclass
class BotConfig:
    token: str
    secret: str

@dataclass
class AppConfig:
    bot: BotConfig
    servers: List[ServerConnectionConfig]

def load_app_config(path: str) -> AppConfig:
    """Load YAML config"""

    with open(path, "r") as f:
        data = yaml.safe_load(f)

    bot_data = data["bot"]
    servers_data = data["servers"]

    bot_config = BotConfig(token=bot_data["token"], secret=bot_data["secret"])
    servers = [
        ServerConnectionConfig(
            name=server["name"],
            base_url=server["base_url"].rstrip("/"),
            username=server["username"],
            password=server["password"],
            inbound_id=server["inbound_id"],
        )
        for server in servers_data
    ]

    return AppConfig(bot=bot_config, servers=servers)
