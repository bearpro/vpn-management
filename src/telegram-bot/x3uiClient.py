#!/usr/bin/env python3
import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any, Dict, List
import requests
from config import ServerConnectionConfig


class X3UIClient:
    def __init__(self, server: ServerConnectionConfig):
        self.config = server
        self.session = requests.Session()
        self._login()

    def _login(self):
        login_url = f"{self.config.base_url}/login"
        resp = self.session.post(login_url, data={
            'username': self.config.username,
            'password': self.config.password
        })
        resp.raise_for_status()
        body = resp.json()
        if not body.get("success"):
            raise RuntimeError(f"[{self.config.name}] Login failed: {body.get('msg')}")

    def add_client_to_inbound(self, inbound_id: int, client: Dict[str, Any]) -> None:
        """
        Add a single client to the specified inbound on one server.
        """
        url = f"{self.config.base_url}/panel/api/inbounds/addClient"
        payload = {
            "id": inbound_id,
            "settings": json.dumps({"clients": [client]})
        }
        resp = self.session.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(f"[{self.config.name}] addClient failed: {data.get('msg')}")
        print(f"[{self.config.name}] Client {client['email']} added to inbound {inbound_id}")

    def add_inbound(self, inbound_conf: Dict[str, Any]) -> int:
        """
        Create a new inbound on one server.
        Returns the new inbound ID.
        """
        url = f"{self.config.base_url}/panel/api/inbounds/add"
        body = {
            **{k: inbound_conf[k] for k in (
                "up", "down", "total", "remark", "enable",
                "expiryTime", "listen", "port", "protocol"
            )},
            "settings": json.dumps(inbound_conf["settings"]),
            "streamSettings": json.dumps(inbound_conf["streamSettings"]),
            "sniffing": json.dumps(inbound_conf["sniffing"]),
            "allocate": json.dumps(inbound_conf["allocate"]),
        }
        resp = self.session.post(url, json=body)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(f"[{self.config.name}] addInbound failed: {data.get('msg')}")
        new_id = data["obj"]["id"]
        print(f"[{self.config.name}] New inbound created with ID {new_id}")
        return new_id

    def get_inbound(self, inbound_id: int) -> Dict[str, Any]:
        """
        Makes GET request to /panel/api/inbounds/get/{inboundId}
        and returns the parsed inbound object.
        """
        url = f"{self.config.base_url}/panel/api/inbounds/get/{inbound_id}"
        resp = self.session.get(url)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            raise RuntimeError(f"[{self.config.name}] getInbound failed: {data.get('msg')}")
        return data["obj"]
