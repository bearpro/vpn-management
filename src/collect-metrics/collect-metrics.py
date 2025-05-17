#!/usr/bin/env python3
import os
import time
import logging
import requests
import yaml
from prometheus_client import start_http_server, Gauge

# === Load config ===
def load_config():
    CONFIG_PATH = os.getenv("SERVERS_CONFIG_PATH", "config.yaml")
    with open(CONFIG_PATH, 'r') as f:
        cfg = yaml.safe_load(f)

    SERVERS = cfg.get("servers", [])
    POLL_INTERVAL = cfg.get("poll_interval", 30)
    return SERVERS,POLL_INTERVAL

# === Prometheus Gauges ===
g_up = Gauge(
    "user_upload_bytes",
    "Bytes uploaded by a user",
    ["server", "email", "inbound_id"],
)
g_down = Gauge(
    "user_download_bytes",
    "Bytes downloaded by a user",
    ["server", "email", "inbound_id"],
)
g_total = Gauge(
    "user_total_bytes",
    "Total bytes transferred by a user",
    ["server", "email", "inbound_id"],
)

# === Helpers ===
def login(server):
    sess = requests.Session()
    url = server["base_url"].rstrip("/") + "/login"
    resp = sess.post(
        url,
        data={
            "username": server["username"],
            "password": server["password"]
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", False):
        raise RuntimeError(f"Login failed ({server['name']}): {data.get('msg')}")
    logging.info(f"Logged in to {server['name']}")
    return sess

def fetch_and_export(server, session):
    url = server["base_url"].rstrip("/") + "/panel/api/inbounds/list"
    resp = session.get(url, headers={"Accept": "application/json"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success", False):
        raise RuntimeError(f"Error fetching inbounds ({server['name']}): {data.get('msg')}")
    for inbound in data.get("obj", []):
        inbound_id = str(inbound["id"])
        for client in inbound.get("clientStats") or []:
            labels = {
                "server": server["name"],
                "email":  client["email"],
                "inbound_id": inbound_id,
            }
            g_up.labels(**labels).set(client["up"])
            g_down.labels(**labels).set(client["down"])
            g_total.labels(**labels).set(client["total"])

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    start_http_server(8000)
    logging.info("Prometheus metrics exposed on :8000")

    while True:
        SERVERS, POLL_INTERVAL = load_config()

        for srv in SERVERS:
            try:
                sess = login(srv)
                fetch_and_export(srv, sess)
            except Exception as exc:
                logging.error(f"[{srv['name']}] {exc}")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
