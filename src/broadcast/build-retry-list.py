import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path


FAILED_PATTERNS = [
    re.compile(r"^Failed to send to (?P<peer>[^:]+):"),
    re.compile(
        r"^Failed username=(?P<username>\S+)(?: peer=(?P<peer>\S+))?"
        r"(?: description_username=(?P<description_username>\S+))?:"
    ),
]

SENT_PATTERNS = [
    re.compile(r"^Sent to (?P<peer>.+)$"),
    re.compile(
        r"^Sent username=(?P<username>\S+)(?: peer=(?P<peer>\S+))?"
        r"(?: description_username=(?P<description_username>\S+))?"
        r" via=(?P<via>\S+)$"
    ),
]


@dataclass
class UserRef:
    key: str
    user: dict
    identifiers: set[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--users",
        default="data/all-users.json",
        help="Source users JSON in the same format as export response.users[]",
    )
    parser.add_argument(
        "--log",
        default="data/broadcast.log",
        help="Broadcast log file with appended run outputs",
    )
    parser.add_argument(
        "--output",
        default="data/retry-users.json",
        help="Where to write filtered JSON with unresolved failures",
    )
    return parser.parse_args()


def extract_tg_username(description: str | None) -> str | None:
    if not description:
        return None

    match = re.search(r"@([A-Za-z0-9_]{5,})", description)
    if match is None:
        return None

    return f"@{match.group(1)}"


def build_user_refs(users: list[dict]) -> list[UserRef]:
    refs: list[UserRef] = []

    for user in users:
        username = user.get("username")
        telegram_id = user.get("telegramId")
        description_username = extract_tg_username(user.get("description"))

        identifiers = {username}
        if telegram_id is not None:
            identifiers.add(str(telegram_id))
        if description_username is not None:
            identifiers.add(description_username)

        refs.append(
            UserRef(
                key=username,
                user=user,
                identifiers=identifiers,
            )
        )

    return refs


def match_event(line: str) -> tuple[str, set[str]] | None:
    for pattern in FAILED_PATTERNS:
        match = pattern.search(line)
        if match:
            identifiers = {
                value
                for value in (
                    match.groupdict().get("peer"),
                    match.groupdict().get("username"),
                    match.groupdict().get("description_username"),
                )
                if value
            }
            return "failed", identifiers

    for pattern in SENT_PATTERNS:
        match = pattern.search(line)
        if match:
            identifiers = {
                value
                for value in (
                    match.groupdict().get("peer"),
                    match.groupdict().get("username"),
                    match.groupdict().get("description_username"),
                    match.groupdict().get("via"),
                )
                if value
            }
            return "sent", identifiers

    return None


def build_latest_status(refs: list[UserRef], log_lines: list[str]) -> dict[str, str]:
    latest_status: dict[str, str] = {}

    for line in log_lines:
        event = match_event(line.strip())
        if event is None:
            continue

        status, identifiers = event
        for ref in refs:
            if ref.identifiers & identifiers:
                latest_status[ref.key] = status

    return latest_status


def main() -> None:
    args = parse_args()

    users_payload = json.loads(Path(args.users).read_text())
    users = users_payload.get("response", {}).get("users", [])
    refs = build_user_refs(users)

    log_lines = Path(args.log).read_text().splitlines()
    latest_status = build_latest_status(refs, log_lines)

    retry_users = [ref.user for ref in refs if latest_status.get(ref.key) == "failed"]

    result = {
        "response": {
            "total": len(retry_users),
            "users": retry_users,
        }
    }
    Path(args.output).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")

    print(f"users_total={len(users)}")
    print(f"retry_total={len(retry_users)}")
    for user in retry_users:
        print(user.get("username"))


if __name__ == "__main__":
    main()
