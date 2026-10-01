"""Validated, operator-owned telemetry sources. Credentials never enter browser state."""

from dataclasses import dataclass, field
import ipaddress
import json
import os
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Source:
    identifier: str
    name: str
    url: str
    token: str = field(default="", repr=False)
    authority: str | None = None


@dataclass(frozen=True)
class Settings:
    name: str
    sources: tuple[Source, ...]
    interval: float = 1.0
    timeout: float = 1.5
    history: int = 300


def loopback(address):
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return address.lower() == "localhost"


def load(path):
    path = Path(path).resolve()
    root = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(root, dict):
        raise ValueError("Dashboard configuration must be a JSON object")
    entries = root.get("brokers")
    if not isinstance(entries, list) or not 1 <= len(entries) <= 32:
        raise ValueError("Dashboard configuration needs 1..32 brokers")
    sources, identifiers, urls = [], set(), set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("Each broker must be a JSON object")
        identifier = entry.get("id", "")
        name = entry.get("name", identifier)
        url = entry.get("url", "")
        if not isinstance(url, str):
            raise ValueError("Broker URL must be a string")
        url = url.rstrip("/")
        parsed = urlsplit(url)
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("Broker telemetry port must be in 1..65535")
        if (not isinstance(identifier, str) or not identifier or len(identifier) > 100
                or identifier in identifiers or not isinstance(name, str) or not 1 <= len(name) <= 100):
            raise ValueError("Broker IDs must be unique and labels must contain 1..100 characters")
        if (parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password
                or parsed.query or parsed.fragment or parsed.path or url in urls):
            raise ValueError("Broker URL must be a unique HTTP(S) origin without credentials, path or query")
        if parsed.scheme == "http" and not loopback(parsed.hostname):
            raise ValueError("Remote telemetry needs HTTPS; an SSH tunnel can expose a loopback HTTP endpoint")
        variable = entry.get("tokenEnv")
        token = os.environ.get(variable, "") if variable else ""
        if variable and (not 32 <= len(token) <= 256 or any(ord(c) < 33 or ord(c) > 126 for c in token)):
            raise ValueError(f"Missing or invalid telemetry token environment variable for {identifier}")
        authority = entry.get("authority")
        if authority:
            authority = str((path.parent / authority).resolve())
            if not Path(authority).is_file():
                raise ValueError(f"CA file does not exist for {identifier}")
        identifiers.add(identifier)
        urls.add(url)
        sources.append(Source(identifier, name, url, token, authority))
    interval = root.get("intervalMs", 1000)
    timeout = root.get("timeoutMs", 1500)
    history = root.get("historySamples", 300)
    if (type(interval) is not int or not 500 <= interval <= 10000 or type(timeout) is not int
            or not 200 <= timeout <= 5000 or type(history) is not int or not 30 <= history <= 900):
        raise ValueError("Invalid polling, timeout or history bounds")
    name = root.get("name", "Empy network")
    if not isinstance(name, str) or not 1 <= len(name) <= 100:
        raise ValueError("Workspace name must contain 1..100 characters")
    return Settings(name, tuple(sources), interval / 1000, timeout / 1000, history)
