from __future__ import annotations

import base64
from typing import Any

import erlpack


def _convert_bytes(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    elif isinstance(value, dict):
        return {_convert_bytes(key): _convert_bytes(val) for key, val in value.items()}
    elif isinstance(value, list):
        return [_convert_bytes(item) for item in value]
    return value


def encode_recorded(value: Any) -> str:
    binary = erlpack.pack(value)
    encoded = base64.b64encode(binary).decode("ascii")

    return encoded.rstrip("=")


def decode_recorded(encoded: str) -> Any:
    padding = 4 - (len(encoded) % 4)

    if padding != 4:
        encoded = encoded + ("=" * padding)

    binary = base64.b64decode(encoded)

    return _convert_bytes(erlpack.unpack(binary))
