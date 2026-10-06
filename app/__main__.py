"""Entry point: ``python -m app`` starts the HTTP coordinator."""

from __future__ import annotations

import os

from .httpapi import serve


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    serve(host, port)


if __name__ == "__main__":
    main()
