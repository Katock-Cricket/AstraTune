#!/usr/bin/env python3
"""AstraTune WebUI 入口：python at_webui.py → http://127.0.0.1:8080"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> None:
    import uvicorn

    from webui.app import app

    host = os.environ.get("AT_WEBUI_HOST", "127.0.0.1")
    port = int(os.environ.get("AT_WEBUI_PORT") or 8080)
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
