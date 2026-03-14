#!/usr/bin/env python3

from __future__ import annotations

import os
import sys


def main() -> int:
    mode = os.environ.get("APP_MODE", "headless").strip().lower() or "headless"

    if mode == "headless":
        os.execv("/bin/sh", ["/bin/sh", "/usr/local/bin/clamav_scheduled.sh"])

    if mode == "ui":
        os.execv(sys.executable, [sys.executable, "/usr/local/bin/clamav_ui_server.py"])

    print(f"[ERROR] Unsupported APP_MODE={mode!r}. Use 'headless' or 'ui'.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
