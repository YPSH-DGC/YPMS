#!/usr/bin/env python3

# -- PyYPSH ----------------------------------------------------- #
# ypms-launcher.py on PyYPSH                                      #
# Made by DiamondGotCat, Licensed under MIT License               #
# Copyright (c) 2025 DiamondGotCat                                #
# ---------------------------------------------- DiamondGotCat -- #

from __future__ import annotations

import os
import sys
import tempfile
import urllib.request
import urllib.error

# ---- Paths ---------------------------------------------------- #

YPMS_DIR: str = os.environ.get("YPMS_DIR") or os.path.join(os.path.expanduser("~"), ".ypms")
YPMS_BIN_DIR: str = os.environ.get("YPMS_BIN_DIR") or os.path.join(YPMS_DIR, "bin")
YPMS_MAIN_PATH: str = os.path.join(YPMS_BIN_DIR, "ypms.py")


# ---- Update --------------------------------------------------- #

def _download_to(path: str, url: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with urllib.request.urlopen(url) as resp:
        data = resp.read()

    fd, tmp = tempfile.mkstemp(prefix=".ypms-download-", suffix=".tmp",
                               dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

    try:
        st = os.stat(path)
        os.chmod(path, st.st_mode | 0o111)
    except Exception:
        pass


def update(tag: str = "latest") -> None:
    base = "https://github.com/YPSH-DGC/YPMS/releases"
    if tag == "latest":
        url = f"{base}/latest/download/ypms.py"
    else:
        url = f"{base}/download/{tag}/ypms.py"
    _download_to(YPMS_MAIN_PATH, url)


# ---- Launch --------------------------------------------------- #

def launch(argv: list[str]) -> "NoReturn":  # type: ignore[override]
    python = os.environ.get("YPMS_PYTHON") or sys.executable
    os.execv(python, [python, YPMS_MAIN_PATH, *argv])


# ---- Main ----------------------------------------------------- #

def main() -> None:
    args = sys.argv[1:]
    do_selfupdate = "selfupdate" in args
    first_run = not os.path.isfile(YPMS_MAIN_PATH)

    if first_run or do_selfupdate:
        try:
            update()
            print("[ypms-launcher] Updated ypms.py.", file=sys.stderr)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
            print(f"[ypms-launcher] Update failed: {e}", file=sys.stderr)
            if do_selfupdate:
                sys.exit(1)
            if not os.path.isfile(YPMS_MAIN_PATH):
                sys.exit(2)

    if not do_selfupdate:
        launch(args)
    return


if __name__ == "__main__":
    main()
