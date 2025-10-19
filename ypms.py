#!/usr/bin/env python3

# -- PyYPSH ----------------------------------------------------- #
# ypms.py on PyYPSH                                               #
# Made by DiamondGotCat, Licensed under MIT License               #
# Copyright (c) 2025 DiamondGotCat                                #
# ---------------------------------------------- DiamondGotCat -- #

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, List, Union, Callable

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

# ---- Optional Rich (UI) ---------------------------------------- #
try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.text import Text
    from rich.markup import escape as _rich_escape
    _RICH_AVAILABLE = True
except Exception:
    Console = None  # type: ignore
    Live = None     # type: ignore
    Text = None     # type: ignore
    _RICH_AVAILABLE = False

# ---- Paths & Constants ----------------------------------------- #

load_dotenv()

YPMS_DIR: str = os.environ.get("YPMS_DIR") or os.path.join(os.path.expanduser("~"), ".ypms")
YPMS_ENVS_DIR: str = os.environ.get("YPMS_ENVS_DIR") or os.path.join(YPMS_DIR, "envs")
SOURCES_PATH: str = os.path.join(YPMS_DIR, "sources.json")
INSTALLED_DB_PATH: str = os.path.join(YPMS_DIR, "installed.json")
CACHE_DIR: str = os.path.join(YPMS_DIR, "cache")

DEFAULT_SOURCES: Dict[str, str] = {
    "yopr": "https://ypsh-dgc.github.io/YPMS/yopr/ypms.json"
}

USER_AGENT = "YPMS (+https://github.com/YPSH-DGC/YPMS/)"
HTTP_TIMEOUT = 20  # seconds

# Verbose logging flag and helper
_VERBOSE: bool = False
def vlog(*msg: object) -> None:
    if _VERBOSE:
        print("[DEBUG]", *msg, file=sys.stderr)

# Pretty printing helpers
class Colors:
    """ ANSI Color Codes """
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    REPLACE = "\033[7m"
    CROSSED = "\033[9m"
    RESET = "\033[0m"

def _p_info(msg: str) -> None:
    print(f"{Colors.CYAN}(i){Colors.RESET} {msg}")

def _p_action(msg: str) -> None:
    print(f"{Colors.BLUE}(>){Colors.RESET} {msg}")

def _p_warn(msg: str) -> None:
    print(f"{Colors.YELLOW}(!){Colors.RESET} {msg}")

def _p_success(msg: str) -> None:
    print(f"{Colors.GREEN}{Colors.BOLD}(i) {msg}{Colors.RESET}")

def _p_error(msg: str) -> None:
    print(f"{Colors.RED}{Colors.BOLD}(!) {msg}{Colors.RESET}")

def _p_question(msg: str) -> None:
    print(f"{Colors.YELLOW}(?){Colors.RESET} {msg}", end="", flush=True)

_CONSOLE = Console() if (_RICH_AVAILABLE and sys.stdout.isatty()) else None

def _print_plan_item(i: int, kind: str, source: str, package_ref: str, version: Optional[str], footnote: Optional[int]) -> None:
    sym_map = {"install": "+", "update": "^", "target": "*"}
    color_map = {"install": "blue", "update": "yellow", "target": "cyan"}
    sym = sym_map.get(kind, "?")
    foot = f" *{footnote}" if footnote else ""
    if _CONSOLE:
        _CONSOLE.print(f"[white]{i}.[/] [bold {color_map.get(kind,'white')}]{sym}[/] [white]{source}/[/][bright_white]{package_ref}[/][white]@{version}{foot}[/]")
    else:
        print(f"{i}. {sym} {source}/{package_ref}@{version}{foot}")


# ---- Exceptions ------------------------------------------------ #

class YPMSError(Exception):
    pass


# ---- Platform helpers ----------------------------------------- #

def _norm_os() -> str:
    sp = sys.platform
    if sp.startswith("win"):
        return "windows"
    if sp.startswith("darwin"):
        return "darwin"
    return "linux"

def _norm_arch() -> str:
    m = platform.machine().lower()
    if m in ("x86_64", "amd64", "x64"):
        return "x86_64"
    if m in ("arm64", "aarch64"):
        return "arm64"
    return m or "unknown"

def _when_matches(when: Optional[Dict[str, List[str]]]) -> bool:
    if not when:
        return True
    os_ok = True
    arch_ok = True
    if "os" in when:
        os_ok = _norm_os() in {s.lower() for s in when["os"]}
    if "arch" in when:
        norm_arches = set()
        for a in when["arch"]:
            a = a.lower()
            if a in ("x86_64", "amd64", "x64"):
                norm_arches.add("x86_64")
            elif a in ("arm64", "aarch64"):
                norm_arches.add("arm64")
            else:
                norm_arches.add(a)
        arch_ok = _norm_arch() in norm_arches
    return os_ok and arch_ok


# ---- Substitution helper -------------------------------------- #

def _subst(s: str, *, env_dir: str, ctx: Dict[str, Any]) -> str:
    return (
        s.replace("{YPMS_ENV_DIR}", env_dir)
         .replace("{OS}", _norm_os())
         .replace("{ARCH}", _norm_arch())
         .replace("{PACKAGE_REF}", str(ctx.get("PACKAGE_REF", "")))
         .replace("{SOURCE_NAME}", str(ctx.get("SOURCE_NAME", "")))
         .replace("{RELEASE_ID}", str(ctx.get("RELEASE_ID", "")))
    )


# ---- Simple JSON cache ---------------------------------------- #

def _ensure_cache_dir() -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    vlog("cache-dir:", CACHE_DIR)

def _cache_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest() + ".json"

def _http_get_json(url: str, *, use_cache: bool = True, force_refresh: bool = False) -> Dict[str, Any]:
    _ensure_cache_dir()
    cpath = os.path.join(CACHE_DIR, _cache_key(url))
    vlog("GET JSON:", url, "| use_cache=", use_cache, "force_refresh=", force_refresh)
    if use_cache and not force_refresh and os.path.exists(cpath):
        try:
            with open(cpath, "r", encoding="utf-8") as f:
                obj = json.load(f)
                vlog("cache-hit:", cpath)
                return obj
        except Exception:
            pass

    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                raise YPMSError(f"HTTP {resp.status} for {url}")
            data = resp.read()
            vlog("http-status:", resp.status, "bytes=", len(data))
            try:
                obj = json.loads(data.decode("utf-8"))
                try:
                    with open(cpath, "w", encoding="utf-8") as f:
                        json.dump(obj, f, ensure_ascii=False, indent=2)
                        vlog("cache-write:", cpath)
                except Exception:
                    pass
                return obj
            except json.JSONDecodeError as e:
                raise YPMSError(f"Invalid JSON at {url}: {e}") from e
    except urllib.error.URLError as e:
        raise YPMSError(f"Failed to GET {url}: {e}") from e


# ---- Live UI (per package) ------------------------------------ #

class _PlainPackageUI:
    """Fallback UI when Rich is unavailable: prints lines without live updates."""
    def __init__(self, header: str):
        self.header = header
        print(header)

    def set_header(self, text: str) -> None:
        # Print replacement as a new line (no true replace in plain mode)
        print(text)

    def set_step(self, idx: int, text: str) -> None:
        print(f"      {idx}. {text}")

    def clear_steps_keep_header(self) -> None:
        # nothing to clear in plain mode
        pass

class PackageLiveUI:
    """Rich-backed UI: header + dynamic steps; steps disappear on finish; header is replaced."""
    def __init__(self, header: str, header_style: Optional[str] = None):
        self._use_rich = _RICH_AVAILABLE and sys.stdout.isatty()
        self.header = header
        if not self._use_rich:
            self._plain = _PlainPackageUI(header)
            return

        self.console = Console()
        self._header_text = Text(header, style=header_style or "")
        self._steps: Dict[int, Text] = {}
        self._live = Live(self._renderable(), console=self.console, refresh_per_second=20, transient=False)
        self._live.start()

    def _renderable(self):
        if not self._use_rich:
            return ""
        # Render header + steps (sorted)
        lines: List[Text] = [self._header_text]
        for i in sorted(self._steps.keys()):
            lines.append(self._steps[i])
        return Group(*lines)

    def set_header(self, text: str, style: Optional[str] = None) -> None:
        if not self._use_rich:
            self._plain.set_header(text)
            return
        self._header_text = Text(text, style=style or "")
        self._live.update(self._renderable())

    def set_step(self, idx: int, text: str, style: Optional[str] = None) -> None:
        if not self._use_rich:
            self._plain.set_step(idx, text)
            return
        self._steps[idx] = Text(f"      {idx}. {text}", style=style or "dim")
        self._live.update(self._renderable())

    def clear_steps_keep_header(self) -> None:
        if not self._use_rich:
            self._plain.clear_steps_keep_header()
            return
        self._steps.clear()
        self._live.update(self._renderable())

    def stop(self) -> None:
        if self._use_rich:
            self._live.stop()

# ---- HTTP download with Live updates --------------------------- #

def _http_download_with_progress(url: str, dest_path: str, *,
                                 ui: Optional[PackageLiveUI] = None,
                                 step_no: Optional[int] = None) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp, open(dest_path, "wb") as f:
            if resp.status != 200:
                raise YPMSError(f"HTTP {resp.status} for {url}")
            total = resp.getheader("Content-Length")
            total_size = int(total) if total and total.isdigit() else None
            downloaded = 0
            fname = os.path.basename(dest_path) or dest_path
            label = f"Downloading {fname}"
            if ui and step_no is not None:
                ui.set_step(step_no, f"{label} (0%)" if total_size else label, style="dim")
            else:
                print(f"      {step_no}. {label}" if step_no else f"      {label}")
            last_pct = -1
            while True:
                chunk = resp.read(1024 * 64)
                if not chunk:
                    break
                f.write(chunk)
                if total_size:
                    downloaded += len(chunk)
                    pct = int(downloaded * 100 / total_size)
                    if pct != last_pct:
                        last_pct = pct
                        if ui and step_no is not None:
                            ui.set_step(step_no, f"{label} ({pct}%)", style="dim")
                        else:
                            # overwrite-less fallback: print only on 10% steps
                            if pct % 10 == 0:
                                print(f"      {step_no}. {label} ({pct}%)" if step_no else f"      {label} ({pct}%)")
            # completion line stays as 100% until the package UI clears steps
            if ui and step_no is not None:
                if total_size:
                    ui.set_step(step_no, f"{label} (100%)", style="dim")
                else:
                    ui.set_step(step_no, f"Downloaded {fname}", style="dim")
            else:
                print(f"      {step_no}. Downloaded {fname}" if step_no else f"      Downloaded {fname}")
    except urllib.error.URLError as e:
        raise YPMSError(f"Failed to download {url}: {e}") from e


def _clear_all_cache() -> None:
    if os.path.isdir(CACHE_DIR):
        vlog("cache-clear:", CACHE_DIR)
        shutil.rmtree(CACHE_DIR, ignore_errors=True)


# ---- Source / Package ----------------------------------------- #

@dataclass
class RepoConfig:
    repo_id: str
    name: str
    desc: str
    base_url: str
    path_index: str
    path_package: str


class YPMSSource:
    def __init__(self, source_name: str, config_url: str, *, force_refresh: bool = False):
        self.source_name = source_name
        self.config_url = config_url
        vlog("source-init:", source_name, "config:", config_url, "force_refresh=", force_refresh)
        cfg = _http_get_json(config_url, use_cache=True, force_refresh=force_refresh)
        try:
            self.config = RepoConfig(
                repo_id=cfg["ypms.repo.id"],
                name=cfg["ypms.repo.name"],
                desc=cfg.get("ypms.repo.desc", ""),
                base_url=cfg["ypms.repo.url"].rstrip("/"),
                path_index=cfg["ypms.repo.path.index"],
                path_package=cfg["ypms.repo.path.package"],
            )
        except KeyError as e:
            raise YPMSError(f"Missing key in ypms.json ({config_url}): {e}") from e

    def _index_url(self) -> str:
        return f"{self.config.base_url}{self.config.path_index}"

    def _package_url(self, user_id: str, package_id: str) -> str:
        return f"{self.config.base_url}{self.config.path_package}".format(
            USER_ID=user_id, PACKAGE_ID=package_id
        )

    def fetch_index(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        return _http_get_json(self._index_url(), use_cache=True, force_refresh=force_refresh)

    def fetch_package_info(self, user_id: str, package_id: str, *, force_refresh: bool = False) -> Dict[str, Any]:
        return _http_get_json(self._package_url(user_id, package_id), use_cache=True, force_refresh=force_refresh)

    @staticmethod
    def resolve_release_tag(pkg_info: Dict[str, Any], tag: Optional[str]) -> str:
        orig = tag
        if not tag:
            tag = pkg_info.get("package.release.default")
            if not tag:
                alias = pkg_info.get("package.release.alias", {})
                tag = alias.get("latest")
            if not tag:
                lst = pkg_info.get("package.release.list", [])
                if lst:
                    tag = lst[0]
        alias = pkg_info.get("package.release.alias", {})
        resolved = alias.get(tag, tag)
        vlog("resolve-release:", {"requested": orig, "resolved": resolved})
        return resolved

    def fetch_release_info(self, pkg_info: Dict[str, Any], release_id: str, *, force_refresh: bool = False) -> Dict[str, Any]:
        url_tmpl = pkg_info["package.release.url"]
        url = url_tmpl.format(RELEASE_ID=release_id)
        return _http_get_json(url, use_cache=True, force_refresh=force_refresh)


class YPMSPackage:
    def __init__(self, source: YPMSSource, user: str, pkg_id: str):
        self.source = source
        self.user = user
        self.pkg_id = pkg_id
        self.pkg_info = self.source.fetch_package_info(user, pkg_id)

    def available_releases(self) -> Dict[str, Any]:
        return {
            "default": self.pkg_info.get("package.release.default"),
            "alias": self.pkg_info.get("package.release.alias", {}),
            "list": self.pkg_info.get("package.release.list", []),
        }

    def _resolve_env_dest(self, template: str, env_dir: str) -> str:
        return template.replace("{YPMS_ENV_DIR}", env_dir)


# ---- Guide execution helpers ---------------------------------- #

def _normalize_guide_to_steps(guide_obj: Any) -> List[Dict[str, Any]]:
    if not isinstance(guide_obj, dict):
        raise YPMSError("Guide object must be a dict")
    if "steps" in guide_obj:
        steps = guide_obj["steps"]
        if not isinstance(steps, list):
            raise YPMSError("Guide.steps must be a list")
        return steps
    return [guide_obj]

def _exec_step_download_file(step: Dict[str, Any], *, env_dir: str, ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step["content"]
    dest_template: str = content["dest"]
    url: str = content["url"]
    dest_path = dest_template.replace("{YPMS_ENV_DIR}", env_dir)
    vlog("guide: download-file", {"url": url, "dest": dest_path})
    _http_download_with_progress(url, dest_path, ui=ui, step_no=step_no)
    return dest_path

def _exec_step_license_agreement(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any], ui: Optional[PackageLiveUI], step_no: Optional[int]) -> bool:
    content = step.get("content")
    if not isinstance(content, str):
        raise YPMSError("license-agreement-url guide: invalid content")
    vlog(f"Please review the license and press A key to accept: {content}")
    b = sys.stdin.buffer.read(1)
    if b == b'a':
        vlog("Accepted.")
        return True
    else:
        raise YPMSError("Not accepted the license.")

def _exec_step_python(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any], ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step.get("content")
    if isinstance(content, str):
        code = content
        cwd = None
    elif isinstance(content, dict):
        code = content.get("code", "")
        cwd = content.get("cwd")
    else:
        raise YPMSError("python guide: invalid content")

    if not isinstance(code, str) or not code.strip():
        raise YPMSError("python guide: empty code")

    g: Dict[str, Any] = {
        "__name__": "__main__",
        "YPMS_ENV_DIR": env_dir,
        "OS": _norm_os(),
        "ARCH": _norm_arch(),
        **context,
    }
    l: Dict[str, Any] = {}
    old_cwd = None
    try:
        if cwd:
            old_cwd = os.getcwd()
            os.makedirs(cwd, exist_ok=True)
            os.chdir(cwd)
        ui.set_step(step_no or 0, "Executing python step") if ui else None
        exec(compile(code, "<ypms_guide_python>", "exec"), g, l)
    finally:
        if old_cwd is not None:
            os.chdir(old_cwd)

    result_path = l.get("RESULT_PATH") or g.get("RESULT_PATH")
    return str(result_path) if result_path else ""

def _exec_step_shell(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any], ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step.get("content")
    cmds: List[Union[str, List[str]]]
    cwd: Optional[str] = None
    use_shell_default: Optional[bool] = None
    check = True
    extra_env: Dict[str, str] = {}

    if isinstance(content, str):
        cmds = [content]
        use_shell_default = True
    elif isinstance(content, list):
        cmds = content
        use_shell_default = True
    elif isinstance(content, dict):
        cmd = content.get("cmd")
        if isinstance(cmd, (str, list)):
            cmds = cmd if isinstance(cmd, list) else [cmd]
        else:
            raise YPMSError("shell guide: 'cmd' must be str or list")
        cwd = content.get("cwd")
        check = bool(content.get("check", True))
        extra_env = {str(k): str(v) for k, v in content.get("env", {}).items()}
        if "shell" in content:
            use_shell_default = bool(content["shell"])
        else:
            use_shell_default = None
    else:
        raise YPMSError("shell guide: invalid content")

    env = os.environ.copy()
    env.update({
        "YPMS_ENV_DIR": env_dir,
        "OS": _norm_os(),
        "ARCH": _norm_arch(),
        "PACKAGE_REF": str(context.get("PACKAGE_REF", "")),
        "SOURCE_NAME": str(context.get("SOURCE_NAME", "")),
        "RELEASE_ID": str(context.get("RELEASE_ID", "")),
    })
    env.update(extra_env)

    last_code = 0
    for c in cmds:
        if isinstance(c, list):
            args = [ _subst(str(a), env_dir=env_dir, ctx=context) for a in c ]
            shell_flag = False if use_shell_default is None else use_shell_default
            ui.set_step(step_no or 0, f"Running: {' '.join(args) if args else 'command'}") if ui else None
            proc = subprocess.run(args, shell=shell_flag, cwd=cwd, env=env, check=check)
            last_code = proc.returncode
        else:
            cmd_str = _subst(str(c), env_dir=env_dir, ctx=context)
            shell_flag = True if use_shell_default is None else use_shell_default
            ui.set_step(step_no or 0, f"Running: {cmd_str}") if ui else None
            proc = subprocess.run(cmd_str, shell=shell_flag, cwd=cwd, env=env, check=check)
            last_code = proc.returncode

    return "" if last_code == 0 else str(last_code)

def _exec_step_remove_file(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any], ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step.get("content")
    missing_ok = True
    paths: List[str] = []
    if isinstance(content, str):
        paths = [content]
    elif isinstance(content, list):
        paths = [str(p) for p in content]
    elif isinstance(content, dict):
        if "path" in content:
            paths = [str(content["path"])]
        elif "paths" in content and isinstance(content["paths"], list):
            paths = [str(p) for p in content["paths"]]
        else:
            raise YPMSError("remove-file guide: need 'path' or 'paths'")
        missing_ok = bool(content.get("missing_ok", True))
    else:
        raise YPMSError("remove-file guide: invalid content")

    removed = 0
    for p in paths:
        spath = _subst(p, env_dir=env_dir, ctx=context)
        if not os.path.exists(spath):
            if missing_ok:
                continue
            raise YPMSError(f"remove-file: not found: {spath}")
        try:
            if os.path.isdir(spath) and not os.path.islink(spath):
                shutil.rmtree(spath, ignore_errors=False)
            else:
                os.remove(spath)
            removed += 1
        except Exception as e:
            if missing_ok:
                continue
            raise YPMSError(f"remove-file: failed to remove {spath}: {e}") from e
    ui.set_step(step_no or 0, f"removed {removed} item(s)") if ui else None
    return f"removed={removed}"

def _exec_step_install_package(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any],
                               mgr: "YPMSManager", env: str, ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step.get("content")
    items: List[Tuple[Optional[str], str, Optional[str]]] = []
    if isinstance(content, str):
        items.append(YPMSManager._parse_dep(content))
    elif isinstance(content, list):
        for it in content:
            items.append(YPMSManager._parse_dep(it))
    elif isinstance(content, dict):
        items.append(YPMSManager._parse_dep(content))
    else:
        raise YPMSError("install-package guide: invalid content")
    db = mgr._db_load()
    env_pkgs = db.get("envs", {}).get(env, {})
    for src_name, pref, ver in items:
        key = mgr._db_key(src_name or (context.get("SOURCE_NAME") or "yopr"), pref)
        if key in env_pkgs:
            continue
        mgr._install_execute(pref, env=env, version=ver, source_name=(src_name or context.get("SOURCE_NAME")), explicit=False, print_header=True)
    return ""

def _exec_step_uninstall_package(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any],
                                 mgr: "YPMSManager", env: str, ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    content = step.get("content")
    items: List[Tuple[Optional[str], str, Optional[str]]] = []
    if isinstance(content, str):
        items.append(YPMSManager._parse_dep(content))
    elif isinstance(content, list):
        for it in content:
            items.append(YPMSManager._parse_dep(it))
    elif isinstance(content, dict):
        items.append(YPMSManager._parse_dep(content))
    else:
        raise YPMSError("uninstall-package guide: invalid content")
    db = mgr._db_load()
    env_pkgs = db.get("envs", {}).get(env, {})
    for src_name, pref, _ver in items:
        key = mgr._db_key(src_name or (context.get("SOURCE_NAME") or "yopr"), pref)
        if key not in env_pkgs:
            continue
        # dependents check
        tsrc = (src_name or (context.get("SOURCE_NAME") or "yopr"))
        dependents = mgr._find_dependents(env=env, target_source=tsrc, target_pref=pref)
        if dependents and not getattr(mgr, "_force_flag", False):
            raise YPMSError(f"uninstall blocked: other packages depend on {tsrc}/{pref}")
        elif dependents and getattr(mgr, "_force_flag", False):
            ui.set_step(step_no or 0, f"WARNING: dependents exist; proceeding due to --force") if ui else None
        ui.set_step(step_no or 0, f"Uninstalling {pref}", style="red") if ui else None
        try:
            mgr.run(pref, "uninstall", env=env, version=None, source_name=(src_name or context.get("SOURCE_NAME")))
        except YPMSError:
            pass
    return ""

def _extract_add_repo_names(guide_obj: Any) -> List[str]:
    names: List[str] = []
    try:
        steps = _normalize_guide_to_steps(guide_obj)
    except Exception:
        return names
    for st in steps:
        if not isinstance(st, dict):
            continue
        if st.get("type") == "add-repo":
            cont = st.get("content")
            if isinstance(cont, dict) and "name" in cont:
                names.append(str(cont["name"]))
            elif isinstance(cont, list):
                for c in cont:
                    if isinstance(c, dict) and "name" in c:
                        names.append(str(c["name"]))
            elif isinstance(cont, str):
                parts = cont.strip().split()
                if parts:
                    names.append(parts[0])
    return names

def _exec_step_add_repo(step: Dict[str, Any], *, mgr: "YPMSManager", ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    cont = step.get("content")
    entries: List[Tuple[str, str]] = []
    if isinstance(cont, dict) and "name" in cont and "url" in cont:
        entries.append((str(cont["name"]), str(cont["url"])))
    elif isinstance(cont, list):
        for e in cont:
            if isinstance(e, dict) and "name" in e and "url" in e:
                entries.append((str(e["name"]), str(e["url"])))
    elif isinstance(cont, dict):
        for k, v in cont.items():
            entries.append((str(k), str(v)))
    elif isinstance(cont, str):
        parts = cont.strip().split()
        if len(parts) >= 2:
            entries.append((parts[0], parts[1]))
    else:
        raise YPMSError("add-repo guide: invalid content")
    for name, url in entries:
        if name not in mgr.sources_map:
            mgr.add_source(name, url)
            ui.set_step(step_no or 0, f"Added repo {name}") if ui else None
    return ""

def _exec_step_remove_repo(step: Dict[str, Any], *, mgr: "YPMSManager", ui: Optional[PackageLiveUI], step_no: Optional[int]) -> str:
    cont = step.get("content")
    names: List[str] = []
    if isinstance(cont, str):
        names = [cont.strip()]
    elif isinstance(cont, list):
        names = [str(x) for x in cont]
    elif isinstance(cont, dict):
        if "name" in cont:
            names = [str(cont["name"])]
        elif "names" in cont and isinstance(cont["names"], list):
            names = [str(x) for x in cont["names"]]
    else:
        raise YPMSError("remove-repo guide: invalid content")
    for name in names:
        if name in mgr.sources_map:
            mgr.remove_source(name)
            ui.set_step(step_no or 0, f"Removed repo {name}") if ui else None
    return ""


def _execute_guide_steps(*, guide_obj: Dict[str, Any], env_dir: str,
                         pkg_ctx: Dict[str, Any],
                         mgr: Optional["YPMSManager"] = None,
                         env: Optional[str] = None,
                         ui: Optional[PackageLiveUI] = None,
                         force: bool = False) -> str:
    steps = _normalize_guide_to_steps(guide_obj)
    ran_any = False
    last_result = ""
    for idx, step in enumerate(steps, 1):
        if not isinstance(step, dict):
            raise YPMSError("Guide step must be a dict")
        if not _when_matches(step.get("when")):
            continue
        gtype = step.get("type")
        if gtype == "license-agreement-url":
            last_result = _exec_step_license_agreement(step, env_dir=env_dir, context=pkg_ctx, ui=ui, step_no=idx)
        elif gtype == "python":
            last_result = _exec_step_python(step, env_dir=env_dir, context=pkg_ctx, ui=ui, step_no=idx)
        elif gtype == "shell":
            last_result = _exec_step_shell(step, env_dir=env_dir, context=pkg_ctx, ui=ui, step_no=idx)
        elif gtype in ["download-file", "download-only"]:
            last_result = _exec_step_download_file(step, env_dir=env_dir, ui=ui, step_no=idx)
        elif gtype == "remove-file":
            last_result = _exec_step_remove_file(step, env_dir=env_dir, context=pkg_ctx, ui=ui, step_no=idx)
        elif gtype == "install-package":
            if mgr is None or env is None:
                raise YPMSError("install-package step requires manager and env")
            last_result = _exec_step_install_package(step, env_dir=env_dir, context=pkg_ctx, mgr=mgr, env=env, ui=ui, step_no=idx)
        elif gtype == "uninstall-package":
            if mgr is None or env is None:
                raise YPMSError("uninstall-package step requires manager and env")
            mgr._force_flag = force
            last_result = _exec_step_uninstall_package(step, env_dir=env_dir, context=pkg_ctx, mgr=mgr, env=env, ui=ui, step_no=idx)
        elif gtype == "add-repo":
            if mgr is None:
                raise YPMSError("add-repo step requires manager")
            last_result = _exec_step_add_repo(step, mgr=mgr, ui=ui, step_no=idx)
        elif gtype == "remove-repo":
            if mgr is None:
                raise YPMSError("remove-repo step requires manager")
            last_result = _exec_step_remove_repo(step, mgr=mgr, ui=ui, step_no=idx)
        elif gtype == "none":
            if ui: ui.set_step(idx, "(none)")
            last_result = last_result
        else:
            raise YPMSError(f"Unsupported guide type: {gtype}")
        ran_any = True
    if not ran_any:
        raise YPMSError("No guide step matched current platform/arch")
    return last_result


# ---- Manager --------------------------------------------------- #

class YPMSManager:
    def __init__(self, ypms_dir: str = YPMS_DIR, envs_dir: str = YPMS_ENVS_DIR):
        self.ypms_dir = ypms_dir
        self.envs_dir = envs_dir
        os.makedirs(self.ypms_dir, exist_ok=True)
        os.makedirs(self.envs_dir, exist_ok=True)
        os.makedirs(CACHE_DIR, exist_ok=True)

        if not os.path.exists(SOURCES_PATH):
            with open(SOURCES_PATH, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_SOURCES, f, ensure_ascii=False, indent=2)

        self.sources_map = self._load_sources()
        self._source_cache: Dict[str, YPMSSource] = {}

        if not os.path.exists(INSTALLED_DB_PATH):
            with open(INSTALLED_DB_PATH, "w", encoding="utf-8") as f:
                json.dump({"envs": {}}, f, ensure_ascii=False, indent=2)

    # ---- DB helpers ----
    def _db_load(self) -> Dict[str, Any]:
        try:
            with open(INSTALLED_DB_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {"envs": {}}

    def _db_save(self, obj: Dict[str, Any]) -> None:
        with open(INSTALLED_DB_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)

    def _db_key(self, source: Optional[str], package_ref: str) -> str:
        return f"{source or 'yopr'}:{package_ref}"

    def _db_mark_installed(self, *, env: str, source: str, package_ref: str,
                           version: str, explicit: bool) -> None:
        db = self._db_load()
        envs = db.setdefault("envs", {})
        e = envs.setdefault(env, {})
        key = self._db_key(source, package_ref)
        e[key] = {
            "source": source,
            "package": package_ref,
            "version": version,
            "explicit": bool(explicit),
            "installed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self._db_save(db)

    def _db_mark_uninstalled(self, *, env: str, source: str, package_ref: str) -> None:
        db = self._db_load()
        envs = db.get("envs", {})
        e = envs.get(env, {})
        key = self._db_key(source, package_ref)
        if key in e:
            e.pop(key)
            self._db_save(db)

    def is_installed(self, *, env: str, source: str, package_ref: str) -> bool:
        db = self._db_load()
        envs = db.get("envs", {})
        e = envs.get(env, {})
        return self._db_key(source, package_ref) in e

    def list_installed(self, env: Optional[str] = None) -> Dict[str, Any]:
        db = self._db_load()
        all_envs = db.get("envs", {})
        if env:
            return {env: all_envs.get(env, {})}
        return all_envs

    # ---- Sources ops ----
    def _load_sources(self) -> Dict[str, str]:
        try:
            with open(SOURCES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise YPMSError("sources.json must be a JSON object mapping name -> ypms.json URL")
                return {str(k): str(v) for k, v in data.items()}
        except FileNotFoundError:
            return {}
    def save_sources(self) -> None:
        with open(SOURCES_PATH, "w", encoding="utf-8") as f:
            json.dump(self.sources_map, f, ensure_ascii=False, indent=2)

    def list_sources(self) -> Dict[str, str]:
        return dict(self.sources_map)

    def add_source(self, name: str, ypms_json_url: str) -> None:
        self.sources_map[name] = ypms_json_url
        self.save_sources()
        self._source_cache.pop(name, None)

    def remove_source(self, name: str) -> None:
        if name in self.sources_map:
            self.sources_map.pop(name)
            self.save_sources()
        self._source_cache.pop(name, None)

    # ---- Source access ----
    def _get_source(self, source_name: Optional[str], *, force_refresh: bool = False) -> YPMSSource:
        if not source_name:
            if "yopr" in self.sources_map:
                source_name = "yopr"
            else:
                if not self.sources_map:
                    raise YPMSError("No sources configured.")
                source_name = sorted(self.sources_map.keys())[0]

        if not force_refresh and source_name in self._source_cache:
            return self._source_cache[source_name]

        url = self.sources_map.get(source_name)
        if not url:
            raise YPMSError(f"Unknown source: {source_name}")

        src = YPMSSource(source_name, url, force_refresh=force_refresh)
        self._source_cache[source_name] = src
        return src

    # ---- Env utils ----
    def ensure_env_dir(self, env_id: str) -> str:
        env_dir = os.path.join(self.envs_dir, env_id)
        os.makedirs(env_dir, exist_ok=True)
        return env_dir

    def list_envs(self) -> Dict[str, str]:
        if not os.path.isdir(self.envs_dir):
            return {}
        envs = {}
        for name in sorted(os.listdir(self.envs_dir)):
            path = os.path.join(self.envs_dir, name)
            if os.path.isdir(path):
                envs[name] = path
        return envs

    # ---- Packages & Guides ----
    def list_packages(self, source_name: Optional[str] = None) -> Dict[str, Any]:
        src = self._get_source(source_name)
        return src.fetch_index()

    def package_info(self, package_ref: str, source_name: Optional[str] = None) -> Dict[str, Any]:
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        info = src.fetch_package_info(user, pkg)
        info["_source"] = src.source_name
        info["_package_ref"] = f"{user}/{pkg}"
        return info

    def _get_release_info(self, src: YPMSSource, pkg_info: Dict[str, Any],
                          version_tag: Optional[str], *, force_refresh: bool = False) -> Tuple[str, Dict[str, Any]]:
        resolved = src.resolve_release_tag(pkg_info, version_tag)
        rel = src.fetch_release_info(pkg_info, resolved, force_refresh=force_refresh)
        return resolved, rel

    @staticmethod
    def _split_pkg_ref(ref: str) -> Tuple[str, str]:
        if "/" not in ref:
            raise YPMSError("Package ref must be 'USER/PACKAGE'")
        user, pkg = ref.split("/", 1)
        user = user.strip()
        pkg = pkg.strip()
        if not user or not pkg:
            raise YPMSError("Invalid package ref")
        return user, pkg

    @staticmethod
    def _parse_dep(dep: Union[str, Dict[str, Any]]) -> Tuple[Optional[str], str, Optional[str]]:
        if isinstance(dep, str):
            src_name = None
            body = dep.strip()
            if ":" in body and "/" in body.split(":", 1)[1]:
                src_name, body = body.split(":", 1)
                src_name = src_name.strip() or None
            if "@" in body:
                pref, ver = body.split("@", 1)
                return src_name, pref.strip(), (ver.strip() or None)
            return src_name, body.strip(), None
        if isinstance(dep, dict):
            pref = dep.get("package")
            ver = dep.get("version")
            sname = dep.get("source")
            if not pref or not isinstance(pref, str):
                raise YPMSError("Invalid dependency object: missing 'package'")
            return (str(sname).strip() if sname else None), pref.strip(), (str(ver).strip() if ver else None)
        raise YPMSError("Invalid dependency entry")

    def _find_dependents(self, *, env: str, target_source: str, target_pref: str) -> List[Dict[str, str]]:
        """Return list of dependents that require target_pref from target_source."""
        res: List[Dict[str, str]] = []
        db = self._db_load().get("envs", {}).get(env, {})
        for meta in db.values():
            dep_src_name = meta["source"]
            pkg_ref = meta["package"]  # "user/pkg"
            version = meta.get("version")
            # read its release and deps
            u, p = self._split_pkg_ref(pkg_ref)
            src = self._get_source(dep_src_name)
            pkg_info = src.fetch_package_info(u, p)
            rel = src.fetch_release_info(pkg_info, version)
            for d in rel.get("release.depends", []):
                ds, dr, dv = self._parse_dep(d)
                ds = ds or target_source
                if ds == target_source and dr == target_pref:
                    # resolve requested version (alias) if any
                    req_version = None
                    if dv:
                        dep_user, dep_pkg = self._split_pkg_ref(dr)
                        dep_src_obj = self._get_source(ds)
                        dep_pkg_info = dep_src_obj.fetch_package_info(dep_user, dep_pkg)
                        req_version = dep_src_obj.resolve_release_tag(dep_pkg_info, dv)
                    res.append({
                        "dependent_source": dep_src_name,
                        "dependent_package": pkg_ref,
                        "dependent_version": version or "",
                        "required_version": req_version or "",
                    })
        return res

    def _check_update_compat(self, *, env: str, target_source: str, target_pref: str, new_version: str) -> List[str]:
        """Return list of blocker messages (empty if compatible)."""
        blockers: List[str] = []
        for d in self._find_dependents(env=env, target_source=target_source, target_pref=target_pref):
            req = d["required_version"]
            # No version specified or 'latest' means accept anything
            if not req or req.lower() == "latest" or req == "*":
                continue
            if req != new_version:
                blockers.append(
                    f"{d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
                    f"requires {target_source}/{target_pref}@{req}, but planned {new_version}"
                )
        return blockers

    # ---- Internal installer using Live UI -----------------------
    def _install_execute(self, package_ref: str, env: str = "default", version: Optional[str] = None,
                         source_name: Optional[str] = None, *, explicit: bool = True, print_header: bool = True, force: bool = False) -> str:
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        env_dir = self.ensure_env_dir(env)
        yp = YPMSPackage(src, user, pkg)
        resolved, rel = self._get_release_info(src, yp.pkg_info, version)
        pkg_ctx = {
            "PACKAGE_REF": f"{user}/{pkg}",
            "SOURCE_NAME": src.source_name,
            "RELEASE_ID": resolved,
        }

        # Start per-package Live UI
        header = f"(>) Installing {package_ref}@{resolved}"
        ui = PackageLiveUI(header, header_style="bold cyan") if print_header else None
        self._force_flag = force
        try:
            guide = rel.get("release.guides", {}).get("install")
            if not guide:
                raise YPMSError("Guide 'install' not defined for this package")
            _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx, mgr=self, env=env, ui=ui, force=force)
            # Mark installed
            self._db_mark_installed(env=env, source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved, explicit=explicit)
            # Autoinstall dependencies (quiet header per dep)
            for dep in rel.get("release.depends", []):
                dep_src, dep_ref, dep_ver = self._parse_dep(dep)
                if self.is_installed(env=env, source=(dep_src or src.source_name), package_ref=dep_ref):
                    continue
                self._install_execute(dep_ref, env=env, version=dep_ver, source_name=(dep_src or src.source_name), explicit=False, print_header=True)
            # Replace header & clear steps
            if ui:
                ui.set_header(f"(>) Installed {package_ref}@{resolved}", style="bold green")
                ui.clear_steps_keep_header()
        finally:
            if ui:
                ui.stop()
        self._force_flag = False

        return env_dir

    # ---- Planning & high-level actions --------------------------
    @dataclass
    class _OpItem:
        kind: str  # 'install' or 'update' or 'target'
        source: str
        package_ref: str
        version: Optional[str]
        footnote: Optional[int] = None

    def _build_operation_plan(self, *, package_ref: str, env: str, version: Optional[str], source_name: Optional[str]) -> Tuple[List["_OpItem"], Dict[int, str]]:
        ops: List[YPMSManager._OpItem] = []
        notes: Dict[int, str] = {}
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        _ = src.fetch_index()
        yp = YPMSPackage(src, user, pkg)
        resolved, rel = self._get_release_info(src, yp.pkg_info, version)

        known_sources = set(self.sources_map.keys())
        providers: Dict[str, str] = {}
        dep_entries: List[Tuple[Optional[str], str, Optional[str]]] = []
        for d in rel.get("release.depends", []):
            dep_entries.append(self._parse_dep(d))

        dep_rel_infos: List[Tuple[Optional[str], str, Optional[str], Dict[str, Any], str]] = []
        for dep_src, dep_ref, dep_ver in dep_entries:
            dep_user, dep_pkg = self._split_pkg_ref(dep_ref)
            dep_src_name = dep_src or src.source_name
            dep_src_obj = self._get_source(dep_src_name)
            dep_pkg_info = dep_src_obj.fetch_package_info(dep_user, dep_pkg)
            dep_resolved = dep_src_obj.resolve_release_tag(dep_pkg_info, dep_ver)
            dep_rel_info = dep_src_obj.fetch_release_info(dep_pkg_info, dep_resolved)
            add_names = _extract_add_repo_names(dep_rel_info.get("release.guides", {}).get("install"))
            for nm in add_names:
                providers[nm] = f"{dep_src_name}/{dep_ref}@{dep_resolved}"
            dep_rel_infos.append((dep_src_name, dep_ref, dep_resolved, dep_rel_info, dep_src_name))

        for dep_src_name, dep_ref, dep_resolved, _dep_rel, _ in dep_rel_infos:
            kind = "update" if self.is_installed(env=env, source=dep_src_name, package_ref=dep_ref) else "install"
            foot = None
            if dep_src_name not in known_sources and dep_src_name in providers:
                foot = len(notes) + 1
                notes[foot] = f"This package repository will be installed by the following package: {providers[dep_src_name]}"
            ops.append(YPMSManager._OpItem(kind=kind, source=dep_src_name, package_ref=dep_ref, version=dep_resolved, footnote=foot))

        # target: already installed ? -> update or noop
        target_key = self._db_key(src.source_name, f"{user}/{pkg}")
        installed = self._db_load().get("envs", {}).get(env, {}).get(target_key)
        if installed:
            if installed.get("version") == resolved:
                # nothing to do (no target op)
                pass
            else:
                ops.append(YPMSManager._OpItem(kind="update", source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved))
        else:
            ops.append(YPMSManager._OpItem(kind="target", source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved))
        return ops, notes

    def install(self, package_ref: str, env: str = "default", version: Optional[str] = None,
                source_name: Optional[str] = None, *, explicit: bool = True,
                assume_yes: bool = False, force: bool = False) -> str:
        try:
            self._get_source(source_name).fetch_index()
        except Exception:
            pass
        _p_action("Updated Local Index")

        ops, notes = self._build_operation_plan(package_ref=package_ref, env=env, version=version, source_name=source_name)
        _p_action("Built operation information")

        print()

        if not ops:
            _p_info("Already installed. Nothing to do.")
            return self.ensure_env_dir(env)
        _p_info("This action will change the package state as follows:")
        for i, it in enumerate(ops, 1):
            _print_plan_item(i, it.kind, it.source, it.package_ref, it.version, it.footnote)
        if notes:
            _p_info("Supplementary information:")
            for k, v in notes.items():
                print(f"*{k}: {v}")

        if not assume_yes:
            _p_question("Continue? [Y/n] ")
            ans = sys.stdin.readline().strip().lower()
            if ans not in ("", "y", "yes"):
                print("Aborted.")
                return ""

        print()

        env_dir = self.ensure_env_dir(env)
        for it in ops:
            if it.kind in ("install", "target"):
                self._install_execute(it.package_ref, env=env, version=it.version, source_name=it.source, explicit=(it.kind == "target" and explicit), print_header=True)
            elif it.kind == "update":
                # dependency compatibility check
                incompat = self._check_update_compat(env=env, target_source=it.source, target_pref=it.package_ref, new_version=it.version or "")
                if incompat:
                    if not force:
                        if _CONSOLE:
                            _CONSOLE.print("[bold red]Blocked by dependency constraints:[/]")
                            for m in incompat:
                                _CONSOLE.print(f"  - {m}")
                        else:
                            print("Blocked by dependency constraints:")
                            print("\n".join(f"  - {m}" for m in incompat))
                        return env_dir
                    else:
                        # show warnings and confirm
                        if _CONSOLE:
                            _CONSOLE.print("[bold yellow]WARNING: dependency constraints may be violated:[/]")
                            for m in incompat:
                                _CONSOLE.print(f"  - {m}")
                        else:
                            print("WARNING: dependency constraints may be violated:")
                            print("\n".join(f"  - {m}" for m in incompat))
                        if not assume_yes:
                            _p_question("Continue anyway? [y/N] ")
                            ans = sys.stdin.readline().strip().lower()
                            if ans not in ("y", "yes"):
                                print("Aborted.")
                                return env_dir
                try:
                    header_ui = PackageLiveUI(f"(>) Updating {it.package_ref}@{it.version}", header_style="bold yellow")
                    _ = self.run(package_ref=it.package_ref, guide_name="update", env=env, version=it.version, source_name=it.source)
                    header_ui.set_header(f"(>) Updated {it.package_ref}@{it.version}", style="bold green")
                    header_ui.clear_steps_keep_header()
                    header_ui.stop()
                except YPMSError as e:
                    if "not defined" in str(e):
                        # no update guide — skip silently
                        pass
                    else:
                        raise
        return env_dir

    def run(self, package_ref: str, guide_name: str, env: str = "default", version: Optional[str] = None,
            source_name: Optional[str] = None, *, force: bool = False, assume_yes: bool = False) -> str:
        user, pkg = self._split_pkg_ref(package_ref)
        env_dir = self.ensure_env_dir(env)

        if guide_name == "uninstall":
            env_db = self._db_load().get("envs", {}).get(env, {})
            installed_meta = None
            for _k, meta in env_db.items():
                if meta.get("package") == f"{user}/{pkg}":
                    installed_meta = meta
                    break
            if not installed_meta:
                _p_info("Not installed. Nothing to do.")
                return ""
            if not source_name:
                source_name = installed_meta.get("source")

        src = self._get_source(source_name)
        yp = YPMSPackage(src, user, pkg)

        resolved, rel = self._get_release_info(src, yp.pkg_info, version)
        pkg_ctx = {
            "PACKAGE_REF": f"{user}/{pkg}",
            "SOURCE_NAME": src.source_name,
            "RELEASE_ID": resolved,
        }
        guides = rel.get("release.guides", {})
        guide = guides.get(guide_name)
        if not guide:
            raise YPMSError(f"Guide '{guide_name}' not defined for release '{resolved}'.")

        if guide_name == "uninstall":
            dependents = self._find_dependents(env=env, target_source=src.source_name, target_pref=f"{user}/{pkg}")
            if dependents:
                if not force:
                    if _CONSOLE:
                        _CONSOLE.print("[bold red]Blocked: other packages depend on this package:[/]")
                        for d in dependents:
                            _CONSOLE.print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
                                           f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
                    else:
                        print("Blocked: other packages depend on this package:")
                        for d in dependents:
                            print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
                                  f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
                    raise YPMSError("uninstall blocked by dependents")
                else:
                    if _CONSOLE:
                        _CONSOLE.print("[bold yellow]WARNING: other packages depend on this package:[/]")
                        for d in dependents:
                            _CONSOLE.print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
                                           f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
                    else:
                        print("WARNING: other packages depend on this package:")
                        for d in dependents:
                            print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
                                  f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
                    if not assume_yes:
                        _p_question("Continue anyway? [y/N] ")
                        ans = sys.stdin.readline().strip().lower()
                        if ans not in ("y", "yes"):
                            print("Aborted.")
                            return env_dir

        ui = PackageLiveUI(f"(>) Running guide '{guide_name}' for {package_ref}@{resolved}", header_style="bold magenta")
        try:
            dest = _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx, mgr=self, env=env, ui=ui)
            if guide_name == "uninstall":
                self._db_mark_uninstalled(env=env, source=src.source_name, package_ref=f"{user}/{pkg}")
            ui.set_header(f"(>) Finished guide '{guide_name}' for {package_ref}@{resolved}", style="bold green")
            ui.clear_steps_keep_header()
        finally:
            ui.stop()
        return dest

    # ---- Refresh/Upgrade/Autoremove ----
    def refresh_sources(self) -> None:
        vlog("refresh: clearing cache...")
        _clear_all_cache()
        self._source_cache.clear()
        for name, _url in self.sources_map.items():
            src = self._get_source(name, force_refresh=True)
            _ = src.fetch_index(force_refresh=True)

    def upgrade(self, env: Optional[str] = None, *, force: bool = False) -> List[str]:
        self.refresh_sources()
        results: List[str] = []
        installed_by_env = self.list_installed(env=env)
        for env_name, pkgs in installed_by_env.items():
            for _key, meta in pkgs.items():
                src_name = meta["source"]
                package_ref = meta["package"]
                version = meta.get("version")
                try:
                    res = self.run(package_ref=package_ref, guide_name="update",
                                   env=env_name, version=version, source_name=src_name, force=force, assume_yes=True)
                    results.append(f"{env_name}:{package_ref} -> {res}")
                except YPMSError as e:
                    if "not defined" in str(e):
                        continue
                    results.append(f"{env_name}:{package_ref} [ERROR] {e}")
        return results

    def autoremove(self, env: Optional[str] = None, *, force: bool = False) -> List[str]:
        results: List[str] = []
        installed_by_env = self.list_installed(env=env)
        for env_name, pkgs in installed_by_env.items():
            targets = [meta for meta in pkgs.values() if not meta.get("explicit")]
            for meta in targets:
                src_name = meta["source"]
                package_ref = meta["package"]
                version = meta.get("version")
                try:
                    res = self.run(package_ref=package_ref, guide_name="uninstall",
                                   env=env_name, version=version, source_name=src_name, force=force, assume_yes=True)
                    results.append(f"{env_name}:{package_ref} -> {res}")
                except YPMSError as e:
                    if "not defined" in str(e):
                        continue
                    results.append(f"{env_name}:{package_ref} [ERROR] {e}")
        return results


# ---- CLI ------------------------------------------------------- #

BUILTIN_CMDS = {"list", "info", "install", "envs", "sources", "refresh", "upgrade", "autoremove"}

def _build_full_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ypms",
        description="YPMS - YPSH Package Manager (CLI & Library)",
    )
    p.add_argument("-s", "--source", help="Source name (default: yopr or first configured)")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    sub = p.add_subparsers(dest="cmd")

    sp_list = sub.add_parser("list", help="List packages in source")
    sp_list.add_argument("-s", "--source", help="Source name (override global)")

    sp_info = sub.add_parser("info", help="Show package info")
    sp_info.add_argument("package", help="Package ref USER/PACKAGE")
    sp_info.add_argument("-s", "--source", help="Source name (override global)")
    sp_info.add_argument("--version", help="Release tag or alias to resolve and show")

    sp_inst = sub.add_parser("install", help="Install a package via its 'install' guide")
    sp_inst.add_argument("package", help="Package ref USER/PACKAGE")
    sp_inst.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
    sp_inst.add_argument("--env", default="default", help="Environment ID (default: default)")
    sp_inst.add_argument("-s", "--source", help="Source name (override global)")
    sp_inst.add_argument("-y", "--yes", action="store_true", help="Assume yes to prompts and run non-interactively")
    sp_inst.add_argument("-f", "--force", action="store_true", help="Force risky operations (override dependency blocks with warning)")

    sub.add_parser("envs", help="List environments")

    sp_src = sub.add_parser("sources", help="Manage sources.json")
    src_sub = sp_src.add_subparsers(dest="src_cmd", required=True)
    src_sub.add_parser("list", help="List configured sources")
    ss_add = src_sub.add_parser("add", help="Add a source")
    ss_add.add_argument("name")
    ss_add.add_argument("url", help="URL to ypms.json")
    ss_rm = src_sub.add_parser("remove", help="Remove a source")
    ss_rm.add_argument("name")

    sub.add_parser("refresh", help="Clear caches and re-fetch all sources")

    sp_up = sub.add_parser("upgrade", help="Refresh caches and run 'update' for all installed packages")
    sp_up.add_argument("--env", help="Target environment ID (default: all envs)")
    sp_up.add_argument("-f", "--force", action="store_true", help="Force risky operations")

    sp_ar = sub.add_parser("autoremove", help="Uninstall non-explicit (dependency) packages if 'uninstall' is available")
    sp_ar.add_argument("--env", help="Target environment ID (default: all envs)")
    sp_ar.add_argument("-f", "--force", action="store_true", help="Force risky operations")

    return p


def _parse_global_args(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    gp = argparse.ArgumentParser(add_help=False)
    gp.add_argument("-s", "--source")
    gp.add_argument("-v", "--verbose", action="store_true")
    gp.add_argument("-h", "--help", action="store_true")
    gargs, rest = gp.parse_known_args(argv)
    return gargs, rest


def _parse_dynamic_command_args(cmd: str, rest: List[str]) -> argparse.Namespace:
    dp = argparse.ArgumentParser(
        prog=f"ypms {cmd}",
        description=f"Run release guide '{cmd}'",
    )
    dp.add_argument("package", help="Package ref USER/PACKAGE")
    dp.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
    dp.add_argument("--env", default="default", help="Environment ID (default: default)")
    dp.add_argument("-s", "--source", help="Source name (override global)")
    dp.add_argument("-y", "--yes", action="store_true", help="Assume yes to prompts (for warnings)")
    dp.add_argument("-f", "--force", action="store_true", help="Force risky operations")
    return dp.parse_args(rest)


# ---- Built-in command handlers -------------------------------- #

def _cmd_list(mgr: YPMSManager, args: argparse.Namespace) -> int:
    pkgs = mgr.list_packages(source_name=args.source)
    for group, names in pkgs.items():
        print(f"[{group}]")
        for nm in names:
            print(f"  - {nm}")
    return 0


def _cmd_info(mgr: YPMSManager, args: argparse.Namespace) -> int:
    info = mgr.package_info(args.package, source_name=args.source)
    print(json.dumps(info, ensure_ascii=False, indent=2))
    if args.version:
        src = mgr._get_source(info.get("_source"))
        resolved = src.resolve_release_tag(info, args.version)
        url_tmpl = info["package.release.url"]
        print("\nResolved version:", resolved)
        print("Release info URL:", url_tmpl.format(RELEASE_ID=resolved))
    return 0


def _cmd_install(mgr: YPMSManager, args: argparse.Namespace) -> int:
    _ = mgr.install(args.package, env=args.env, version=args.version, source_name=args.source, explicit=True, assume_yes=args.yes, force=args.force)
    return 0


def _cmd_envs(mgr: YPMSManager, _args: argparse.Namespace) -> int:
    envs = mgr.list_envs()
    if not envs:
        print("(no environments yet)")
        return 0
    for name, path in envs.items():
        print(f"{name}: {path}")
    return 0


def _cmd_sources(mgr: YPMSManager, args: argparse.Namespace) -> int:
    if args.src_cmd == "list":
        srcs = mgr.list_sources()
        for name, url in srcs.items():
            print(f"{name}: {url}")
        return 0
    if args.src_cmd == "add":
        mgr.add_source(args.name, args.url)
        print(f"Added source '{args.name}' -> {args.url}")
        return 0
    if args.src_cmd == "remove":
        mgr.remove_source(args.name)
        print(f"Removed source '{args.name}'")
        return 0
    raise YPMSError("Unknown sources subcommand")

def _cmd_refresh(mgr: YPMSManager, _args: argparse.Namespace) -> int:
    mgr.refresh_sources()
    _p_success("cache cleared and sources re-fetched.")
    return 0

def _cmd_upgrade(mgr: YPMSManager, args: argparse.Namespace) -> int:
    results = mgr.upgrade(env=args.env, force=args.force)
    print("\n".join(results) if results else "(nothing to upgrade)")
    return 0

def _cmd_autoremove(mgr: YPMSManager, args: argparse.Namespace) -> int:
    results = mgr.autoremove(env=args.env, force=args.force)
    print("\n".join(results) if results else "(nothing to autoremove)")
    return 0


# ---- main ------------------------------------------------------ #

def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    gargs, rest = _parse_global_args(argv)
    global _VERBOSE
    _VERBOSE = bool(gargs.verbose) or (os.environ.get("YPMS_DEBUG") == "1")
    if _VERBOSE:
        vlog("argv:", sys.argv)
        vlog("globals:", {"source": gargs.source, "verbose": gargs.verbose})

    full_parser = _build_full_parser()
    if gargs.help or not rest:
        full_parser.print_help()
        return 0

    cmd = rest[0]
    rest_after_cmd = rest[1:]

    try:
        mgr = YPMSManager()

        if cmd in BUILTIN_CMDS:
            parsed = full_parser.parse_args(
                (["-s", gargs.source] if gargs.source else [])
                + (["-v"] if gargs.verbose else [])
                + [cmd] + rest_after_cmd
            )
            if cmd == "list":
                return _cmd_list(mgr, parsed)
            elif cmd == "info":
                return _cmd_info(mgr, parsed)
            elif cmd == "install":
                return _cmd_install(mgr, parsed)
            elif cmd == "envs":
                return _cmd_envs(mgr, parsed)
            elif cmd == "sources":
                return _cmd_sources(mgr, parsed)
            elif cmd == "refresh":
                return _cmd_refresh(mgr, parsed)
            elif cmd == "upgrade":
                return _cmd_upgrade(mgr, parsed)
            elif cmd == "autoremove":
                return _cmd_autoremove(mgr, parsed)
            else:
                full_parser.print_help()
                return 2

        dyn_args = _parse_dynamic_command_args(cmd, rest_after_cmd)
        if not getattr(dyn_args, "source", None):
            dyn_args.source = gargs.source

        try:
            mgr.run(
                package_ref=dyn_args.package,
                guide_name=cmd,
                env=dyn_args.env,
                version=dyn_args.version,
                source_name=dyn_args.source,
                force=getattr(dyn_args, "force", False),
                assume_yes=getattr(dyn_args, "yes", False),
            )
            return 0
        except YPMSError as e:
            print(f"[YPMS ERROR] {e}", file=sys.stderr)
            full_parser.print_help()
            return 2

    except YPMSError as e:
        print(f"[YPMS ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
