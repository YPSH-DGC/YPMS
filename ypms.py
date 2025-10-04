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
from typing import Dict, Any, Optional, Tuple, List, Union

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False

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
    """Verbose logger (enabled by -v or YPMS_DEBUG=1)."""
    if _VERBOSE:
        print("[DEBUG]", *msg, file=sys.stderr)


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


def _http_download(url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    vlog("DOWNLOAD:", url, "->", dest_path)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp, open(dest_path, "wb") as f:
            if resp.status != 200:
                raise YPMSError(f"HTTP {resp.status} for {url}")
            while True:
                chunk = resp.read(1024 * 64)
                if not chunk:
                    break
                f.write(chunk)
        vlog("download-done:", dest_path, "size=", os.path.getsize(dest_path))
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
    """A single package source (repository)."""

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

    # URLs
    def _index_url(self) -> str:
        url = f"{self.config.base_url}{self.config.path_index}"
        vlog("index-url:", url)
        return url

    def _package_url(self, user_id: str, package_id: str) -> str:
        url = f"{self.config.base_url}{self.config.path_package}".format(
            USER_ID=user_id, PACKAGE_ID=package_id
        )
        vlog("package-url:", url)
        return url

    # Fetchers
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
    """Represents a package in a given source."""

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
    """
    Accepts either:
      - {"type": "...", "content": {...}, "when": {...}}
      - {"steps": [ {...}, {...} ]}
    Returns a list of step dicts.
    """
    if not isinstance(guide_obj, dict):
        raise YPMSError("Guide object must be a dict")
    if "steps" in guide_obj:
        steps = guide_obj["steps"]
        if not isinstance(steps, list):
            raise YPMSError("Guide.steps must be a list")
        return steps
    return [guide_obj]

def _exec_step_download_file(step: Dict[str, Any], *, env_dir: str) -> str:
    content = step["content"]
    dest_template: str = content["dest"]
    url: str = content["url"]
    dest_path = dest_template.replace("{YPMS_ENV_DIR}", env_dir)
    vlog("guide: download-file", {"url": url, "dest": dest_path})
    _http_download(url, dest_path)
    return dest_path

def _exec_step_python(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any]) -> str:
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
        vlog("guide: python", {"cwd": cwd, "code_len": len(code)})
        exec(compile(code, "<ypms_guide_python>", "exec"), g, l)
    finally:
        if old_cwd is not None:
            os.chdir(old_cwd)

    result_path = l.get("RESULT_PATH") or g.get("RESULT_PATH")
    return str(result_path) if result_path else ""

def _exec_step_shell(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any]) -> str:
    """
    Execute external command(s).
    content:
      - "echo hello"                       (str -> run via shell)
      - ["echo hello", "whoami"]           (list[str] -> each via shell)
      - {"cmd": "echo hi", "cwd": "...", "env": {...}, "shell": true, "check": true}
      - {"cmd": ["python", "-V"], "cwd": "...", "env": {...}, "check": true}
      - {"cmd": [["python","-V"], ["pip","--version"]]}  (list[list[str]])
    """
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
        use_shell_default = True  # treat as list of strings to shell
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

    # env build
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

    # run sequence
    last_code = 0
    for c in cmds:
        if isinstance(c, list):
            args = [ _subst(str(a), env_dir=env_dir, ctx=context) for a in c ]
            shell_flag = False if use_shell_default is None else use_shell_default
            vlog("guide: shell(list)", {"args": args, "cwd": cwd, "shell": shell_flag})
            proc = subprocess.run(args, shell=shell_flag, cwd=cwd, env=env, check=check)
            last_code = proc.returncode
        else:
            cmd_str = _subst(str(c), env_dir=env_dir, ctx=context)
            shell_flag = True if use_shell_default is None else use_shell_default
            vlog("guide: shell(str)", {"cmd": cmd_str, "cwd": cwd, "shell": shell_flag})
            proc = subprocess.run(cmd_str, shell=shell_flag, cwd=cwd, env=env, check=check)
            last_code = proc.returncode
        vlog("guide: shell -> returncode", last_code)

    return "" if last_code == 0 else str(last_code)

def _exec_step_remove_file(step: Dict[str, Any], *, env_dir: str, context: Dict[str, Any]) -> str:
    """
    Remove files/directories.
    content:
      - "/path/to/file"
      - ["a", "b", "c"]
      - {"path": "x"} or {"paths": ["x","y"], "missing_ok": true}
    """
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
        vlog("guide: remove-file", {"path": spath})
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
    return f"removed={removed}"

def _execute_guide_steps(*, guide_obj: Dict[str, Any], env_dir: str,
                         pkg_ctx: Dict[str, Any]) -> str:
    steps = _normalize_guide_to_steps(guide_obj)
    vlog("guide: start", {"steps": len(steps), "env_dir": env_dir, "ctx": pkg_ctx})
    ran_any = False
    last_result = ""
    for step in steps:
        if not isinstance(step, dict):
            raise YPMSError("Guide step must be a dict")
        matched = _when_matches(step.get("when"))
        vlog("guide: step", {"type": step.get("type"), "when": step.get("when"), "matched": matched})
        if not matched:
            continue
        gtype = step.get("type")
        if gtype == "python":
            last_result = _exec_step_python(step, env_dir=env_dir, context=pkg_ctx)
        elif gtype == "shell":
            last_result = _exec_step_shell(step, env_dir=env_dir, context=pkg_ctx)
        elif gtype in ["download-file", "download-only"]:
            last_result = _exec_step_download_file(step, env_dir=env_dir)
        elif gtype == "remove-file":
            last_result = _exec_step_remove_file(step, env_dir=env_dir, context=pkg_ctx)
        elif gtype == "none":
            last_result = last_result  # do nothing
        else:
            raise YPMSError(f"Unsupported guide type: {gtype}")
        ran_any = True
    if not ran_any:
        raise YPMSError("No guide step matched current platform/arch")
    return last_result


# ---- Manager --------------------------------------------------- #

class YPMSManager:
    """High-level manager for sources, packages, and environments."""

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

    def _db_key(self, source: str, package_ref: str) -> str:
        return f"{source}:{package_ref}"

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

        vlog("get-source:", {"name": source_name, "url": url, "force_refresh": force_refresh})
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
            raise YPMSError("Package ref must be 'USER/PACKAGE', e.g., 'ypsh/hello-world'")
        user, pkg = ref.split("/", 1)
        user = user.strip()
        pkg = pkg.strip()
        if not user or not pkg:
            raise YPMSError("Invalid package ref")
        return user, pkg

    # ---- Dependency parsing (with cross-source) ----
    @staticmethod
    def _parse_dep(dep: Union[str, Dict[str, Any]]) -> Tuple[Optional[str], str, Optional[str]]:
        """
        Returns (source_name|None, package_ref, version_tag|None)
        Accepts:
          - "user/pkg"
          - "user/pkg@tag"
          - "source:user/pkg"
          - "source:user/pkg@tag"
          - { "package": "user/pkg", "version": "v1.2", "source": "yopr" }
        """
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
        raise YPMSError("Invalid dependency entry (must be string or object)")

    # ---- High level actions ----
    def install(self, package_ref: str, env: str = "default", version: Optional[str] = None,
                source_name: Optional[str] = None, *, explicit: bool = True) -> str:
        vlog("install: begin", {"package": package_ref, "env": env, "version": version, "source": source_name})
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        env_dir = self.ensure_env_dir(env)
        vlog("install: env_dir", env_dir)
        yp = YPMSPackage(src, user, pkg)

        resolved, rel = self._get_release_info(src, yp.pkg_info, version)
        vlog("install: resolved", {"version": resolved})
        pkg_ctx = {
            "PACKAGE_REF": f"{user}/{pkg}",
            "SOURCE_NAME": src.source_name,
            "RELEASE_ID": resolved,
        }
        guides = rel.get("release.guides", {})
        guide = guides.get("install")
        if not guide:
            raise YPMSError("Guide 'install' not defined for this package")
        dest = _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx)

        self._db_mark_installed(env=env, source=src.source_name,
                                package_ref=f"{user}/{pkg}", version=resolved, explicit=explicit)
        vlog("install: marked-installed", {"env": env, "source": src.source_name, "package": f"{user}/{pkg}", "version": resolved})

        for dep in rel.get("release.depends", []):
            vlog("install: dep", dep)
            dep_src, dep_ref, dep_ver = self._parse_dep(dep)
            self.install(dep_ref,
                         env=env,
                         version=dep_ver,
                         source_name=(dep_src or src.source_name),
                         explicit=False)

        return dest

    def run(self, package_ref: str, guide_name: str, env: str = "default", version: Optional[str] = None,
            source_name: Optional[str] = None) -> str:
        vlog("run: begin", {"package": package_ref, "guide": guide_name, "env": env, "version": version, "source": source_name})
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        env_dir = self.ensure_env_dir(env)
        yp = YPMSPackage(src, user, pkg)

        resolved, rel = self._get_release_info(src, yp.pkg_info, version)
        vlog("run: resolved", {"version": resolved})
        pkg_ctx = {
            "PACKAGE_REF": f"{user}/{pkg}",
            "SOURCE_NAME": src.source_name,
            "RELEASE_ID": resolved,
        }
        guides = rel.get("release.guides", {})
        guide = guides.get(guide_name)
        if not guide:
            raise YPMSError(f"Guide '{guide_name}' not defined for release '{resolved}'.")

        dest = _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx)

        if guide_name == "uninstall":
            self._db_mark_uninstalled(env=env, source=src.source_name, package_ref=f"{user}/{pkg}")
            vlog("run: uninstalled", {"env": env, "source": src.source_name, "package": f"{user}/{pkg}"})

        return dest

    # ---- Refresh/Upgrade/Autoremove ----
    def refresh_sources(self) -> None:
        vlog("refresh: clearing cache...")
        _clear_all_cache()
        self._source_cache.clear()
        for name, _url in self.sources_map.items():
            src = self._get_source(name, force_refresh=True)
            vlog("refresh: fetch-index", {"source": name})
            _ = src.fetch_index(force_refresh=True)

    def upgrade(self, env: Optional[str] = None) -> List[str]:
        self.refresh_sources()
        results: List[str] = []
        installed_by_env = self.list_installed(env=env)
        vlog("upgrade: installed-by-env keys", list(installed_by_env.keys()))
        for env_name, pkgs in installed_by_env.items():
            for _key, meta in pkgs.items():
                src_name = meta["source"]
                package_ref = meta["package"]
                version = meta.get("version")
                try:
                    res = self.run(package_ref=package_ref, guide_name="update",
                                   env=env_name, version=version, source_name=src_name)
                    results.append(f"{env_name}:{package_ref} -> {res}")
                except YPMSError as e:
                    if "not defined" in str(e):
                        continue
                    results.append(f"{env_name}:{package_ref} [ERROR] {e}")
        vlog("upgrade: done", {"count": len(results)})
        return results

    def autoremove(self, env: Optional[str] = None) -> List[str]:
        results: List[str] = []
        installed_by_env = self.list_installed(env=env)
        vlog("autoremove: scanning", {"env": env})
        for env_name, pkgs in installed_by_env.items():
            targets = [meta for meta in pkgs.values() if not meta.get("explicit")]
            vlog("autoremove: targets", {"env": env_name, "count": len(targets)})
            for meta in targets:
                src_name = meta["source"]
                package_ref = meta["package"]
                version = meta.get("version")
                try:
                    res = self.run(package_ref=package_ref, guide_name="uninstall",
                                   env=env_name, version=version, source_name=src_name)
                    results.append(f"{env_name}:{package_ref} -> {res}")
                except YPMSError as e:
                    if "not defined" in str(e):
                        continue
                    results.append(f"{env_name}:{package_ref} [ERROR] {e}")
        vlog("autoremove: done", {"count": len(results)})
        return results


# ---- CLI (flexible command parsing) ---------------------------- #

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
    sp_info.add_argument("package", help="Package ref USER/PACKAGE (e.g., ypsh/hello-world)")
    sp_info.add_argument("-s", "--source", help="Source name (override global)")
    sp_info.add_argument("--version", help="Release tag or alias to resolve and show")

    sp_inst = sub.add_parser("install", help="Install a package via its 'install' guide")
    sp_inst.add_argument("package", help="Package ref USER/PACKAGE")
    sp_inst.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
    sp_inst.add_argument("--env", default="default", help="Environment ID (default: default)")
    sp_inst.add_argument("-s", "--source", help="Source name (override global)")

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

    sp_ar = sub.add_parser("autoremove", help="Uninstall non-explicit (dependency) packages if 'uninstall' is available")
    sp_ar.add_argument("--env", help="Target environment ID (default: all envs)")

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
    dp.add_argument("package", help="Package ref USER/PACKAGE (e.g., ypsh/hello-world)")
    dp.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
    dp.add_argument("--env", default="default", help="Environment ID (default: default)")
    dp.add_argument("-s", "--source", help="Source name (override global)")
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
    dest = mgr.install(args.package, env=args.env, version=args.version, source_name=args.source, explicit=True)
    print(f"Installed -> {dest}" if dest else "Installed")
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
    print("Refreshed: cache cleared and sources re-fetched.")
    return 0

def _cmd_upgrade(mgr: YPMSManager, args: argparse.Namespace) -> int:
    results = mgr.upgrade(env=args.env)
    print("\n".join(results) if results else "(nothing to upgrade)")
    return 0

def _cmd_autoremove(mgr: YPMSManager, args: argparse.Namespace) -> int:
    results = mgr.autoremove(env=args.env)
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
    vlog("command:", cmd, "rest:", rest_after_cmd)

    try:
        mgr = YPMSManager()
        vlog("mgr: ready", {"ypms_dir": mgr.ypms_dir, "envs_dir": mgr.envs_dir})

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
        vlog("dyn-args:", vars(dyn_args))

        try:
            dest = mgr.run(
                package_ref=dyn_args.package,
                guide_name=cmd,
                env=dyn_args.env,
                version=dyn_args.version,
                source_name=dyn_args.source,
            )
            print(f"{cmd} -> {dest}" if dest else f"{cmd} done")
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
