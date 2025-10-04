#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# -- PyYPSH ----------------------------------------------------- #
# ypms.py on PyYPSH                                               #
# Made by DiamondGotCat, Licensed under MIT License               #
# Copyright (c) 2025 DiamondGotCat                                #
# ---------------------------------------------- DiamondGotCat -- #

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple

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

DEFAULT_SOURCES: Dict[str, str] = {
    "yopr": "https://ypsh-dgc.github.io/YPMS/yopr/ypms.json"
}

USER_AGENT = "YPMS/1.1 (+https://github.com/YPSH-DGC/YPMS/)"
HTTP_TIMEOUT = 20  # seconds


# ---- Exceptions ------------------------------------------------ #

class YPMSError(Exception):
    pass


# ---- HTTP helpers --------------------------------------------- #

def _http_get_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            if resp.status != 200:
                raise YPMSError(f"HTTP {resp.status} for {url}")
            data = resp.read()
            try:
                return json.loads(data.decode("utf-8"))
            except json.JSONDecodeError as e:
                raise YPMSError(f"Invalid JSON at {url}: {e}") from e
    except urllib.error.URLError as e:  # includes HTTPError
        raise YPMSError(f"Failed to GET {url}: {e}") from e


def _http_download(url: str, dest_path: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
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
    except urllib.error.URLError as e:
        raise YPMSError(f"Failed to download {url}: {e}") from e


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

    def __init__(self, source_name: str, config_url: str):
        self.source_name = source_name
        self.config_url = config_url
        cfg = _http_get_json(config_url)
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
        return f"{self.config.base_url}{self.config.path_index}"

    def _package_url(self, user_id: str, package_id: str) -> str:
        return f"{self.config.base_url}{self.config.path_package}".format(
            USER_ID=user_id, PACKAGE_ID=package_id
        )

    # Fetchers
    def fetch_index(self) -> Dict[str, Any]:
        return _http_get_json(self._index_url())

    def fetch_package_info(self, user_id: str, package_id: str) -> Dict[str, Any]:
        return _http_get_json(self._package_url(user_id, package_id))

    @staticmethod
    def resolve_release_tag(pkg_info: Dict[str, Any], tag: Optional[str]) -> str:
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
        return alias.get(tag, tag)

    def fetch_release_info(self, pkg_info: Dict[str, Any], release_id: str) -> Dict[str, Any]:
        url_tmpl = pkg_info["package.release.url"]
        url = url_tmpl.format(RELEASE_ID=release_id)
        return _http_get_json(url)


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

    def run_guide(self, env_dir: str, version_tag: Optional[str], guide_name: str) -> str:
        """Run an arbitrary guide existing in release.guides[guide_name]."""
        resolved = self.source.resolve_release_tag(self.pkg_info, version_tag)
        rel = self.source.fetch_release_info(self.pkg_info, resolved)

        guides = rel.get("release.guides", {})
        guide = guides.get(guide_name)
        if not guide:
            raise YPMSError(f"Guide '{guide_name}' not defined for release '{resolved}'.")

        gtype = guide.get("type")
        if gtype == "download-only":
            content = guide["content"]
            dest_template: str = content["dest"]
            url: str = content["url"]
            dest_path = self._resolve_env_dest(dest_template, env_dir)
            _http_download(url, dest_path)
            return dest_path

        raise YPMSError(f"Unsupported guide type: {gtype}")

    def install(self, env_dir: str, version_tag: Optional[str] = None) -> str:
        return self.run_guide(env_dir=env_dir, version_tag=version_tag, guide_name="install")


# ---- Manager --------------------------------------------------- #

class YPMSManager:
    """High-level manager for sources, packages, and environments."""

    def __init__(self, ypms_dir: str = YPMS_DIR, envs_dir: str = YPMS_ENVS_DIR):
        self.ypms_dir = ypms_dir
        self.envs_dir = envs_dir
        os.makedirs(self.ypms_dir, exist_ok=True)
        os.makedirs(self.envs_dir, exist_ok=True)

        # Initialize default sources.json if missing
        if not os.path.exists(SOURCES_PATH):
            with open(SOURCES_PATH, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_SOURCES, f, ensure_ascii=False, indent=2)

        self.sources_map = self._load_sources()
        self._source_cache: Dict[str, YPMSSource] = {}

    def _load_sources(self) -> Dict[str, str]:
        try:
            with open(SOURCES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise YPMSError("sources.json must be a JSON object mapping name -> ypms.json URL")
                return {str(k): str(v) for k, v in data.items()}
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError as e:
            raise YPMSError(f"Invalid JSON: {SOURCES_PATH}: {e}") from e

    def save_sources(self) -> None:
        with open(SOURCES_PATH, "w", encoding="utf-8") as f:
            json.dump(self.sources_map, f, ensure_ascii=False, indent=2)

    # Sources ops
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

    # Source access
    def _get_source(self, source_name: Optional[str]) -> YPMSSource:
        if not source_name:
            if "yopr" in self.sources_map:
                source_name = "yopr"
            else:
                if not self.sources_map:
                    raise YPMSError("No sources configured.")
                source_name = sorted(self.sources_map.keys())[0]

        if source_name in self._source_cache:
            return self._source_cache[source_name]

        url = self.sources_map.get(source_name)
        if not url:
            raise YPMSError(f"Unknown source: {source_name}")

        src = YPMSSource(source_name, url)
        self._source_cache[source_name] = src
        return src

    # Env utils
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

    # Packages
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

    def install(self, package_ref: str, env: str = "default", version: Optional[str] = None,
                source_name: Optional[str] = None) -> str:
        return self.run(package_ref, guide_name="install", env=env, version=version, source_name=source_name)

    def run(self, package_ref: str, guide_name: str, env: str = "default", version: Optional[str] = None,
            source_name: Optional[str] = None) -> str:
        """Run an arbitrary guide by name (e.g., 'update', 'uninstall', etc.)."""
        user, pkg = self._split_pkg_ref(package_ref)
        src = self._get_source(source_name)
        env_dir = self.ensure_env_dir(env)
        yp = YPMSPackage(src, user, pkg)
        return yp.run_guide(env_dir=env_dir, version_tag=version, guide_name=guide_name)

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


# ---- CLI (flexible command parsing) ---------------------------- #

BUILTIN_CMDS = {"list", "info", "install", "envs", "sources"}

def _build_full_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="ypms",
        description="YPMS - YPSH Package Manager (CLI & Library)",
    )
    p.add_argument("-s", "--source", help="Source name (default: yopr or first configured)")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose output")

    sub = p.add_subparsers(dest="cmd")

    # list
    sp_list = sub.add_parser("list", help="List packages in source")
    sp_list.add_argument("-s", "--source", help="Source name (override global)")

    # info
    sp_info = sub.add_parser("info", help="Show package info")
    sp_info.add_argument("package", help="Package ref USER/PACKAGE (e.g., ypsh/hello-world)")
    sp_info.add_argument("-s", "--source", help="Source name (override global)")
    sp_info.add_argument("--version", help="Release tag or alias to resolve and show")

    # install
    sp_inst = sub.add_parser("install", help="Install a package via its 'install' guide")
    sp_inst.add_argument("package", help="Package ref USER/PACKAGE")
    sp_inst.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
    sp_inst.add_argument("--env", default="default", help="Environment ID (default: default)")
    sp_inst.add_argument("-s", "--source", help="Source name (override global)")

    # envs
    sub.add_parser("envs", help="List environments")

    # sources
    sp_src = sub.add_parser("sources", help="Manage sources.json")
    src_sub = sp_src.add_subparsers(dest="src_cmd", required=True)
    src_sub.add_parser("list", help="List configured sources")
    ss_add = src_sub.add_parser("add", help="Add a source")
    ss_add.add_argument("name")
    ss_add.add_argument("url", help="URL to ypms.json")
    ss_rm = src_sub.add_parser("remove", help="Remove a source")
    ss_rm.add_argument("name")

    return p


def _parse_global_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    gp = argparse.ArgumentParser(add_help=False)
    gp.add_argument("-s", "--source")
    gp.add_argument("-v", "--verbose", action="store_true")
    gp.add_argument("-h", "--help", action="store_true")
    gargs, rest = gp.parse_known_args(argv)
    return gargs, rest


def _parse_dynamic_command_args(cmd: str, rest: list[str]) -> argparse.Namespace:
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
    dest = mgr.install(args.package, env=args.env, version=args.version, source_name=args.source)
    print(f"Installed -> {dest}")
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


# ---- main ------------------------------------------------------ #

def main(argv: Optional[list[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    gargs, rest = _parse_global_args(argv)

    full_parser = _build_full_parser()
    if gargs.help or not rest:
        full_parser.print_help()
        return 0

    cmd = rest[0]
    rest_after_cmd = rest[1:]

    try:
        mgr = YPMSManager()

        if cmd in BUILTIN_CMDS:
            parsed = full_parser.parse_args([cmd] + rest_after_cmd + (["-s", gargs.source] if gargs.source else []) + (["-v"] if gargs.verbose else []))
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
            else:
                full_parser.print_help()
                return 2

        dyn_args = _parse_dynamic_command_args(cmd, rest_after_cmd)
        if not getattr(dyn_args, "source", None):
            dyn_args.source = gargs.source

        try:
            dest = mgr.run(
                package_ref=dyn_args.package,
                guide_name=cmd,
                env=dyn_args.env,
                version=dyn_args.version,
                source_name=dyn_args.source,
            )
            print(f"{cmd} -> {dest}")
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
