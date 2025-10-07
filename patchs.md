```diff
@@
-try:
-    from rich.console import Console, Group
+try:
+    from rich.console import Console, Group
     from rich.live import Live
     from rich.text import Text
+    from rich.markup import escape as _rich_escape
     _RICH_AVAILABLE = True
@@
-class PackageLiveUI:
+class PackageLiveUI:
@@
-    def __init__(self, header: str):
-        self._use_rich = _RICH_AVAILABLE and sys.stdout.isatty()
+    def __init__(self, header: str, header_style: Optional[str] = None):
+        self._use_rich = _RICH_AVAILABLE and sys.stdout.isatty()
         self.header = header
         if not self._use_rich:
-            self._plain = _PlainPackageUI(header)
+            self._plain = _PlainPackageUI(header)
             return
@@
-        self._header_text = Text(header)
+        self._header_text = Text(header, style=header_style or "")
         self._steps: Dict[int, Text] = {}
@@
-    def set_header(self, text: str) -> None:
+    def set_header(self, text: str, style: Optional[str] = None) -> None:
         if not self._use_rich:
-            self._plain.set_header(text)
+            self._plain.set_header(text)
             return
-        self._header_text = Text(text)
+        self._header_text = Text(text, style=style or "")
         self._live.update(self._renderable())
 
-    def set_step(self, idx: int, text: str) -> None:
+    def set_step(self, idx: int, text: str, style: Optional[str] = None) -> None:
         if not self._use_rich:
-            self._plain.set_step(idx, text)
+            self._plain.set_step(idx, text)
             return
-        self._steps[idx] = Text(f"      {idx}. {text}")
+        self._steps[idx] = Text(f"      {idx}. {text}", style=style or "dim")
         self._live.update(self._renderable())
```

```diff
-            if ui and step_no is not None:
-                ui.set_step(step_no, f"{label} (0%)" if total_size else label)
+            if ui and step_no is not None:
+                ui.set_step(step_no, f"{label} (0%)" if total_size else label, style="dim")
@@
-                        if ui and step_no is not None:
-                            ui.set_step(step_no, f"{label} ({pct}%)")
+                        if ui and step_no is not None:
+                            ui.set_step(step_no, f"{label} ({pct}%)", style="dim")
@@
-                if total_size:
-                    ui.set_step(step_no, f"{label} (100%)")
-                else:
-                    ui.set_step(step_no, f"Downloaded {fname}")
+                if total_size:
+                    ui.set_step(step_no, f"{label} (100%)", style="dim")
+                else:
+                    ui.set_step(step_no, f"Downloaded {fname}", style="dim")
```

```diff
@@
 def _p_info(msg: str) -> None:
     print(f"(i) {msg}")
@@
+_CONSOLE = Console() if (_RICH_AVAILABLE and sys.stdout.isatty()) else None
+
+def _print_plan_item(i: int, kind: str, source: str, package_ref: str, version: Optional[str], footnote: Optional[int]) -> None:
+    sym_map = {"install": "+", "update": "^", "target": "*"}
+    color_map = {"install": "green", "update": "yellow", "target": "cyan"}
+    sym = sym_map.get(kind, "?")
+    foot = f" *{footnote}" if footnote else ""
+    if _CONSOLE:
+        _CONSOLE.print(f"{i}. [bold {color_map.get(kind,'white')}]{sym}[/] {source}/{package_ref}@{version}{foot}")
+    else:
+        print(f"{i}. {sym} {source}/{package_ref}@{version}{foot}")
```

```diff
@@ def install(...):
-        for i, it in enumerate(ops, 1):
-            sym = {"install": "+", "update": "^", "target": "*"}.get(it.kind, "?")
-            line = f"{i}. {sym} {it.source}/{it.package_ref}@{it.version}"
-            if it.footnote:
-                line += f" *{it.footnote}"
-            print(line)
+        for i, it in enumerate(ops, 1):
+            _print_plan_item(i, it.kind, it.source, it.package_ref, it.version, it.footnote)
```

```diff
@@ class YPMSManager:
     def _build_operation_plan(self, *, package_ref: str, env: str, version: Optional[str], source_name: Optional[str]) -> Tuple[List["_OpItem"], Dict[int, str]]:
@@
-        ops.append(YPMSManager._OpItem(kind="target", source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved))
+        # target: already installed ? -> update or noop
+        target_key = self._db_key(src.source_name, f"{user}/{pkg}")
+        installed = self._db_load().get("envs", {}).get(env, {}).get(target_key)
+        if installed:
+            if installed.get("version") == resolved:
+                # nothing to do (no target op)
+                pass
+            else:
+                ops.append(YPMSManager._OpItem(kind="update", source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved))
+        else:
+            ops.append(YPMSManager._OpItem(kind="target", source=src.source_name, package_ref=f"{user}/{pkg}", version=resolved))
         return ops, notes
```

```diff
@@ def install(...):
-        _p_info("This action will change the package state as follows:")
-        for i, it in enumerate(ops, 1):
+        if not ops:
+            _p_info("Already installed. Nothing to do.")
+            return self.ensure_env_dir(env)
+        _p_info("This action will change the package state as follows:")
+        for i, it in enumerate(ops, 1):
             _print_plan_item(i, it.kind, it.source, it.package_ref, it.version, it.footnote)
```

```diff
@@ class YPMSManager:
+    def _find_dependents(self, *, env: str, target_source: str, target_pref: str) -> List[Dict[str, str]]:
+        """Return list of dependents that require target_pref from target_source."""
+        res: List[Dict[str, str]] = []
+        db = self._db_load().get("envs", {}).get(env, {})
+        for meta in db.values():
+            dep_src_name = meta["source"]
+            pkg_ref = meta["package"]  # "user/pkg"
+            version = meta.get("version")
+            # read its release and deps
+            u, p = self._split_pkg_ref(pkg_ref)
+            src = self._get_source(dep_src_name)
+            pkg_info = src.fetch_package_info(u, p)
+            rel = src.fetch_release_info(pkg_info, version)
+            for d in rel.get("release.depends", []):
+                ds, dr, dv = self._parse_dep(d)
+                ds = ds or target_source
+                if ds == target_source and dr == target_pref:
+                    # resolve requested version (alias) if any
+                    req_version = None
+                    if dv:
+                        dep_user, dep_pkg = self._split_pkg_ref(dr)
+                        dep_src_obj = self._get_source(ds)
+                        dep_pkg_info = dep_src_obj.fetch_package_info(dep_user, dep_pkg)
+                        req_version = dep_src_obj.resolve_release_tag(dep_pkg_info, dv)
+                    res.append({
+                        "dependent_source": dep_src_name,
+                        "dependent_package": pkg_ref,
+                        "dependent_version": version or "",
+                        "required_version": req_version or "",
+                    })
+        return res
+
+    def _check_update_compat(self, *, env: str, target_source: str, target_pref: str, new_version: str) -> List[str]:
+        """Return list of blocker messages (empty if compatible)."""
+        blockers: List[str] = []
+        for d in self._find_dependents(env=env, target_source=target_source, target_pref=target_pref):
+            req = d["required_version"]
+            # No version specified or 'latest' means accept anything
+            if not req or req.lower() == "latest" or req == "*":
+                continue
+            if req != new_version:
+                blockers.append(
+                    f"{d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
+                    f"requires {target_source}/{target_pref}@{req}, but planned {new_version}"
+                )
+        return blockers
```

```diff
@@ def install(...):
-            elif it.kind == "update":
+            elif it.kind == "update":
+                # dependency compatibility check
+                incompat = self._check_update_compat(env=env, target_source=it.source, target_pref=it.package_ref, new_version=it.version or "")
+                if incompat:
+                    if not force:
+                        if _CONSOLE:
+                            _CONSOLE.print("[bold red]Blocked by dependency constraints:[/]")
+                            for m in incompat:
+                                _CONSOLE.print(f"  - {m}")
+                        else:
+                            print("Blocked by dependency constraints:")
+                            print("\n".join(f"  - {m}" for m in incompat))
+                        return env_dir
+                    else:
+                        # show warnings and confirm
+                        if _CONSOLE:
+                            _CONSOLE.print("[bold yellow]WARNING: dependency constraints may be violated:[/]")
+                            for m in incompat:
+                                _CONSOLE.print(f"  - {m}")
+                        else:
+                            print("WARNING: dependency constraints may be violated:")
+                            print("\n".join(f"  - {m}" for m in incompat))
+                        if not assume_yes:
+                            _p_question("Continue anyway? [y/N] ")
+                            ans = sys.stdin.readline().strip().lower()
+                            if ans not in ("y", "yes"):
+                                print("Aborted.")
+                                return env_dir
                 try:
-                    header_ui = PackageLiveUI(f"(>) Updating {it.package_ref}@{it.version}")
+                    header_ui = PackageLiveUI(f"(>) Updating {it.package_ref}@{it.version}", header_style="bold yellow")
                     _ = self.run(package_ref=it.package_ref, guide_name="update", env=env, version=it.version, source_name=it.source)
-                    header_ui.set_header(f"(>) Updated {it.package_ref}@{it.version}")
+                    header_ui.set_header(f"(>) Updated {it.package_ref}@{it.version}", style="bold green")
                     header_ui.clear_steps_keep_header()
                     header_ui.stop()
```

```diff
@@ def run(self, package_ref: str, guide_name: str, env: str = "default", version: Optional[str] = None,
-            source_name: Optional[str] = None) -> str:
+            source_name: Optional[str] = None, *, force: bool = False, assume_yes: bool = False) -> str:
@@
-        ui = PackageLiveUI(f"(>) Running guide '{guide_name}' for {package_ref}@{resolved}")
+        ui = PackageLiveUI(f"(>) Running guide '{guide_name}' for {package_ref}@{resolved}", header_style="bold magenta")
+        # guard: uninstall blocked if there are dependents
+        if guide_name == "uninstall":
+            dependents = self._find_dependents(env=env, target_source=src.source_name, target_pref=f"{user}/{pkg}")
+            if dependents:
+                if not force:
+                    if _CONSOLE:
+                        _CONSOLE.print("[bold red]Blocked: other packages depend on this package:[/]")
+                        for d in dependents:
+                            _CONSOLE.print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
+                                           f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
+                    else:
+                        print("Blocked: other packages depend on this package:")
+                        for d in dependents:
+                            print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
+                                  f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
+                    ui.stop()
+                    raise YPMSError("uninstall blocked by dependents")
+                else:
+                    if _CONSOLE:
+                        _CONSOLE.print("[bold yellow]WARNING: other packages depend on this package:[/]")
+                        for d in dependents:
+                            _CONSOLE.print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
+                                           f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
+                    else:
+                        print("WARNING: other packages depend on this package:")
+                        for d in dependents:
+                            print(f"  - {d['dependent_source']}/{d['dependent_package']}@{d['dependent_version']} "
+                                  f"requires {src.source_name}/{user}/{pkg}@{d['required_version'] or '*'}")
+                    if not assume_yes:
+                        _p_question("Continue anyway? [y/N] ")
+                        ans = sys.stdin.readline().strip().lower()
+                        if ans not in ("y", "yes"):
+                            ui.stop()
+                            print("Aborted.")
+                            return env_dir
@@
-            if guide_name == "uninstall":
+            if guide_name == "uninstall":
                 self._db_mark_uninstalled(env=env, source=src.source_name, package_ref=f"{user}/{pkg}")
-            ui.set_header(f"(>) Finished guide '{guide_name}' for {package_ref}@{resolved}")
+            ui.set_header(f"(>) Finished guide '{guide_name}' for {package_ref}@{resolved}", style="bold green")
             ui.clear_steps_keep_header()
```

```diff
@@ def _exec_step_uninstall_package(...):
-    for src_name, pref, _ver in items:
+    for src_name, pref, _ver in items:
         key = mgr._db_key(src_name or (context.get("SOURCE_NAME") or "yopr"), pref)
         if key not in env_pkgs:
             continue
-        ui.set_step(step_no or 0, f"Uninstalling {pref}") if ui else None
+        # dependents check
+        tsrc = (src_name or (context.get("SOURCE_NAME") or "yopr"))
+        dependents = mgr._find_dependents(env=env, target_source=tsrc, target_pref=pref)
+        if dependents and not getattr(mgr, "_force_flag", False):
+            raise YPMSError(f"uninstall blocked: other packages depend on {tsrc}/{pref}")
+        elif dependents and getattr(mgr, "_force_flag", False):
+            ui.set_step(step_no or 0, f"WARNING: dependents exist; proceeding due to --force") if ui else None
+        ui.set_step(step_no or 0, f"Uninstalling {pref}", style="red") if ui else None
```

```diff
@@ class YPMSManager:
-    def _install_execute(..., print_header: bool = True) -> str:
+    def _install_execute(..., print_header: bool = True, force: bool = False) -> str:
@@
-        ui = PackageLiveUI(header) if print_header else None
+        ui = PackageLiveUI(header, header_style="bold cyan") if print_header else None
+        self._force_flag = force
         try:
@@
-            _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx, mgr=self, env=env, ui=ui)
+            _execute_guide_steps(guide_obj=guide, env_dir=env_dir, pkg_ctx=pkg_ctx, mgr=self, env=env, ui=ui, force=force)
@@
-            if ui:
-                ui.set_header(f"(>) Installed {package_ref}@{resolved}")
+            if ui:
+                ui.set_header(f"(>) Installed {package_ref}@{resolved}", style="bold green")
                 ui.clear_steps_keep_header()
         finally:
             if ui:
                 ui.stop()
+        self._force_flag = False
```

```diff
@@ def _execute_guide_steps(...):
-                         mgr: Optional["YPMSManager"] = None,
-                         env: Optional[str] = None,
-                         ui: Optional[PackageLiveUI] = None) -> str:
+                         mgr: Optional["YPMSManager"] = None,
+                         env: Optional[str] = None,
+                         ui: Optional[PackageLiveUI] = None,
+                         force: bool = False) -> str:
@@
-        elif gtype == "uninstall-package":
+        elif gtype == "uninstall-package":
             if mgr is None or env is None:
                 raise YPMSError("uninstall-package step requires manager and env")
-            last_result = _exec_step_uninstall_package(step, env_dir=env_dir, context=pkg_ctx, mgr=mgr, env=env, ui=ui, step_no=idx)
+            mgr._force_flag = force
+            last_result = _exec_step_uninstall_package(step, env_dir=env_dir, context=pkg_ctx, mgr=mgr, env=env, ui=ui, step_no=idx)
```

```diff
@@ def _build_full_parser():
-    sp_inst.add_argument("-y", "--yes", action="store_true", help="Assume yes to prompts and run non-interactively")
+    sp_inst.add_argument("-y", "--yes", action="store_true", help="Assume yes to prompts and run non-interactively")
+    sp_inst.add_argument("-f", "--force", action="store_true", help="Force risky operations (override dependency blocks with warning)")
@@
-    sp_up = sub.add_parser("upgrade", help="Refresh caches and run 'update' for all installed packages")
+    sp_up = sub.add_parser("upgrade", help="Refresh caches and run 'update' for all installed packages")
     sp_up.add_argument("--env", help="Target environment ID (default: all envs)")
+    sp_up.add_argument("-f", "--force", action="store_true", help="Force risky operations")
@@
-    sp_ar = sub.add_parser("autoremove", help="Uninstall non-explicit (dependency) packages if 'uninstall' is available")
+    sp_ar = sub.add_parser("autoremove", help="Uninstall non-explicit (dependency) packages if 'uninstall' is available")
     sp_ar.add_argument("--env", help="Target environment ID (default: all envs)")
+    sp_ar.add_argument("-f", "--force", action="store_true", help="Force risky operations")
```

```diff
@@ def _parse_dynamic_command_args(cmd: str, rest: List[str]) -> argparse.Namespace:
     dp.add_argument("package", help="Package ref USER/PACKAGE")
     dp.add_argument("--version", help="Release tag or alias (e.g., latest, v1.0)")
     dp.add_argument("--env", default="default", help="Environment ID (default: default)")
     dp.add_argument("-s", "--source", help="Source name (override global)")
+    dp.add_argument("-y", "--yes", action="store_true", help="Assume yes to prompts (for warnings)")
+    dp.add_argument("-f", "--force", action="store_true", help="Force risky operations")
     return dp.parse_args(rest)
```

```diff
@@ def _cmd_install(...):
-    _ = mgr.install(args.package, env=args.env, version=args.version, source_name=args.source, explicit=True, assume_yes=args.yes)
+    _ = mgr.install(args.package, env=args.env, version=args.version, source_name=args.source, explicit=True, assume_yes=args.yes, force=args.force)
@@ def _cmd_upgrade(...):
-    results = mgr.upgrade(env=args.env)
+    results = mgr.upgrade(env=args.env, force=args.force)
@@ def _cmd_autoremove(...):
-    results = mgr.autoremove(env=args.env)
+    results = mgr.autoremove(env=args.env, force=args.force)
```

```diff
@@
-            dest = mgr.run(
+            dest = mgr.run(
                 package_ref=dyn_args.package,
                 guide_name=cmd,
                 env=dyn_args.env,
                 version=dyn_args.version,
-                source_name=dyn_args.source,
+                source_name=dyn_args.source,
+                force=getattr(dyn_args, "force", False),
+                assume_yes=getattr(dyn_args, "yes", False),
             )
```

```diff
-    def upgrade(self, env: Optional[str] = None) -> List[str]:
+    def upgrade(self, env: Optional[str] = None, *, force: bool = False) -> List[str]:
@@
-                    res = self.run(package_ref=package_ref, guide_name="update",
-                                   env=env_name, version=version, source_name=src_name)
+                    res = self.run(package_ref=package_ref, guide_name="update",
+                                   env=env_name, version=version, source_name=src_name, force=force, assume_yes=True)
```

```diff
-    def autoremove(self, env: Optional[str] = None) -> List[str]:
+    def autoremove(self, env: Optional[str] = None, *, force: bool = False) -> List[str]:
@@
-                    res = self.run(package_ref=package_ref, guide_name="uninstall",
-                                   env=env_name, version=version, source_name=src_name)
+                    res = self.run(package_ref=package_ref, guide_name="uninstall",
+                                   env=env_name, version=version, source_name=src_name, force=force, assume_yes=True)
```

