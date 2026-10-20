#!/usr/bin/env python3
"""Patch the Octop dashboard bundle so plugin channel fields support `select`.

Why this exists
---------------
The dashboard's generic renderer for plugin-contributed channel fields
(``FormItemForField`` in the ``AgentPluginsPanel`` chunk) only understands
``switch`` / ``password`` / ``textarea`` / ``json`` / ``number`` / plain text.
A field declared with ``type: "select"`` + ``options`` (like this plugin's
reply-policy fields) silently degrades to a bare text input, so users must
hand-type raw values such as ``mention`` — with no list of valid values and
no error when they get it wrong.

This script injects, into the deployed bundle:

1. a ``select`` branch rendering an AntD ``Select`` from ``field.options``
   (``{label, value}`` dicts — the shape this plugin already declares);
2. a ``help`` fallback for field descriptions: ``field.help`` (a literal
   string, e.g. Chinese text shipped by the plugin) takes priority over
   ``helpKey`` i18n lookup, and is now also shown for text/number/password
   fields (upstream only ever showed helpKey-based help on switches);
3. a widened plugin-field sanitizer in the entry chunk so ``select`` /
   ``options`` / ``help`` survive registration instead of being reset to a
   plain text field;
4. the channel intro (channel-card description + drawer banner) for plugin
   kinds, read from the plugin's ``_CHANNEL_INTRO`` constant — the dashboard
   otherwise falls back to the channel label because shipped i18n bundles
   carry no ``channels.intro_<plugin-kind>`` key;
5. a matching ``revision`` bump for the chunk's entry in ``sw.js`` so
   service-worker-precached deployments (HTTPS origins) re-fetch it.

The patch is anchor-based and refuses to run when the bundle shape differs
from expectations (no silent corruption).  Re-runs rebuild the panel chunk
from the ``*.orig`` backup, so updating the intro (or any injected text) is
just "edit the plugin, re-run this script".  Results are syntax-checked with
``node --check`` when node is available.

Assets are served as ``immutable`` with hashed filenames, so an already-open
browser keeps the old chunk from its HTTP cache: after patching, hard-reload
the channels page once (Cmd/Ctrl+Shift+R).  An Octop upgrade ships new
filenames (no cache problem) but also a pristine bundle — re-run this script.

Examples
--------
    # Patch a remote host (self-uploads, runs with the octop venv python):
    python3 scripts/patch_dashboard.py --host root@43.155.171.15

    # Run directly on the Octop host:
    python3 scripts/patch_dashboard.py

    # Inspect without writing:
    python3 scripts/patch_dashboard.py --host root@43.155.171.15 --dry-run

    # Roll back:
    python3 scripts/patch_dashboard.py --host root@43.155.171.15 --restore
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PATCH_MARKER = 'field.type==="select"'
SW_REVISION = "select-p1"

# The entry chunk sanitizes plugin-contributed fields through a whitelist:
# any type outside {password, textarea, json, switch} becomes "text" and
# `options`/`help`/`helpKey` are dropped entirely.  Without widening this
# sanitizer the renderer patch below can never see a select field.
SANITIZER_OLD = (
    'type:(f==null?void 0:f.type)==="password"||(f==null?void 0:f.type)==="textarea"'
    '||(f==null?void 0:f.type)==="json"||(f==null?void 0:f.type)==="switch"?f.type:"text",'
    'placeholder:f!=null&&f.placeholder?String(f.placeholder):void 0,'
    'required:(f==null?void 0:f.required)===!0}'
)
SANITIZER_NEW = (
    'type:(f==null?void 0:f.type)==="password"||(f==null?void 0:f.type)==="textarea"'
    '||(f==null?void 0:f.type)==="json"||(f==null?void 0:f.type)==="switch"'
    '||(f==null?void 0:f.type)==="select"?f.type:"text",'
    'placeholder:f!=null&&f.placeholder?String(f.placeholder):void 0,'
    'required:(f==null?void 0:f.required)===!0,'
    'options:Array.isArray(f==null?void 0:f.options)?f.options:void 0,'
    'help:f!=null&&f.help?String(f.help):void 0,'
    'helpKey:f!=null&&f.helpKey?String(f.helpKey):void 0}'
)
SANITIZER_MARKER = "options:Array.isArray(f==null?void 0:f.options)?f.options:void 0"

# Channel card + drawer banner resolve their description through i18n keys
# (``channels.intro_<kind>``) that only exist for builtin channels; plugin
# kinds silently fall back to the label.  Wrap both call sites with a lookup
# into a map built from the plugin's ``_CHANNEL_INTRO`` constant.
INTRO_CALLS = (
    ("channelKey", "label2"),      # channel card description
    ("selectedKind", "kindLabel2"),  # drawer intro banner
)
INTRO_DEFAULT = (
    "通过 OneBot 11 协议接入 QQ 机器人（NapCat 等实现），支持正向/反向 WebSocket、"
    "群聊与私聊回复策略及白名单管控，接入后可在 QQ 中直接对话。"
)
INTRO_PLUGIN_KIND = "onebot11"
INTRO_PLUGIN_DIR = "octop-onebot11"

# --- injected code (identifiers are substituted from the host bundle) -------

HELP_CONST = "const fieldHelp=field.help||(field.helpKey?__T__(field.helpKey):void 0);"

SELECT_BRANCH = (
    'if(field.type==="select")return __JSX__.jsx(__FORM__.Item,{'
    "name:field.name,label:__LBL__,extra:fieldHelp,"
    'rules:field.required?[{required:true,message:__T__("channels.fieldRequired",{label:__LBL__})}]:[],'
    "children:__JSX__.jsx(Select,{"
    'disabled,placeholder:field.placeholder,style:{width:"100%"},'
    'allowClear:true,showSearch:true,optionFilterProp:"label",'
    "options:(Array.isArray(field.options)?field.options:[]).map(opt=>({"
    'label:opt&&opt.label!=null?String(opt.label):String(opt&&opt.value!=null?opt.value:""),'
    "value:opt?opt.value:undefined})"
    ")})});"
)


def die(msg: str) -> None:
    raise SystemExit(f"patch_dashboard: {msg}")


def detect_dashboard_dir(explicit: str | None) -> str:
    if explicit:
        path = os.path.abspath(os.path.expanduser(explicit))
        if not os.path.isdir(os.path.join(path, "assets")):
            die(f"{path} does not look like a dashboard dir (no assets/)")
        return path
    try:
        import octop  # type: ignore

        candidate = os.path.join(os.path.dirname(octop.__file__), "dashboard")
        if os.path.isdir(candidate):
            return candidate
    except Exception:
        pass
    for pattern in (
        "/opt/octop/venv/lib/python3*/site-packages/octop/dashboard",
        "/usr/lib/python3*/site-packages/octop/dashboard",
        "/usr/local/lib/python3*/site-packages/octop/dashboard",
    ):
        hits = sorted(glob.glob(pattern))
        if hits:
            return hits[-1]
    die("cannot locate the octop dashboard dir; pass --dashboard-dir")
    raise AssertionError  # unreachable


def find_chunk(assets: str) -> str:
    path = find_chunk_by(assets, "function FormItemForField")
    if path is None:
        die("no chunk defines FormItemForField; bundle shape changed")
    return path


def find_chunk_by(assets: str, marker: str) -> str | None:
    for name in sorted(os.listdir(assets)):
        if not name.endswith(".js"):
            continue
        path = os.path.join(assets, name)
        try:
            with open(path, encoding="utf-8") as fh:
                if marker in fh.read():
                    return path
        except OSError:
            continue
    return None


def patch_entry_text(text: str) -> str:
    if text.count(SANITIZER_OLD) != 1:
        die(f"sanitizer anchor occurs {text.count(SANITIZER_OLD)} times (expected 1); "
            "bundle shape changed — update this script's anchors")
    return text.replace(SANITIZER_OLD, SANITIZER_NEW, 1)


def load_plugin_intro() -> str:
    """Read ``_CHANNEL_INTRO`` from the plugin source (single source of truth)."""
    candidates = [
        Path(__file__).resolve().parents[1] / "plugins" / INTRO_PLUGIN_DIR / "main.py",
        Path.home() / ".octop" / "plugins" / INTRO_PLUGIN_DIR / "main.py",
    ]
    for path in candidates:
        if not path.is_file():
            continue
        match = re.search(
            r'^_CHANNEL_INTRO = "((?:[^"\\]|\\.)*)"', path.read_text(encoding="utf-8"), re.M
        )
        if match:
            return match.group(1).replace('\\"', '"').replace("\\\\", "\\")
    print(f"warning: _CHANNEL_INTRO not found in {candidates}; using built-in default")
    return INTRO_DEFAULT


def patch_intros(text: str, intro: str) -> str:
    """Wrap the card/drawer i18n intro calls with a plugin-kind lookup map."""
    maplit = json.dumps({INTRO_PLUGIN_KIND: intro}, ensure_ascii=True, separators=(",", ":"))
    for var, fallback in INTRO_CALLS:
        call = "t(`channels.intro_${%s}`,%s)" % (var, fallback)
        pattern = re.compile(r"(\{[^{}]*\}\[%s\]\|\|)?%s" % (re.escape(var), re.escape(call)))
        if len(pattern.findall(text)) != 1:
            die(f"intro anchor for {var} not found exactly once; bundle shape changed")
        text = pattern.sub(lambda _m, v=var: f"{maplit}[{v}]||" + call, text, count=1)
    return text


def extract_slice(text: str) -> tuple[int, int]:
    start = text.find("function FormItemForField")
    if start < 0:
        die("FormItemForField not found in chunk")
    end = text.find("function ", start + len("function FormItemForField"))
    if end < 0:
        die("cannot find the end of FormItemForField (no following function)")
    body = text[start:end]
    if 'valuePropName:"checked"' not in body:
        die("FormItemForField slice looks unexpected (no switch branch); refusing to patch")
    return start, end


def capture_identifiers(body: str) -> dict[str, str]:
    def grab(pattern: str, what: str) -> str:
        m = re.search(pattern, body)
        if not m:
            die(f"cannot locate {what} in FormItemForField; bundle shape changed — "
                "update this script's anchors")
        return m.group(1)

    jsx = grab(r"return (\w+)\.jsx\(", "the jsx runtime identifier")
    form = grab(r"\.jsx\((\w+)\.Item,", "the Form identifier")
    lbl = grab(r"(\w+)=field\.label\.startsWith", "the label variable")
    m_t = re.search(r"const\{t(?::(\w+))?\}=", body)
    if not m_t:
        die("cannot locate the useTranslation() destructuring")
    t_name = m_t.group(1) or "t"
    return {"__JSX__": jsx, "__FORM__": form, "__LBL__": lbl, "__T__": t_name}


def patch_chunk_text(text: str) -> str:
    """Return the patched chunk text; raises SystemExit on shape mismatch."""
    start, end = extract_slice(text)
    body = text[start:end]
    ids = capture_identifiers(body)

    def sub(template: str) -> str:
        out = template
        for key, value in ids.items():
            out = out.replace(key, value)
        return out

    # a) inject `const fieldHelp=...` + the select branch before the switch branch.
    anchor = 'if(field.type==="switch")return '
    if body.count(anchor) != 1:
        die(f"switch-branch anchor occurs {body.count(anchor)} times (expected 1)")
    injected = sub(HELP_CONST) + sub(SELECT_BRANCH)
    new_body = body.replace(anchor, injected + anchor, 1)

    # b) switch branch: replace the raw helpKey extra with the help-aware one.
    extra_re = re.compile(r"extra:field\.helpKey\?\w+\(field\.helpKey\):void 0")
    if len(extra_re.findall(new_body)) != 1:
        die("switch-branch `extra:` anchor not unique after injection")
    new_body = extra_re.sub("extra:fieldHelp", new_body, count=1)

    # c) default (text/number/password/textarea/json) branch: attach help text.
    default_anchor = f"label:{ids['__LBL__']},rules,children:"
    if new_body.count(default_anchor) != 1:
        die(f"default-branch anchor occurs {new_body.count(default_anchor)} times (expected 1)")
    new_body = new_body.replace(
        default_anchor,
        f"label:{ids['__LBL__']},rules,extra:fieldHelp,children:",
        1,
    )

    return text[:start] + new_body + text[end:]


def patch_sw_manifest(dashboard: str, chunk_name: str, dry_run: bool) -> str:
    """Bump the precache revision of the patched chunk in sw.js (if present)."""
    sw_path = os.path.join(dashboard, "sw.js")
    if not os.path.isfile(sw_path):
        return "sw.js: not found (skipped)"
    with open(sw_path, encoding="utf-8") as fh:
        text = fh.read()
    entry = f'{{url:"assets/{chunk_name}",revision:null}}'
    patched_entry = f'{{url:"assets/{chunk_name}",revision:"{SW_REVISION}"}}'
    if patched_entry in text:
        return "sw.js: already patched"
    if text.count(entry) != 1:
        return f"sw.js: precache entry for {chunk_name} not found verbatim (skipped)"
    if not dry_run:
        backup_once(sw_path)
        with open(sw_path, "w", encoding="utf-8") as fh:
            fh.write(text.replace(entry, patched_entry, 1))
    return "sw.js: precache revision bumped"


def backup_once(path: str) -> None:
    backup = path + ".orig"
    if not os.path.exists(backup):
        shutil.copy2(path, backup)


def node_syntax_check(path: str) -> str:
    node = shutil.which("node")
    if not node:
        return "node not available — syntax check skipped"
    with tempfile.TemporaryDirectory() as tmp:
        probe = os.path.join(tmp, "chunk.mjs")
        shutil.copy2(path, probe)
        result = subprocess.run([node, "--check", probe], capture_output=True, text=True)
    if result.returncode != 0:
        die(f"patched chunk failed `node --check`:\n{result.stderr[:2000]}")
    return "node --check: OK"


def patch_entry_chunk(dashboard: str, args: argparse.Namespace) -> None:
    """Widen the entry chunk's plugin-field sanitizer (type whitelist + options/help)."""
    assets = os.path.join(dashboard, "assets")
    entry = find_chunk_by(assets, "function registerPluginChannelKind")
    if entry is None:
        print("entry chunk: registerPluginChannelKind not found (skipped)")
        return
    name = os.path.basename(entry)
    with open(entry, encoding="utf-8") as fh:
        etext = fh.read()
    if SANITIZER_MARKER in etext:
        print(f"{name}: sanitizer already widened — nothing to do")
        return
    patched_entry = patch_entry_text(etext)  # validates anchors before any write
    if args.dry_run:
        print(f"{name}: dry run — would widen the plugin-field sanitizer "
              f"(+{len(patched_entry) - len(etext)} bytes)")
        return
    backup_once(entry)
    with open(entry, "w", encoding="utf-8") as fh:
        fh.write(patched_entry)
    print(f"{name}: sanitizer widened to select/options/help "
          f"(+{len(patched_entry) - len(etext)} bytes, backup at {name}.orig)")
    print(node_syntax_check(entry))


def run_local(args: argparse.Namespace) -> None:
    dashboard = detect_dashboard_dir(args.dashboard_dir)
    chunk = find_chunk(os.path.join(dashboard, "assets"))
    chunk_name = os.path.basename(chunk)
    backup = chunk + ".orig"
    with open(chunk, encoding="utf-8") as fh:
        current = fh.read()

    if args.restore:
        if not os.path.exists(backup):
            die(f"no backup to restore ({backup})")
        shutil.copy2(backup, chunk)
        entry = find_chunk_by(os.path.join(dashboard, "assets"), "function registerPluginChannelKind")
        if entry and os.path.exists(entry + ".orig"):
            shutil.copy2(entry + ".orig", entry)
        sw_backup = os.path.join(dashboard, "sw.js.orig")
        if os.path.exists(sw_backup):
            shutil.copy2(sw_backup, os.path.join(dashboard, "sw.js"))
        print(f"restored {chunk_name} (plus entry chunk / sw.js when backed up)")
        return

    # Rebuild from the pristine backup when present, so re-runs (e.g. after an
    # intro copy change) always start from an unpatched chunk.
    with open(backup if os.path.exists(backup) else chunk, encoding="utf-8") as fh:
        base = fh.read()

    if " as Select" not in base:
        die("chunk does not import an AntD `Select` binding; cannot inject")

    patched = base if PATCH_MARKER in base else patch_chunk_text(base)
    patched = patch_intros(patched, load_plugin_intro())
    if args.dry_run:
        state = "unchanged" if patched == current else f"would write (+{len(patched) - len(current)} bytes vs current)"
        print(f"{chunk_name}: dry run — {state}")
        patch_entry_chunk(dashboard, args)
        return

    if patched == current:
        print(f"{chunk_name}: up to date — nothing to do")
    else:
        backup_once(chunk)
        with open(chunk, "w", encoding="utf-8") as fh:
            fh.write(patched)
        print(f"{chunk_name}: patched ({len(patched)} bytes, backup at {chunk_name}.orig)")
        print(node_syntax_check(chunk))
    print(patch_sw_manifest(dashboard, chunk_name, args.dry_run))
    patch_entry_chunk(dashboard, args)
    print("reminder: assets are immutable-cached — hard-reload the channels page once")


def run_remote(args: argparse.Namespace) -> None:
    """Upload this script to the host and execute it with the octop venv python."""
    self_path = os.path.abspath(__file__)
    remote_script = "/tmp/_patch_dashboard_remote.py"
    forwarded: list[str] = []
    if args.dashboard_dir:
        forwarded += ["--dashboard-dir", args.dashboard_dir]
    if args.dry_run:
        forwarded.append("--dry-run")
    if args.restore:
        forwarded.append("--restore")
    scp = subprocess.run(["scp", "-q", self_path, f"{args.host}:{remote_script}"],
                         capture_output=True, text=True)
    if scp.returncode != 0:
        die(f"scp failed: {scp.stderr.strip()}")
    remote_cmd = (
        f"if [ -x /opt/octop/venv/bin/python ]; then PY=/opt/octop/venv/bin/python; "
        f"else PY=python3; fi; $PY {remote_script} {' '.join(forwarded)}"
    )
    result = subprocess.run(["ssh", args.host, remote_cmd], text=True)
    if result.returncode != 0:
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", help="ssh target, e.g. root@43.155.171.15 (omit to run locally)")
    parser.add_argument("--dashboard-dir", help="octop dashboard dir (default: auto-detect)")
    parser.add_argument("--dry-run", action="store_true", help="report without writing")
    parser.add_argument("--restore", action="store_true", help="restore the pre-patch backup")
    args = parser.parse_args()
    if args.host:
        run_remote(args)
    else:
        run_local(args)


if __name__ == "__main__":
    main()
