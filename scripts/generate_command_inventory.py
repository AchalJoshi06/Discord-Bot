"""Generate command inventory markdown from active cog decorators.

Usage:
  python scripts/generate_command_inventory.py

This script updates BOT_DOCUMENTATION.md between markers:
  <!-- COMMAND_INVENTORY_START -->
  <!-- COMMAND_INVENTORY_END -->
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


ROOT = Path(__file__).resolve().parents[1]
COGS_DIR = ROOT / "cogs"
DOC_PATH = ROOT / "BOT_DOCUMENTATION.md"
START_MARKER = "<!-- COMMAND_INVENTORY_START -->"
END_MARKER = "<!-- COMMAND_INVENTORY_END -->"


@dataclass
class CommandRow:
    name: str
    aliases: str
    command_type: str
    access: str
    cog: str


def _const_str(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _extract_aliases(call: ast.Call) -> List[str]:
    for kw in call.keywords:
        if kw.arg != "aliases":
            continue
        if isinstance(kw.value, (ast.List, ast.Tuple)):
            out: List[str] = []
            for item in kw.value.elts:
                v = _const_str(item)
                if v:
                    out.append(v)
            return out
    return []


def _extract_name(call: ast.Call, fallback: str) -> str:
    for kw in call.keywords:
        if kw.arg == "name":
            v = _const_str(kw.value)
            if v:
                return v
    return fallback


def _is_decorator(call: ast.Call, root: str, attr: str) -> bool:
    fn = call.func
    return (
        isinstance(fn, ast.Attribute)
        and isinstance(fn.value, ast.Name)
        and fn.value.id == root
        and fn.attr == attr
    )


def _detect_access(fn_node: ast.AsyncFunctionDef) -> str:
    src = ast.unparse(fn_node)
    if "_ensure_admin_ctx" in src or "has_admin_role" in src:
        return "Admin"
    if "_ensure_leadership_ctx" in src or "has_leadership_role" in src:
        return "Leadership"
    return "Everyone"


def _iter_commands(py_path: Path) -> Iterable[CommandRow]:
    tree = ast.parse(py_path.read_text(encoding="utf-8"))
    cog = py_path.stem
    group_fn_names: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue
        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue
            if _is_decorator(deco, "commands", "hybrid_group"):
                group_fn_names.add(node.name)

    for node in ast.walk(tree):
        if not isinstance(node, ast.AsyncFunctionDef):
            continue

        for deco in node.decorator_list:
            if not isinstance(deco, ast.Call):
                continue

            cmd_type = None
            if _is_decorator(deco, "commands", "hybrid_command"):
                cmd_type = "Hybrid"
            elif _is_decorator(deco, "commands", "command"):
                cmd_type = "Text-only"
            elif _is_decorator(deco, "app_commands", "command"):
                cmd_type = "Slash-only"
            elif _is_decorator(deco, "commands", "hybrid_group"):
                cmd_type = "Hybrid Group"
            elif (
                isinstance(deco.func, ast.Attribute)
                and isinstance(deco.func.value, ast.Name)
                and deco.func.attr == "command"
                and deco.func.value.id in group_fn_names
            ):
                cmd_type = "Group Subcommand"

            if not cmd_type:
                continue

            name = _extract_name(deco, node.name)
            aliases = ", ".join(_extract_aliases(deco)) or "-"
            access = _detect_access(node)
            yield CommandRow(name=name, aliases=aliases, command_type=cmd_type, access=access, cog=cog)


def build_markdown_table(rows: List[CommandRow]) -> str:
    lines = [
        "| Name | Aliases | Type | Access | Cog |",
        "|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r.name} | {r.aliases} | {r.command_type} | {r.access} | {r.cog} |"
        )
    return "\n".join(lines)


def update_document(table_md: str, total: int) -> None:
    text = DOC_PATH.read_text(encoding="utf-8")
    start = text.find(START_MARKER)
    end = text.find(END_MARKER)
    if start == -1 or end == -1 or end < start:
        raise RuntimeError("Command inventory markers not found in BOT_DOCUMENTATION.md")

    before = text[: start + len(START_MARKER)]
    after = text[end:]
    replacement = (
        "\n\n"
        f"Generated from decorators in `cogs/*.py`. Total entries: **{total}**.\n\n"
        f"{table_md}\n\n"
    )
    DOC_PATH.write_text(before + replacement + after, encoding="utf-8")


def main() -> None:
    rows: List[CommandRow] = []
    for py in sorted(COGS_DIR.glob("*.py")):
        rows.extend(_iter_commands(py))

    rows.sort(key=lambda r: (r.cog, r.name))
    table_md = build_markdown_table(rows)
    update_document(table_md, total=len(rows))
    print(f"Updated BOT_DOCUMENTATION.md with {len(rows)} command entries")


if __name__ == "__main__":
    main()
