#!/usr/bin/env python3
"""Write a compact description of the external Data tree for the dashboard."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path


FULL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(FULL_DIR / "crawlers"))
from project_paths import DATA_ROOT, SHARED_ROOT  # noqa: E402


DESCRIPTIONS = {
    "eu": "Current EU treaties and in-force basic acts from EUR-Lex/CELLAR.",
    "bund": "Current federal constitutional and GII legal-text corpus.",
    "laender": "State constitutions, downloaded state-law texts, and portal inventories.",
    "legacy": "Earlier regulation-measurement raw, processed, input, and derived datasets.",
    "_dashboard_snapshot_archive": "Superseded full manifest snapshots retained outside the shared folder.",
}


def display_path(path):
    try:
        return "~/" + path.relative_to(Path.home()).as_posix()
    except ValueError:
        return str(path)


def tree_stats(root):
    files = 0
    size = 0
    for directory, _, filenames in os.walk(root):
        directory_path = Path(directory)
        for filename in filenames:
            candidate = directory_path / filename
            try:
                stat = candidate.stat()
            except OSError:
                continue
            files += 1
            size += stat.st_size
    return {"files": files, "bytes": size}


def build_summary(data_root):
    sections = []
    total_files = 0
    total_bytes = 0
    for entry in sorted(data_root.iterdir(), key=lambda item: item.name):
        if not entry.is_dir():
            continue
        stats = tree_stats(entry)
        total_files += stats["files"]
        total_bytes += stats["bytes"]
        sections.append({
            "name": entry.name,
            "relative_path": entry.relative_to(data_root).as_posix(),
            "description": DESCRIPTIONS.get(entry.name, "External project data."),
            **stats,
        })
    return {
        "data_root": display_path(data_root),
        "totals": {"files": total_files, "bytes": total_bytes},
        "sections": sections,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=DATA_ROOT)
    parser.add_argument(
        "--output",
        type=Path,
        default=SHARED_ROOT / "dashboard" / "data-summary.json",
    )
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    data_root = args.data_dir.expanduser()
    output = args.output.expanduser()
    summary = build_summary(data_root)

    if args.check:
        existing = json.loads(output.read_text(encoding="utf-8"))
        comparable = {key: existing.get(key) for key in ("data_root", "totals", "sections")}
        if comparable != summary:
            raise SystemExit("Dashboard data summary is stale.")
        print("Dashboard data summary is current.")
        return

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **summary,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output)
    print("Wrote compact dashboard summary to %s." % output)


if __name__ == "__main__":
    main()
