#!/usr/bin/env python3

"""Filter NUL-delimited file lists without losing unusual file names."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterator


def iter_nul_paths(path: str) -> Iterator[str]:
    pending = b""
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(64 * 1024)
            if not chunk:
                break
            fields = (pending + chunk).split(b"\0")
            pending = fields.pop()
            for field in fields:
                if field:
                    yield os.fsdecode(field)

    if pending:
        raise ValueError(f"NUL-delimited list is missing its final terminator: {path}")


def split_path_list(value: str) -> list[str]:
    return [os.path.normpath(entry) for entry in value.split(":") if entry]


def path_is_within(path: str, root: str) -> bool:
    normalized_path = os.path.normpath(path)
    normalized_root = os.path.normpath(root)
    try:
        return os.path.commonpath((normalized_path, normalized_root)) == normalized_root
    except ValueError:
        return False


def append_filtered_paths(
    input_path: str,
    output_path: str,
    excluded_paths: list[str],
    ignored_paths: list[str],
    quarantine_path: str = "",
) -> int:
    written = 0
    blocked_roots = excluded_paths + ignored_paths
    if quarantine_path:
        blocked_roots.append(os.path.normpath(quarantine_path))
    with open(output_path, "ab") as output:
        for file_path in iter_nul_paths(input_path):
            if any(path_is_within(file_path, blocked_root) for blocked_root in blocked_roots):
                continue
            output.write(os.fsencode(file_path))
            output.write(b"\0")
            written += 1
    return written


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, dest="input_path")
    parser.add_argument("--output", required=True, dest="output_path")
    parser.add_argument("--exclude-paths", default="")
    parser.add_argument("--ignore-paths", default="")
    parser.add_argument("--quarantine-path", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        append_filtered_paths(
            args.input_path,
            args.output_path,
            split_path_list(args.exclude_paths),
            split_path_list(args.ignore_paths),
            args.quarantine_path,
        )
    except (OSError, ValueError) as exc:
        print(f"[ERROR] Could not filter scan list: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
