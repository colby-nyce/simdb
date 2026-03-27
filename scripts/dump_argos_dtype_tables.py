#!/usr/bin/env python3
"""Pretty-print Argos collection SQLite tables: DataTypeSchemas, DataTypeNodes, DataTypeEnumMembers.

DataTypeNodes includes a Description column (after Name) when the DB was created with a recent
schema; use --description-max-width to allow longer text before truncation (default 96).
"""

import argparse
import sqlite3
import sys
from typing import Any, List, Sequence, Tuple

TABLES = ("DataTypeSchemas", "DataTypeNodes", "DataTypeEnumMembers")


def _cell_str(val):
    # type: (Any) -> str
    if val is None:
        return ""
    return str(val)


def _column_widths(headers, rows, min_w, max_w, col_max_overrides=None):
    # type: (Sequence[str], Sequence[Sequence[Any]], int, int, Any) -> List[int]
    overrides = col_max_overrides or {}
    widths = []  # type: List[int]
    for col_idx, header in enumerate(headers):
        cap = int(overrides.get(header, max_w))
        w = max(min_w, len(header))
        for row in rows:
            w = max(w, len(_cell_str(row[col_idx])))
        widths.append(min(w, cap))
    return widths


def _pad_cell(text, width):
    # type: (str, int) -> str
    if len(text) <= width:
        return text.ljust(width)
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def print_fixed_table(title, headers, rows, min_w, max_w, col_max_overrides=None):
    # type: (str, Sequence[str], Sequence[Sequence[Any]], int, int, Any) -> None
    if not headers:
        print("\n## {}\n  (no columns)".format(title))
        return

    widths = _column_widths(headers, rows, min_w, max_w, col_max_overrides)
    sep_inner = "+".join("-" * (w + 2) for w in widths)
    rule = "+{}+".format(sep_inner)

    print("\n## {}".format(title))
    print(rule)
    head_cells = [_pad_cell(h, widths[i]) for i, h in enumerate(headers)]
    print("| {} |".format(" | ".join(head_cells)))
    print(rule)
    for row in rows:
        cells = [_pad_cell(_cell_str(row[i]), widths[i]) for i in range(len(headers))]
        print("| {} |".format(" | ".join(cells)))
    print(rule)
    print("  ({} row(s))".format(len(rows)))


def table_exists(conn, name):
    # type: (sqlite3.Connection, str) -> bool
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (name,),
    )
    return cur.fetchone() is not None


def _quote_ident(ident):
    # type: (str) -> str
    return '"{}"'.format(ident.replace('"', '""'))


def fetch_ordered(conn, table, columns):
    # type: (sqlite3.Connection, str, Sequence[str]) -> List[Tuple[Any, ...]]
    cols_sql = ", ".join(_quote_ident(c) for c in columns)
    order = "Id" if "Id" in columns else columns[0]
    sql = "SELECT {} FROM {} ORDER BY {}".format(cols_sql, _quote_ident(table), _quote_ident(order))
    cur = conn.execute(sql)
    return cur.fetchall()


def main():
    # type: () -> int
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "db",
        nargs="?",
        default="test.db",
        help="Path to SimDB SQLite file (default: test.db in current directory)",
    )
    p.add_argument(
        "--min-width",
        type=int,
        default=8,
        metavar="N",
        help="Minimum column width (default: 8)",
    )
    p.add_argument(
        "--max-width",
        type=int,
        default=48,
        metavar="N",
        help="Maximum column width; longer cells truncated (default: 48)",
    )
    p.add_argument(
        "--description-max-width",
        type=int,
        default=96,
        metavar="N",
        help="Max width for the DataTypeNodes Description column (default: 96)",
    )
    args = p.parse_args()

    if args.min_width < 1 or args.max_width < args.min_width:
        print("error: need 1 <= min-width <= max-width", file=sys.stderr)
        return 2

    try:
        conn = sqlite3.connect("file:{}?mode=ro".format(args.db), uri=True)
    except sqlite3.Error as e:
        print("error: cannot open database {!r}: {}".format(args.db, e), file=sys.stderr)
        return 1

    try:
        for table in TABLES:
            if not table_exists(conn, table):
                print(
                    "\n## {}\n  (table missing — run collector to create schema)".format(table)
                )
                continue
            cur = conn.execute('PRAGMA table_info("{}")'.format(table.replace('"', '""')))
            columns = [row[1] for row in cur.fetchall()]
            rows = fetch_ordered(conn, table, columns)
            col_max_overrides = None
            if table == "DataTypeNodes" and "Description" in columns:
                col_max_overrides = {"Description": max(args.max_width, args.description_max_width)}
            print_fixed_table(
                table,
                columns,
                rows,
                args.min_width,
                args.max_width,
                col_max_overrides=col_max_overrides,
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
