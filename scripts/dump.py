#!/usr/bin/env python3
"""Pretty-print all rows from tables in a SQLite database (read-only).

By default every non-internal table is dumped in name order (skips sqlite_* and
names starting with internal$). Use --table / -t
(one or more times) to restrict output. Wider cells in any column named
Description can use --description-max-width.
"""

import argparse
import sqlite3
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _cell_str(val: Any) -> str:
    if val is None:
        return ""
    return str(val)


def _column_widths(
    headers: Sequence[str],
    rows: Sequence[Sequence[Any]],
    min_w: int,
    max_w: int,
    col_max_overrides=None,
):
    # type: (Sequence[str], Sequence[Sequence[Any]], int, int, Optional[Dict[str, int]]) -> List[int]
    overrides = col_max_overrides or {}
    widths: List[int] = []
    for col_idx, header in enumerate(headers):
        cap = int(overrides.get(header, max_w))
        w = max(min_w, len(header))
        for row in rows:
            w = max(w, len(_cell_str(row[col_idx])))
        widths.append(min(w, cap))
    return widths


def _pad_cell(text: str, width: int) -> str:
    if len(text) <= width:
        return text.ljust(width)
    if width <= 3:
        return text[:width]
    return text[: width - 3] + "..."


def print_fixed_table(title, headers, rows, min_w, max_w, col_max_overrides=None):
    # type: (str, List[str], Sequence[Sequence[Any]], int, int, Optional[Dict[str, int]]) -> None
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


def list_user_tables(conn):
    # type: (sqlite3.Connection) -> List[str]
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' "
        "AND name NOT GLOB 'internal$*' ORDER BY name"
    )
    return [row[0] for row in cur.fetchall()]


def _quote_ident(ident: str) -> str:
    return '"{}"'.format(ident.replace('"', '""'))


def fetch_ordered(conn, table, columns):
    # type: (sqlite3.Connection, str, Sequence[str]) -> List[Tuple[Any, ...]]
    cols_sql = ", ".join(_quote_ident(c) for c in columns)
    order = "Id" if "Id" in columns else columns[0]
    sql = "SELECT {} FROM {} ORDER BY {}".format(
        cols_sql, _quote_ident(table), _quote_ident(order)
    )
    cur = conn.execute(sql)
    return cur.fetchall()


def _dedupe_preserve(names):
    # type: (Sequence[str]) -> List[str]
    seen = set()  # type: set
    out = []  # type: List[str]
    for n in names:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def main():
    # type: () -> int
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("db", help="Path to SQLite database file (read-only open).")
    p.add_argument(
        "--table",
        "-t",
        action="append",
        dest="tables",
        metavar="NAME",
        default=None,
        help="Dump only this table (repeat for multiple tables).",
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
        help="Extra width ceiling for any column named Description (default: 96)",
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
        if args.tables:
            tables_to_dump = _dedupe_preserve(args.tables)
            for name in tables_to_dump:
                if not table_exists(conn, name):
                    print(
                        "error: no such table {!r} in {!r}".format(name, args.db),
                        file=sys.stderr,
                    )
                    return 1
        else:
            tables_to_dump = list_user_tables(conn)
            if not tables_to_dump:
                print("(no user tables in {!r})".format(args.db))
                return 0

        for table in tables_to_dump:
            cur = conn.execute('PRAGMA table_info("{}")'.format(table.replace('"', '""')))
            columns = [row[1] for row in cur.fetchall()]
            if not columns:
                print("\n## {}\n  (table exists but has no columns)".format(table))
                continue
            rows = fetch_ordered(conn, table, columns)
            col_max_overrides = None
            if "Description" in columns:
                col_max_overrides = {
                    "Description": max(args.max_width, args.description_max_width)
                }
            print_fixed_table(
                table,
                list(columns),
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
