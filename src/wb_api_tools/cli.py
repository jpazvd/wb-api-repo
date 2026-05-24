"""Command-line interface for ``wb-api-tools``.

Invoked as the ``wb-api-tools`` console script (declared in
``pyproject.toml`` ``[project.scripts]``) or via ``python -m wb_api_tools``.

The subcommand surface is documented in
``docs/PYTHON_USER_GUIDE.md`` and via ``wb-api-tools <subcmd> --help``.
"""

from __future__ import annotations

import argparse
import sys
from typing import Optional

import pandas as pd

from . import data as _data
from .data import (
    get_country_metadata,
    get_indicator_metadata,
    get_data,
)


def _save_df(df, out: Optional[str]) -> None:
    """Persist a DataFrame to disk in CSV / parquet / YAML, or print a preview."""
    if not out:
        print(df.head(20).to_string(index=False))
        return
    out = out.strip(); lower = out.lower()
    if lower.endswith(".csv"):
        df.to_csv(out, index=False)
    elif lower.endswith(".parquet"):
        df.to_parquet(out, index=False)
    elif lower.endswith(".yaml") or lower.endswith(".yml"):
        try:
            import yaml
        except Exception as e:
            raise SystemExit("Install PyYAML for YAML output: pip install pyyaml") from e
        records = df.to_dict(orient="records")
        with open(out, "w", encoding="utf-8") as f:
            yaml.safe_dump(records, f, sort_keys=False, allow_unicode=True)
    else:
        df.to_csv(out, index=False)
    print(f"Wrote: {out}  (rows={len(df):,}, cols={len(df.columns)})")


def build_parser():
    """Build the ``argparse`` tree for the ``wb-api-tools`` CLI.

    Returns the top-level parser. Subcommands map 1:1 onto the public
    library surface (see :mod:`wb_api_tools.discovery`, :func:`get_data`,
    :func:`enrich_country_context`). Exposed so tests and external
    callers can introspect the parser without running ``main()``.
    """
    p = argparse.ArgumentParser(description="World Bank API helper")
    p.add_argument("--verbose", action="store_true", help="Show debug output (verbose)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_c = sub.add_parser("countries", help="Fetch country metadata")
    p_c.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    p_i = sub.add_parser("indicators", help="Fetch indicator metadata")
    p_i.add_argument("--codes", help="Comma-separated indicator codes (e.g. SP.POP.TOTL,NY.GDP.MKTP.CD)")
    p_i.add_argument("--search", help="Substring to search across indicator names")
    p_i.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    p_d = sub.add_parser("data", help="Fetch indicator data")
    p_d.add_argument("--indicators", required=True,
                     help="Comma-separated indicator codes (e.g. SP.POP.TOTL,NY.GDP.MKTP.CD)")
    p_d.add_argument("--countries", default="all",
                     help="Semicolon-separated ISO3 codes (e.g. BRA;USA;IND), 'all', or aggregate code")
    p_d.add_argument("--date", help="Year or year range (e.g. 2020 or 2010:2020)")
    p_d.add_argument("--long", action="store_true",
                     help="Emit long (tidy) format instead of wide")
    p_d.add_argument("--no-basic", action="store_true",
                     help="Skip the 8-field country-context auto-merge (Phase 5 parity)")
    p_d.add_argument("--geo", action="store_true",
                     help="Also merge capital/latitude/longitude (PR C; combinable with --no-basic for geo-only)")
    p_d.add_argument("--language", default=None,
                     help="ISO-639-1 code (es, fr); en/None uses default endpoint (PR C)")
    p_d.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    # --- Discovery subcommands (PR B) --------------------------------
    # All read from the YAML metadata cache (see wb_api_tools.cache);
    # populate via the `sync` subcommand below.
    p_src = sub.add_parser("sources", help="List WB data sources")
    p_src.add_argument("--limit", type=int, default=20,
                       help="Max sources to show (default 20; pass --all for no cap)")
    p_src.add_argument("--all", action="store_true", help="No limit (equivalent to allsources)")
    p_src.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    p_top = sub.add_parser("alltopics", help="List all WB topic categories")
    p_top.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    p_info = sub.add_parser("info", help="Show full metadata for one indicator (from YAML cache)")
    p_info.add_argument("id", help="Indicator code, e.g. SP.POP.TOTL")

    p_desc = sub.add_parser("describe", help="Fetch fresh metadata for one indicator (from WB API)")
    p_desc.add_argument("id", help="Indicator code, e.g. SP.POP.TOTL")
    p_desc.add_argument("--language", default=None,
                        help="ISO-639-1 code (es, fr); en/None uses default endpoint (PR C)")

    p_srch = sub.add_parser("search", help="Paginated indicator search")
    p_srch.add_argument("term", nargs="?", default="", help="Substring to search (or empty for browse-mode)")
    p_srch.add_argument("--page", type=int, default=1, help="1-based page index (default 1)")
    p_srch.add_argument("--limit", type=int, default=20, help="Results per page (default 20)")
    p_srch.add_argument("--source", help="Filter by source ID")
    p_srch.add_argument("--topic", help="Filter by topic ID")
    p_srch.add_argument("--field", default="name+description",
                        help='Search field(s): name | description | note | code | name+description | all')
    p_srch.add_argument("--exact", action="store_true", help="Exact code match (use with --field code)")
    p_srch.add_argument("--out", help="Output path (.csv, .parquet, .yaml, or .yml); prints to stdout if omitted")

    p_sync = sub.add_parser("sync", help="Refresh YAML metadata cache from WB API (Phase 1 pipeline)")
    p_sync.add_argument("--save-raw", action="store_true", dest="save_raw",
                        help="Persist raw API JSON snapshots alongside generated YAML")
    p_sync.add_argument("--no-validate", action="store_true", dest="no_validate",
                        help="Skip schema validation of generated YAML")
    p_sync.add_argument("--skip-diff", action="store_true", dest="skip_diff",
                        help="Skip diff analysis against the previous YAML cache")
    p_sync.add_argument("--commit", action="store_true",
                        help="git-commit the regenerated YAML cache when the pipeline succeeds")
    p_sync.add_argument("--tag", action="store_true",
                        help="Create a metadata-vYYYYMMDD git tag after committing (requires --commit)")

    return p


def main(argv=None):
    """CLI entrypoint.

    Parses ``argv`` (defaults to :data:`sys.argv` when ``None``),
    dispatches to the selected subcommand, and writes results to
    ``--out`` or stdout. Returns ``0`` on success and ``1`` on
    handled errors; the ``__main__`` guard forwards the value to
    :func:`sys.exit`.
    """
    if argv is None:
        argv = sys.argv[1:]
    args = build_parser().parse_args(argv)
    # Flip the data module's VERBOSE flag (get_data() reads it).
    _data.VERBOSE = args.verbose
    if _data.VERBOSE:
        print(f"Debug-main args: cmd={args.cmd}, indicators={getattr(args, 'indicators', None)}, countries={getattr(args, 'countries', None)}, date={getattr(args, 'date', None)}, long={getattr(args, 'long', None)}, out={getattr(args, 'out', None)}")
    if args.cmd == "countries":
        df = get_country_metadata()
        _save_df(df, args.out)
    elif args.cmd == "indicators":
        codes = [c.strip() for c in (args.codes or "").split(",") if c.strip()] or None
        df = get_indicator_metadata(codes=codes, search=args.search)
        _save_df(df, args.out)
    elif args.cmd == "data":
        df = get_data(indicators=args.indicators, countries=args.countries,
                      date=args.date, long=args.long,
                      no_basic=args.no_basic, geo=args.geo, language=args.language)
        if _data.VERBOSE:
            print(f"Debug-final df shape: {df.shape}")
            if not df.empty:
                print(df.head(5).to_string(index=False))
        _save_df(df, args.out)
    elif args.cmd in ("sources", "alltopics", "info", "describe", "search", "sync"):
        # Discovery subcommands delegate to wb_api_tools.discovery.
        # Imported lazily so simple `wb-api-tools data` invocations
        # don't pay the YAML-cache-resolution cost.
        from .discovery import sources, allsources, alltopics, info, describe, search, sync
        if args.cmd == "sources":
            recs = allsources() if args.all else sources(limit=args.limit)
            df = pd.DataFrame.from_records(recs)
            _save_df(df, args.out)
        elif args.cmd == "alltopics":
            df = pd.DataFrame.from_records(alltopics())
            _save_df(df, args.out)
        elif args.cmd == "info":
            rec = info(args.id)
            if rec is None:
                print(f"Indicator not found in YAML cache: {args.id}")
                return 1
            for k, v in rec.items():
                print(f"  {k}: {v}")
        elif args.cmd == "describe":
            rec = describe(args.id, language=args.language)
            if rec is None:
                print(f"Indicator not found via WB API: {args.id}")
                return 1
            for k, v in rec.items():
                print(f"  {k}: {v}")
        elif args.cmd == "search":
            res = search(args.term, page=args.page, limit=args.limit,
                         source=args.source, topic=args.topic,
                         field=args.field, exact=args.exact)
            print(f"  total={res['total']}  page={res['page']}/{res['pages']}  limit={res['limit']}")
            if args.out:
                _save_df(pd.DataFrame.from_records(res['results']), args.out)
            else:
                for r in res['results']:
                    print(f"  [{r.get('code'):<20}] {r.get('name')}")
        elif args.cmd == "sync":
            sub_argv = []
            if args.save_raw:    sub_argv.append("--save-raw")
            if args.no_validate: sub_argv.append("--no-validate")
            if args.skip_diff:   sub_argv.append("--skip-diff")
            if args.commit:      sub_argv.append("--commit")
            if args.tag:         sub_argv.append("--tag")
            return sync(sub_argv)
    return 0
