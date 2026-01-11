#!/usr/bin/env python3
"""
wikiviz ingestion pipeline (extensible)

Reads:
- enwiki-latest-page.sql.gz
- enwiki-latest-categorylinks.sql.gz
- enwiki-latest-linktarget.sql.gz
- pageviews-YYYYMMDD-HH0000.gz

Writes:
- wikiviz/data/rdf/wikipedia.ttl
- wikiviz/data/rdf/pageviews.ttl

Design goals:
- streaming (no full decompression to disk)
- configurable subset sizing
- easy to extend with more sources/transforms/sinks
"""

from __future__ import annotations

import time
import argparse
import gzip
import io
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple


# -----------------------------
# Config
# -----------------------------

class ProgressLine:
    """
    One-line progress indicator based on compressed bytes read.
    Updates at most every `min_interval` seconds.
    """
    def __init__(self, label: str, total_bytes: int, min_interval: float = 0.5):
        self.label = label
        self.total = max(total_bytes, 1)
        self.min_interval = min_interval
        self._last = 0.0
        self._done = False

    def update(self, current_bytes: int):
        now = time.time()
        if (now - self._last) < self.min_interval and current_bytes < self.total:
            return
        self._last = now
        pct = (current_bytes / self.total) * 100.0
        # keep it compact
        msg = f"\r{self.label}: {pct:6.2f}%  ({current_bytes/1e6:7.1f}/{self.total/1e6:7.1f} MB)"
        print(msg, end="", file=sys.stderr)

    def done(self):
        if self._done:
            return
        self._done = True
        self.update(self.total)
        print(file=sys.stderr)  # newline


def iter_gzip_lines_with_progress(path: Path, label: str) -> Iterator[str]:
    """
    Stream lines from a .gz file while showing progress based on compressed bytes read.
    """
    total = path.stat().st_size  # compressed bytes on disk
    prog = ProgressLine(label, total_bytes=total)

    with path.open("rb") as raw:
        # gzip.GzipFile reads from `raw`; raw.tell() gives compressed position.
        gz = gzip.GzipFile(fileobj=raw, mode="rb")
        text = io.TextIOWrapper(gz, encoding="utf-8", errors="replace")

        for line in text:
            prog.update(raw.tell())
            yield line

        prog.done()


@dataclass(frozen=True)
class Config:
    # Paths
    page_sql_gz: Path
    categorylinks_sql_gz: Path
    linktarget_sql_gz: Path
    pageviews_gz: Path
    out_wikipedia_ttl: Path
    out_pageviews_ttl: Path

    # Project codes / namespaces
    project_code: str = "en"              # matches pageviews project token in your file
    category_namespace: int = 14          # confirmed via linktarget namespace samples

    # Limits / sampling
    page_limit: int = 3000                # configurable subset
    require_namespace0: bool = True       # only main/article namespace

    # RDF / graphs / namespace base
    base: str = "https://wikinsight.org/"
    graph_wikipedia: str = "https://wikinsight.org/graph/wikipedia"
    graph_pageviews: str = "https://wikinsight.org/graph/pageviews"
    ns_vocab: str = "https://wikinsight.org/ns/"

    # Pageviews behavior
    skip_titles_with_colon: bool = True   # skip Category:, Template:, File:, etc.
    timestamp_iso: Optional[str] = None   # if None, derive from filename


# -----------------------------
# Helpers: safe I/O
# -----------------------------

def open_gzip_text(path: Path, encoding: str = "utf-8", errors: str = "replace") -> io.TextIOBase:
    # Wikipedia SQL dumps are text; replacement avoids crashes on odd bytes.
    return io.TextIOWrapper(gzip.open(path, "rb"), encoding=encoding, errors=errors)


# -----------------------------
# SQL INSERT parsing (streaming)
# -----------------------------

_INSERT_RE = re.compile(r"^INSERT INTO `(?P<table>[^`]+)` VALUES\s*(?P<values>.*);\s*$", re.DOTALL)

def iter_insert_tuples(sql_gz_path: Path, table: str) -> Iterator[List[str]]:
    label = f"{table}"
    for line in iter_gzip_lines_with_progress(sql_gz_path, label=label):
        line = line.strip()
        if not line.startswith("INSERT INTO"):
            continue
        m = _INSERT_RE.match(line)
        if not m:
            continue
        if m.group("table") != table:
            continue

        values_blob = m.group("values")
        for tup in _split_top_level_tuples(values_blob):
            yield _split_csv_sql(tup)


def _split_top_level_tuples(values_blob: str) -> Iterator[str]:
    """
    Splits "(a),(b),(c)" into "a", "b", "c" (without outer parentheses).
    Handles quotes and escapes.
    """
    i = 0
    n = len(values_blob)
    while i < n:
        # seek '('
        while i < n and values_blob[i] != "(":
            i += 1
        if i >= n:
            break
        i += 1  # past '('
        start = i
        depth = 1
        in_str = False
        esc = False
        while i < n:
            ch = values_blob[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == "'":
                    in_str = False
            else:
                if ch == "'":
                    in_str = True
                elif ch == "(":
                    depth += 1
                elif ch == ")":
                    depth -= 1
                    if depth == 0:
                        yield values_blob[start:i]
                        i += 1
                        break
            i += 1


def _split_csv_sql(tuple_body: str) -> List[str]:
    """
    Split tuple body by commas at top level (respecting quoted strings).
    Returns list of raw token strings.
    """
    out: List[str] = []
    buf: List[str] = []
    in_str = False
    esc = False
    for ch in tuple_body:
        if in_str:
            buf.append(ch)
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == "'":
                in_str = False
        else:
            if ch == "'":
                in_str = True
                buf.append(ch)
            elif ch == ",":
                out.append("".join(buf).strip())
                buf = []
            else:
                buf.append(ch)
    out.append("".join(buf).strip())
    return out


def sql_token_to_python(token: str) -> Optional[str]:
    """
    Convert a SQL literal token to python string (for the few fields we need).
    - NULL -> None
    - 'text' -> text (unescaped basic backslash sequences)
    - numbers -> string of number (caller can int())
    """
    if token.upper() == "NULL":
        return None
    token = token.strip()
    if token.startswith("'") and token.endswith("'"):
        inner = token[1:-1]
        # Unescape backslash-escaped sequences used by mysqldump
        # Keep it conservative; we mainly need titles which are ASCII-ish with underscores.
        inner = inner.replace("\\'", "'").replace("\\\\", "\\")
        inner = inner.replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t")
        return inner
    return token  # numbers etc.


# -----------------------------
# RDF writing (simple Turtle)
# -----------------------------

class TurtleWriter:
    def __init__(self, fp: io.TextIOBase):
        self.fp = fp
        self._wrote_prefixes = False

    def write_prefixes(self, base: str, ns_vocab: str):
        if self._wrote_prefixes:
            return
        self.fp.write(f"@base <{base}> .\n")
        self.fp.write(f"@prefix wi: <{ns_vocab}> .\n")
        self.fp.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n")
        self.fp.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")
        self.fp.write("@prefix skos: <http://www.w3.org/2004/02/skos/core#> .\n\n")
        self._wrote_prefixes = True

    def start_graph(self, graph_iri: str):
        self.fp.write(f"# Graph: {graph_iri}\n")
        self.fp.write(f"GRAPH <{graph_iri}> {{\n")

    def end_graph(self):
        self.fp.write("}\n\n")

    def triple(self, s: str, p: str, o: str):
        self.fp.write(f"  {s} {p} {o} .\n")


def iri(s: str) -> str:
    return f"<{s}>"


def lit(s: str, lang: Optional[str] = None, datatype: Optional[str] = None) -> str:
    esc = s.replace("\\", "\\\\").replace('"', '\\"')
    if lang:
        return f"\"{esc}\"@{lang}"
    if datatype:
        return f"\"{esc}\"^^<{datatype}>"
    return f"\"{esc}\""


def uri_page(base: str, project: str, title: str) -> str:
    return f"{base}page/{project}/{title}"


def uri_category(base: str, project: str, title: str) -> str:
    return f"{base}category/{project}/{title}"


# -----------------------------
# Extractors (extensible units)
# -----------------------------

@dataclass
class PageRecord:
    page_id: int
    namespace: int
    title: str


class PageExtractor:
    """
    Extract a limited set of article pages.
    Expected page table starts with:
      page_id, page_namespace, page_title, ...
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def extract(self) -> Tuple[Dict[int, str], Set[str]]:
        id_to_title: Dict[int, str] = {}
        title_set: Set[str] = set()
        count = 0
        for row in iter_insert_tuples(self.cfg.page_sql_gz, "page"):
            if len(row) < 3:
                continue
            page_id_s = sql_token_to_python(row[0])
            ns_s = sql_token_to_python(row[1])
            title = sql_token_to_python(row[2])
            if page_id_s is None or ns_s is None or title is None:
                continue

            try:
                page_id = int(page_id_s)
                ns = int(ns_s)
            except ValueError:
                continue

            if self.cfg.require_namespace0 and ns != 0:
                continue

            # Keep titles exactly as in DB (underscores)
            id_to_title[page_id] = title
            title_set.add(title)
            count += 1
            if count >= self.cfg.page_limit:
                break

        return id_to_title, title_set


class LinkTargetExtractor:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def extract_categories(self, only_ids: Optional[Set[int]] = None) -> Dict[int, str]:
        lt_to_cat: Dict[int, str] = {}
        for row in iter_insert_tuples(self.cfg.linktarget_sql_gz, "linktarget"):
            if len(row) < 3:
                continue
            lt_id_s = sql_token_to_python(row[0])
            lt_ns_s = sql_token_to_python(row[1])
            lt_title = sql_token_to_python(row[2])
            if lt_id_s is None or lt_ns_s is None or lt_title is None:
                continue
            try:
                lt_id = int(lt_id_s)
                lt_ns = int(lt_ns_s)
            except ValueError:
                continue

            if only_ids is not None and lt_id not in only_ids:
                continue

            if lt_ns == self.cfg.category_namespace:
                lt_to_cat[lt_id] = lt_title
        return lt_to_cat



class CategoryLinksExtractor:
    """
    categorylinks columns confirmed (new schema):
      cl_from, cl_sortkey, cl_timestamp, cl_sortkey_prefix, cl_type, cl_collation_id, cl_target_id
    We only need:
      cl_from (page_id), cl_target_id (lt_id)
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def extract_memberships(
        self,
        page_ids: Set[int],
        lt_to_cat: Dict[int, str],
        page_id_to_title: Dict[int, str]
    ) -> Dict[str, Set[str]]:
        page_to_cats: Dict[str, Set[str]] = {}
        for row in iter_insert_tuples(self.cfg.categorylinks_sql_gz, "categorylinks"):
            if len(row) < 7:
                continue
            cl_from_s = sql_token_to_python(row[0])
            cl_target_id_s = sql_token_to_python(row[6])
            if cl_from_s is None or cl_target_id_s is None:
                continue
            try:
                cl_from = int(cl_from_s)
                cl_target_id = int(cl_target_id_s)
            except ValueError:
                continue

            if cl_from not in page_ids:
                continue

            cat_title = lt_to_cat.get(cl_target_id)
            if not cat_title:
                continue

            page_title = page_id_to_title.get(cl_from)
            if not page_title:
                continue

            page_to_cats.setdefault(page_title, set()).add(cat_title)

        return page_to_cats

class CategoryTargetIdCollector:
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def collect(self, page_ids: Set[int]) -> Set[int]:
        needed: Set[int] = set()
        for row in iter_insert_tuples(self.cfg.categorylinks_sql_gz, "categorylinks"):
            if len(row) < 7:
                continue
            cl_from_s = sql_token_to_python(row[0])
            cl_target_id_s = sql_token_to_python(row[6])
            if cl_from_s is None or cl_target_id_s is None:
                continue
            try:
                cl_from = int(cl_from_s)
                cl_target_id = int(cl_target_id_s)
            except ValueError:
                continue
            if cl_from in page_ids:
                needed.add(cl_target_id)
        return needed


@dataclass
class PageviewObs:
    project: str
    title: str
    views: int
    bytes_: int


class PageviewsStream:
    """
    Expected format (per line):
      <project> <title> <views> <bytes>
    Example you showed:
      en !Hero 1 0
    """
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def iter_obs(self) -> Iterator[PageviewObs]:
        with open_gzip_text(self.cfg.pageviews_gz, encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Split on whitespace
                parts = line.split()
                if len(parts) < 4:
                    continue
                project, title, views_s, bytes_s = parts[0], parts[1], parts[2], parts[3]
                if project != self.cfg.project_code:
                    continue
                if self.cfg.skip_titles_with_colon and ":" in title:
                    continue
                try:
                    views = int(views_s)
                    bytes_ = int(bytes_s)
                except ValueError:
                    continue
                yield PageviewObs(project=project, title=title, views=views, bytes_=bytes_)


# -----------------------------
# Pipeline runner
# -----------------------------

def derive_timestamp_iso_from_filename(path: Path) -> Optional[str]:
    """
    For filenames like pageviews-20260101-110000.gz => 2026-01-01T11:00:00Z
    """
    m = re.search(r"pageviews-(\d{8})-(\d{6})", path.name)
    if not m:
        return None
    ymd, hms = m.group(1), m.group(2)
    dt = datetime(
        int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8]),
        int(hms[0:2]), int(hms[2:4]), int(hms[4:6]),
        tzinfo=timezone.utc
    )
    return dt.isoformat().replace("+00:00", "Z")


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def run(cfg: Config) -> None:
    print(f"[1/6] Extract pages (limit={cfg.page_limit}) from: {cfg.page_sql_gz}")
    pages = PageExtractor(cfg)
    page_id_to_title, title_set = pages.extract()
    page_ids = set(page_id_to_title.keys())
    print(f"  -> pages kept: {len(page_id_to_title)}")

    print(f"[2/6] Collect category target IDs for selected pages from: {cfg.categorylinks_sql_gz}")
    needed_target_ids = CategoryTargetIdCollector(cfg).collect(page_ids)
    print(f"  -> needed cl_target_id count: {len(needed_target_ids)}")

    print(f"[3/6] Resolve needed category targets from linktarget (namespace={cfg.category_namespace}) from: {cfg.linktarget_sql_gz}")
    lt_to_cat = LinkTargetExtractor(cfg).extract_categories(only_ids=needed_target_ids)
    print(f"  -> resolved categories: {len(lt_to_cat)}")

    print(f"[3b/6] Build page->category memberships (using resolved targets)")
    page_to_cats = CategoryLinksExtractor(cfg).extract_memberships(page_ids, lt_to_cat, page_id_to_title)

    # Write wikipedia.ttl
    ensure_parent(cfg.out_wikipedia_ttl)
    print(f"[4/6] Write Wikipedia RDF: {cfg.out_wikipedia_ttl}")
    with cfg.out_wikipedia_ttl.open("w", encoding="utf-8") as out:
        tw = TurtleWriter(out)
        tw.write_prefixes(cfg.base, cfg.ns_vocab)
        tw.start_graph(cfg.graph_wikipedia)

        # Declare pages
        for title in sorted(title_set):
            s = iri(uri_page(cfg.base, cfg.project_code, title))
            tw.triple(s, "rdf:type", "wi:Page")
            tw.triple(s, "wi:title", lit(title))
            # optional: store project
            tw.triple(s, "wi:project", lit(cfg.project_code))

        # Declare categories + memberships
        seen_cats: Set[str] = set()
        for page_title, cats in page_to_cats.items():
            s_page = iri(uri_page(cfg.base, cfg.project_code, page_title))
            for cat in cats:
                if cat not in seen_cats:
                    s_cat = iri(uri_category(cfg.base, cfg.project_code, cat))
                    tw.triple(s_cat, "rdf:type", "wi:Category")
                    tw.triple(s_cat, "wi:title", lit(cat))
                    tw.triple(s_cat, "wi:project", lit(cfg.project_code))
                    seen_cats.add(cat)
                tw.triple(s_page, "wi:inCategory", iri(uri_category(cfg.base, cfg.project_code, cat)))

        tw.end_graph()

    # Pageviews: only for selected titles
    ts = cfg.timestamp_iso or derive_timestamp_iso_from_filename(cfg.pageviews_gz) or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    print(f"[5/6] Extract pageviews for selected pages from: {cfg.pageviews_gz}")
    pv_stream = PageviewsStream(cfg)

    ensure_parent(cfg.out_pageviews_ttl)
    print(f"[6/6] Write Pageviews RDF: {cfg.out_pageviews_ttl} (timestamp={ts})")
    with cfg.out_pageviews_ttl.open("w", encoding="utf-8") as out:
        tw = TurtleWriter(out)
        tw.write_prefixes(cfg.base, cfg.ns_vocab)
        tw.start_graph(cfg.graph_pageviews)

        kept = 0
        for obs in pv_stream.iter_obs():
            if obs.title not in title_set:
                continue
            obs_iri = iri(f"{cfg.base}pageview/{cfg.project_code}/{ts}/{obs.title}")
            page_iri = iri(uri_page(cfg.base, cfg.project_code, obs.title))
            tw.triple(obs_iri, "rdf:type", "wi:PageviewObservation")
            tw.triple(obs_iri, "wi:forPage", page_iri)
            tw.triple(obs_iri, "wi:atTime", lit(ts, datatype="http://www.w3.org/2001/XMLSchema#dateTime"))
            tw.triple(obs_iri, "wi:views", lit(str(obs.views), datatype="http://www.w3.org/2001/XMLSchema#integer"))
            tw.triple(obs_iri, "wi:bytes", lit(str(obs.bytes_), datatype="http://www.w3.org/2001/XMLSchema#integer"))
            kept += 1

        tw.end_graph()
    print(f"Done. Pageviews observations kept: {kept}")


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="wikiviz ingestion pipeline (enwiki + pageviews)")
    p.add_argument("--raw-dir", default="wikiviz/data/raw", help="Directory containing the raw dump files")
    p.add_argument("--out-dir", default="wikiviz/data/rdf", help="Directory to write RDF Turtle outputs")
    p.add_argument("--pageviews", default="pageviews-20260101-110000.gz", help="Pageviews .gz filename inside raw-dir")
    p.add_argument("--page-limit", type=int, default=3000, help="How many namespace-0 pages to ingest")
    p.add_argument("--project", default="en", help="Project token in pageviews file (e.g., en, en.m)")
    return p.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    args = parse_args(argv)
    raw = Path(args.raw_dir)
    out = Path(args.out_dir)

    cfg = Config(
        page_sql_gz=raw / "enwiki-latest-page.sql.gz",
        categorylinks_sql_gz=raw / "enwiki-latest-categorylinks.sql.gz",
        linktarget_sql_gz=raw / "enwiki-latest-linktarget.sql.gz",
        pageviews_gz=raw / args.pageviews,
        out_wikipedia_ttl=out / "wikipedia.ttl",
        out_pageviews_ttl=out / "pageviews.ttl",
        page_limit=args.page_limit,
        project_code=args.project,
    )
    run(cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
