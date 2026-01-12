from typing import Literal, List, Dict, Any
from fastapi import FastAPI, Query
import httpx
from fastapi.middleware.cors import CORSMiddleware
import base64
from urllib.parse import unquote
import asyncio
import os
import json
import time
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

def uri_to_id(uri: str) -> str:
    return base64.urlsafe_b64encode(uri.encode("utf-8")).decode("ascii").rstrip("=")

def id_to_uri(id_: str) -> str:
    padded = id_ + "=" * (-len(id_) % 4)
    return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")


app = FastAPI(
    title="WADe DBpedia BFF",
    version="0.2.0",
    description="Backend-for-Frontend that queries DBpedia SPARQL and exposes stable REST endpoints."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


DBPEDIA_SPARQL = "https://dbpedia.org/sparql"

from fastapi import HTTPException
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

def sparql_escape_str(s: str) -> str:
    # safest minimal escaping for SPARQL string literal in double quotes
    return (
        s.replace("\\", "\\\\")
         .replace('"', '\\"')
         .replace("\n", " ")
         .replace("\r", " ")
    )

# ---------------------------
# Simple persistent disk cache (JSON) with TTL
# ---------------------------

CACHE_TTL_SECONDS = int(os.getenv("WADE_CACHE_TTL_SECONDS", str(12 * 60 * 60)))  # 12 hours
CACHE_DIR = Path(os.getenv("WADE_CACHE_DIR", ".wade_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_cache_lock = asyncio.Lock()
_inflight = {}  # type: Dict[str, asyncio.Task]

def _normalize_sparql(q: str) -> str:
    # Make formatting differences less likely to miss the cache
    return " ".join(q.split())

def _cache_key(query: str) -> str:
    h = hashlib.sha256()
    h.update(DBPEDIA_SPARQL.encode("utf-8"))
    h.update(b"\n")
    h.update(_normalize_sparql(query).encode("utf-8"))
    return h.hexdigest()

def _cache_path(key: str) -> Path:
    # store each entry as one JSON file
    return CACHE_DIR / f"{key}.json"

def _read_cache_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def _write_cache_file_atomic(path: Path, payload: Dict[str, Any]) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    tmp.replace(path)

async def cache_get(key: str) -> Optional[Dict[str, Any]]:
    path = _cache_path(key)
    if not path.exists():
        return None
    try:
        obj = _read_cache_file(path)
        expires_at = float(obj.get("expires_at", 0))
        if expires_at <= time.time():
            # expired
            try:
                path.unlink()
            except Exception:
                pass
            return None
        return obj.get("value")
    except Exception:
        # corrupted cache entry -> delete it
        try:
            path.unlink()
        except Exception:
            pass
        return None

async def cache_set(key: str, value: Dict[str, Any]) -> None:
    path = _cache_path(key)
    payload = {
        "created_at": time.time(),
        "expires_at": time.time() + CACHE_TTL_SECONDS,
        "value": value,
    }
    try:
        _write_cache_file_atomic(path, payload)
    except Exception:
        # caching is best-effort; never break API calls because cache failed
        pass


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
    retry=retry_if_exception_type((httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError)),
)
async def run_sparql(query: str) -> Dict[str, Any]:
    key = _cache_key(query)

    # 1) Cache hit
    cached = await cache_get(key)
    if cached is not None:
        return cached

    async def _do_fetch() -> Dict[str, Any]:
        # NOTE: query must be closed over; key computed outside
        params = {"query": query, "format": "application/sparql-results+json"}
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=10.0)

        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                r = await client.get(
                    DBPEDIA_SPARQL,
                    params=params,
                    headers={"Accept": "application/sparql-results+json"},
                )
                r.raise_for_status()
                data = r.json()

                # cache successful responses
                await cache_set(key, data)
                return data

            except httpx.TimeoutException as e:
                raise HTTPException(status_code=504, detail="SPARQL endpoint timed out") from e
            except httpx.HTTPStatusError as e:
                raise HTTPException(status_code=502, detail=f"SPARQL endpoint error: {e.response.status_code}") from e
            except httpx.HTTPError as e:
                raise HTTPException(status_code=502, detail="SPARQL request failed") from e

    # 2) Single-flight: reuse in-flight task
    async with _cache_lock:
        # re-check cache inside lock (avoid thundering herd)
        cached2 = await cache_get(key)
        if cached2 is not None:
            return cached2

        task = _inflight.get(key)
        if task is None:
            task = asyncio.create_task(_do_fetch())
            _inflight[key] = task

    try:
        return await task
    finally:
        # Only remove if we're removing the same task instance
        async with _cache_lock:
            if _inflight.get(key) is task:
                _inflight.pop(key, None)


@app.get("/health")
def health():
    return {"ok": True}

@app.get("/sparql")
async def sparql(q: str = Query(..., description="SPARQL query string")):
    return await run_sparql(q)

@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=1),
    kind: str = Query("all", regex="^(all|category|entity)$"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    needle = sparql_escape_str(q)

    # --- ONE count query gives entityTotal + categoryTotal ---
    counts_q = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

SELECT
  (SUM(?isEnt) AS ?entityTotal)
  (SUM(?isCat) AS ?categoryTotal)
WHERE {{
  ?s rdfs:label ?label .
  FILTER(lang(?label)="en") .
  FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{needle}"))) .

  BIND(IF(CONTAINS(STR(?s), "Category:"), 1, 0) AS ?isCat)
  BIND(IF(CONTAINS(STR(?s), "Category:"), 0, 1) AS ?isEnt)
}}
"""

    # --- Results query (paged) ---
    filter_kind = ""
    if kind == "entity":
        filter_kind = 'FILTER(!CONTAINS(STR(?s), "Category:"))'
    elif kind == "category":
        filter_kind = 'FILTER(CONTAINS(STR(?s), "Category:"))'

    results_q = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?s (SAMPLE(?label) AS ?label)
WHERE {{
  ?s rdfs:label ?label .
  FILTER(lang(?label)="en") .
  FILTER(CONTAINS(LCASE(STR(?label)), LCASE("{needle}"))) .
  {filter_kind}
}}
GROUP BY ?s
LIMIT {limit}
OFFSET {offset}
"""

    # run both in parallel (still only 2 calls)
    counts_data, data = await asyncio.gather(
        run_sparql(counts_q),
        run_sparql(results_q),
    )

    def extract_sum(d, key: str) -> int:
        b = d.get("results", {}).get("bindings", [])
        if not b:
            return 0
        v = b[0].get(key, {}).get("value", "0")
        try:
            return int(float(v))
        except Exception:
            return 0

    entity_total = extract_sum(counts_data, "entityTotal")
    category_total = extract_sum(counts_data, "categoryTotal")

    if kind == "entity":
        total = entity_total
    elif kind == "category":
        total = category_total
    else:
        total = entity_total + category_total

    bindings = data.get("results", {}).get("bindings", [])
    results = []
    for b in bindings:
        uri = b["s"]["value"]
        label = b.get("label", {}).get("value", uri.split("/")[-1])
        is_category = "Category:" in uri
        results.append({
        "uri": uri,
        "id": uri_to_id(uri),
        "label": label,
        "kind": "category" if is_category else "entity",
    })


    return {
        "query": q,
        "kind": kind,
        "limit": limit,
        "offset": offset,
        "count": len(results),
        "total": total,
        "entityTotal": entity_total,
        "categoryTotal": category_total,
        "results": results,
    }


@app.get("/api/category/{id_}")
async def category_details(id_: str):
    cat_uri = id_to_uri(id_)
    cat_uri = cat_uri.replace("https://dbpedia.org/", "http://dbpedia.org/")
    sparql_query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX dct:  <http://purl.org/dc/terms/>

SELECT ?label ?broader ?broaderLabel ?narrower ?narrowerLabel
       (COUNT(DISTINCT ?entity) AS ?entityCount)
WHERE {{
  OPTIONAL {{ <{cat_uri}> rdfs:label ?label FILTER(lang(?label)="en") . }}
  OPTIONAL {{
    <{cat_uri}> skos:broader ?broader .
    OPTIONAL {{ ?broader rdfs:label ?broaderLabel FILTER(lang(?broaderLabel)="en") }}
  }}
  OPTIONAL {{
    ?narrower skos:broader <{cat_uri}> .
    OPTIONAL {{ ?narrower rdfs:label ?narrowerLabel FILTER(lang(?narrowerLabel)="en") }}
  }}
  OPTIONAL {{ ?entity dct:subject <{cat_uri}> . }}
}}
GROUP BY ?label ?broader ?broaderLabel ?narrower ?narrowerLabel
LIMIT 500
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    label = None
    broader = {}
    narrower = {}
    entity_count = 0

    for b in bindings:
        if "label" in b and label is None:
            label = b["label"]["value"]

        if "entityCount" in b:
            try:
                entity_count = int(b["entityCount"]["value"])
            except Exception:
                pass

        if "broader" in b:
            uri = b["broader"]["value"]
            broader[uri] = {
                "uri": uri,
                "id": uri_to_id(uri),
                "label": b.get("broaderLabel", {}).get("value", uri.split("/")[-1]),
            }

        if "narrower" in b:
            uri = b["narrower"]["value"]
            narrower[uri] = {
                "uri": uri,
                "id": uri_to_id(uri),
                "label": b.get("narrowerLabel", {}).get("value", uri.split("/")[-1]),
            }

    return {
        "uri": cat_uri,
        "id": id_,
        "label": label or cat_uri.split("/")[-1],
        "broader": list(broader.values()),
        "narrower": list(narrower.values()),
        "entityCount": entity_count,
    }

@app.get("/api/category/{id_}/entities")
async def category_entities(id_: str, limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0)):
    cat_uri = id_to_uri(id_)
    cat_uri = cat_uri.replace("https://dbpedia.org/", "http://dbpedia.org/")
    sparql_query = f"""
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?entity (SAMPLE(?label) AS ?label)
WHERE {{
  ?entity dct:subject <{cat_uri}> .
  OPTIONAL {{ ?entity rdfs:label ?label FILTER(lang(?label)="en") }}
}}
GROUP BY ?entity
ORDER BY ?entity
LIMIT {limit}
OFFSET {offset}
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    results = []
    for b in bindings:
        uri = b["entity"]["value"]
        results.append({
            "uri": uri,
            "id": uri_to_id(uri),
            "label": b.get("label", {}).get("value", uri.split("/")[-1]),
        })

    return {
        "categoryUri": cat_uri,
        "categoryId": id_,
        "limit": limit,
        "offset": offset,
        "count": len(results),
        "results": results,
    }


@app.get("/api/entity/{id_}")
async def entity_details(id_: str):
    ent_uri = id_to_uri(id_)

    sparql_query = f"""
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dbo:  <http://dbpedia.org/ontology/>
PREFIX dct:  <http://purl.org/dc/terms/>

SELECT ?label ?abstract ?type ?cat ?catLabel
WHERE {{
  OPTIONAL {{ <{ent_uri}> rdfs:label ?label FILTER(lang(?label)="en") . }}
  OPTIONAL {{ <{ent_uri}> dbo:abstract ?abstract FILTER(lang(?abstract)="en") . }}
  OPTIONAL {{ <{ent_uri}> a ?type . }}
  OPTIONAL {{
    <{ent_uri}> dct:subject ?cat .
    OPTIONAL {{ ?cat rdfs:label ?catLabel FILTER(lang(?catLabel)="en") }}
  }}
}}
LIMIT 500
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    label = None
    abstract = None
    types = set()
    cats = {}

    for b in bindings:
        if "label" in b and label is None:
            label = b["label"]["value"]
        if "abstract" in b and abstract is None:
            abstract = b["abstract"]["value"]
        if "type" in b:
            types.add(b["type"]["value"])
        if "cat" in b:
            uri = b["cat"]["value"]
            cats[uri] = {
                "uri": uri,
                "id": uri_to_id(uri),
                "label": b.get("catLabel", {}).get("value", uri.split("/")[-1]),
            }

    return {
        "uri": ent_uri,
        "id": id_,
        "label": label or ent_uri.split("/")[-1],
        "abstract": abstract,
        "types": sorted(types),
        "categories": list(cats.values()),
    }

@app.get("/api/category/{id_}/facets/types")
async def category_type_facets(id_: str, limit: int = Query(15, ge=1, le=50)):
    cat_uri = id_to_uri(id_)
    cat_uri = cat_uri.replace("https://dbpedia.org/", "http://dbpedia.org/")
    sparql_query = f"""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?type (COUNT(DISTINCT ?e) AS ?count)
WHERE {{
  ?e dct:subject <{cat_uri}> .
  ?e rdf:type ?type .
}}
GROUP BY ?type
ORDER BY DESC(?count)
LIMIT {limit}
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    facets = []
    for b in bindings:
        facets.append({
            "type": b["type"]["value"],
            "count": int(b["count"]["value"]),
        })

    return {
        "categoryUri": cat_uri,
        "categoryId": id_,
        "limit": limit,
        "facets": facets,
    }

@app.get("/api/category/{id_}/entitiesByType")
async def category_entities_by_type(
    id_: str,
    types: str = Query("", description="Comma-separated list of type URIs"),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    cat_uri = id_to_uri(id_)
    cat_uri = cat_uri.replace("https://dbpedia.org/", "http://dbpedia.org/")
    type_list = [t.strip() for t in types.split(",") if t.strip()]

    values_block = ""
    type_triple = ""
    if type_list:
        values = " ".join(f"<{t}>" for t in type_list)
        values_block = f"VALUES ?t {{ {values} }}"
        type_triple = "?entity a ?t ."

    sparql_query = f"""
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?entity (SAMPLE(?label) AS ?label)
WHERE {{
  ?entity dct:subject <{cat_uri}> .
  {values_block}
  {type_triple}
  OPTIONAL {{ ?entity rdfs:label ?label FILTER(lang(?label)="en") }}
}}
GROUP BY ?entity
ORDER BY ?entity
LIMIT {limit}
OFFSET {offset}
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    results = []
    for b in bindings:
        uri = b["entity"]["value"]
        results.append({
            "uri": uri,
            "id": uri_to_id(uri),
            "label": b.get("label", {}).get("value", uri.split("/")[-1]),
        })

    return {
        "categoryUri": cat_uri,
        "categoryId": id_,
        "types": type_list,
        "limit": limit,
        "offset": offset,
        "count": len(results),
        "results": results,
    }

@app.get("/api/entity/{id_}/related")
async def entity_related(id_: str, limit: int = Query(10, ge=1, le=50)):
    entity_uri = id_to_uri(id_)
    entity_uri = entity_uri.replace("https://dbpedia.org/", "http://dbpedia.org/")

    sparql_query = f"""
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?other (SAMPLE(?label) AS ?label) (COUNT(DISTINCT ?cat) AS ?shared)
WHERE {{
  <{entity_uri}> dct:subject ?cat .
  ?other dct:subject ?cat .
  FILTER(?other != <{entity_uri}>) .
  OPTIONAL {{ ?other rdfs:label ?label FILTER(lang(?label)="en") }}
}}
GROUP BY ?other
ORDER BY DESC(?shared)
LIMIT {limit}
"""
    data = await run_sparql(sparql_query)
    bindings = data.get("results", {}).get("bindings", [])

    results = []
    for b in bindings:
        uri = b["other"]["value"]
        results.append({
            "uri": uri,
            "id": uri_to_id(uri),
            "label": b.get("label", {}).get("value", uri.split("/")[-1]),
            "shared": int(b.get("shared", {}).get("value", "0")),
        })

    return {
        "entityUri": entity_uri,
        "entityId": id_,
        "limit": limit,
        "count": len(results),
        "results": results,
    }