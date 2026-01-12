// apps/spa/src/pages/SearchPage.tsx
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { search } from "../lib/api";
import type { SearchKind, SearchResult } from "../lib/api";

export default function SearchPage() {
  // user typing (NO fetch)
  const [qInput, setQInput] = useState("");

  // only changes when user presses Search (DO fetch)
  const [submittedQ, setSubmittedQ] = useState("");

  const [kind, setKind] = useState<SearchKind>("all");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [results, setResults] = useState<SearchResult[]>([]);

  const LIMIT = 20;
  const [page, setPage] = useState(1);

  const [total, setTotal] = useState(0);
  const [entityTotal, setEntityTotal] = useState(0);
  const [categoryTotal, setCategoryTotal] = useState(0);

  // When user changes kind, keep submitted query but reset to page 1
  useEffect(() => {
    setPage(1);
  }, [kind]);

  // Fetch ONLY when submittedQ/kind/page changes
  useEffect(() => {
    let cancelled = false;

    if (submittedQ.trim().length < 2) {
      setResults([]);
      setTotal(0);
      setEntityTotal(0);
      setCategoryTotal(0);
      setErr(null);
      return;
    }

    setLoading(true);
    setErr(null);

    const offset = (page - 1) * LIMIT;

    search(submittedQ.trim(), kind, LIMIT, offset)
      .then((data) => {
        if (cancelled) return;
        setResults(data.results ?? []);
        setTotal(data.total ?? 0);
        setEntityTotal(data.entityTotal ?? 0);
        setCategoryTotal(data.categoryTotal ?? 0);
      })
      .catch((e) => {
        if (cancelled) return;
        setErr(String(e));
        setResults([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [submittedQ, kind, page]);

  const totalPages = Math.max(1, Math.ceil(total / LIMIT));

  function doSearch() {
    const trimmed = qInput.trim();
    setPage(1); // always start at page 1 for a new search
    setSubmittedQ(trimmed);
  }

  return (
    <div style={{ maxWidth: 900, margin: "40px auto", fontFamily: "system-ui, sans-serif" }}>
      <h1 style={{ marginBottom: 8 }}>DBpedia Explorer (WADe)</h1>
      <p style={{ marginTop: 0, opacity: 0.8 }}>Search DBpedia entities and categories.</p>

      {/* Form = Enter triggers search */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          doSearch();
        }}
        style={{ display: "flex", gap: 12, alignItems: "center", margin: "16px 0" }}
      >
        <input
          value={qInput}
          onChange={(e) => setQInput(e.target.value)}
          placeholder="Search… (min 2 chars)"
          style={{ flex: 1, padding: 10, fontSize: 16 }}
        />

        <select
          value={kind}
          onChange={(e) => setKind(e.target.value as SearchKind)}
          style={{ padding: 10 }}
        >
          <option value="all">All</option>
          <option value="entity">Entities</option>
          <option value="category">Categories</option>
        </select>

        <button
          type="submit"
          style={{
            border: "1px solid #ddd",
            padding: "10px 14px",
            borderRadius: 10,
            cursor: "pointer",
            fontWeight: 700,
            background: "white",
          }}
        >
          Search
        </button>
      </form>

      {/* Totals */}
      {submittedQ.trim().length >= 2 && (
        <div style={{ fontSize: 13, opacity: 0.85, marginBottom: 10 }}>
          Showing <strong>{results.length}</strong> of <strong>{total}</strong> result(s) for{" "}
          <strong>{submittedQ}</strong> — entities: <strong>{entityTotal}</strong>, categories:{" "}
          <strong>{categoryTotal}</strong>
        </div>
      )}

      {loading && <div>Loading…</div>}
      {err && <div style={{ color: "crimson" }}>{err}</div>}

      <ul style={{ paddingLeft: 0, listStyle: "none" }}>
        {results.map((r) => {
          const href = r.kind === "category" ? `/category/${r.id}` : `/entity/${r.id}`;

          return (
            <li key={r.uri} style={{ marginBottom: 10 }}>
              <Link
                to={href}
                style={{
                  display: "block",
                  border: "1px solid #ddd",
                  padding: "10px 12px",
                  borderRadius: 10,
                  textDecoration: "none",
                  color: "inherit",
                  background: "white",
                }}
              >
                <div style={{ fontWeight: 700 }}>[{r.kind}] {r.label}</div>
                <div style={{ fontSize: 12, opacity: 0.7 }}>{r.uri}</div>
              </Link>
            </li>
          );
        })}
      </ul>

      {/* Pagination */}
      {submittedQ.trim().length >= 2 && total > LIMIT && (
        <div style={{ display: "flex", gap: 10, alignItems: "center", marginTop: 12 }}>
          <button
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={page <= 1 || loading}
            style={{
              border: "1px solid #ddd",
              padding: "8px 10px",
              borderRadius: 10,
              cursor: page <= 1 || loading ? "not-allowed" : "pointer",
              background: "white",
            }}
          >
            ← Prev
          </button>

          <div style={{ fontSize: 13, opacity: 0.85 }}>
            Page <strong>{page}</strong> / <strong>{totalPages}</strong>
          </div>

          <button
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={page >= totalPages || loading}
            style={{
              border: "1px solid #ddd",
              padding: "8px 10px",
              borderRadius: 10,
              cursor: page >= totalPages || loading ? "not-allowed" : "pointer",
              background: "white",
            }}
          >
            Next →
          </button>
        </div>
      )}
    </div>
  );
}
