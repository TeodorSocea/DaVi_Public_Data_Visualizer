import { useEffect, useState } from "react";
import { search } from "./lib/api";
import type { SearchKind, SearchResult } from "./lib/api";
import "./extensions";

export default function App() {
  const [q, setQ] = useState("physics");
  const [kind, setKind] = useState<SearchKind>("all");
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [results, setResults] = useState<SearchResult[]>([]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErr(null);

    search(q, kind, 20)
      .then((data) => {
        if (!cancelled) setResults(data.results);
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [q, kind]);

  return (
    <div style={{ maxWidth: 900, margin: "40px auto", fontFamily: "system-ui, sans-serif" }}>
      <h1 style={{ marginBottom: 8 }}>DBpedia Explorer (WADe)</h1>
      <p style={{ marginTop: 0, opacity: 0.8 }}>
        Search DBpedia entities and categories via your BFF API.
      </p>

      <div style={{ display: "flex", gap: 12, alignItems: "center", margin: "16px 0" }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          placeholder="Search… (min 2 chars)"
          style={{ flex: 1, padding: 10, fontSize: 16 }}
        />
        <select value={kind} onChange={(e) => setKind(e.target.value as SearchKind)} style={{ padding: 10 }}>
          <option value="all">All</option>
          <option value="entity">Entities</option>
          <option value="category">Categories</option>
        </select>
      </div>

      {loading && <div>Loading…</div>}
      {err && <div style={{ color: "crimson" }}>{err}</div>}

      <ul style={{ paddingLeft: 18 }}>
        {results.map((r) => (
          <li key={r.uri} style={{ marginBottom: 6 }}>
            <strong style={{ marginRight: 8 }}>[{r.kind}]</strong>
            {r.label}
            <div style={{ fontSize: 12, opacity: 0.7 }}>{r.uri}</div>
          </li>
        ))}
      </ul>
    </div>
  );
}
