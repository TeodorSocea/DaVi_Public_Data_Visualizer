import { useEffect, useState } from "react";

type Facet = { type: string; count: number };

const API_BASE = import.meta.env.VITE_API_BASE ??  window.location.origin;

export default function TypeFacets({ categoryId }: { categoryId: string }) {
  const [facets, setFacets] = useState<Facet[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErr(null);

    const cleanId = categoryId.replace(/\?+$/, "");
    fetch(`${API_BASE}/api/category/${encodeURIComponent(cleanId)}/facets/types?limit=15`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data) => {
        if (!cancelled) setFacets(data.facets ?? []);
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
  }, [categoryId]);

  return (
    <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 800, marginBottom: 6 }}>Extension: Type facets</div>
      <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 10 }}>
        Shows the most common rdf:type values among entities in this category.
      </div>

      {loading && <div style={{ fontSize: 13 }}>Loading…</div>}
      {err && <div style={{ color: "crimson", fontSize: 13 }}>{err}</div>}

      {!loading && !err && (
  <>
    <div style={{ display: "grid", gap: 6, maxHeight: 320, overflowY: "auto", overflowX: "hidden" }}>
      {facets.map((f) => (
        <label key={f.type} style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <input
            type="checkbox"
            checked={!!selected[f.type]}
            onChange={(e) =>
              setSelected((s) => ({ ...s, [f.type]: e.target.checked }))
            }
          />
          <span style={{ fontSize: 13, wordBreak: "break-word" }}>
            <span style={{ fontWeight: 600 }}>{short(f.type)}</span>{" "}
            <span style={{ opacity: 0.7 }}>({f.count})</span>
          </span>
        </label>
      ))}
    </div>

    <div style={{ display: "flex", gap: 8, marginTop: 10 }}>
      <button
        onClick={() => {
          const types = Object.entries(selected)
            .filter(([, v]) => v)
            .map(([k]) => k);

          window.dispatchEvent(
            new CustomEvent("wadev:typeFilterChanged", { detail: { types } })
          );
        }}
        style={{
          border: "1px solid #ddd",
          padding: "6px 10px",
          borderRadius: 10,
          cursor: "pointer",
          fontWeight: 600,
        }}
      >
        Apply filter
      </button>

      <button
        onClick={() => {
          setSelected({});
          window.dispatchEvent(
            new CustomEvent("wadev:typeFilterChanged", { detail: { types: [] } })
          );
        }}
        style={{
          border: "1px solid #ddd",
          padding: "6px 10px",
          borderRadius: 10,
          cursor: "pointer",
        }}
      >
        Clear
      </button>
    </div>
  </>
)}


    </div>
  );
}


function short(uri: string) {
    return uri
      .replace("http://dbpedia.org/ontology/", "dbo:")
      .replace("http://www.w3.org/2002/07/owl#", "owl:")
      .replace("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf:")
      .replace("http://xmlns.com/foaf/0.1/", "foaf:")
      .replace("http://www.wikidata.org/entity/", "wd:")
      .replace("https://www.wikidata.org/entity/", "wd:");
  }
  
