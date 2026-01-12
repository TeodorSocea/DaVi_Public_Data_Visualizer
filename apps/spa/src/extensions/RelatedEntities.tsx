import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { getRelatedEntities } from "../lib/api";
import type { RelatedEntity } from "../lib/api";

export default function RelatedEntities({ entityId }: { entityId: string }) {
  const [items, setItems] = useState<RelatedEntity[]>([]);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setErr(null);
    setItems([]);

    getRelatedEntities(entityId, 10)
      .then((r) => {
        if (!cancelled) setItems(r.results ?? []);
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
  }, [entityId]);

  return (
    <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 800, marginBottom: 6 }}>Extension: Related entities</div>
      <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 10 }}>
        Similarity by shared DBpedia categories (dct:subject). Ranked by overlap.
      </div>

      {loading && <div style={{ fontSize: 13 }}>Loading…</div>}
      {err && <div style={{ color: "crimson", fontSize: 13 }}>{err}</div>}

      {!loading && !err && (
        items.length === 0 ? (
          <div style={{ fontSize: 13, opacity: 0.7 }}>No related entities found.</div>
        ) : (
          <ul style={{ margin: 0, paddingLeft: 16 }}>
            {items.map((x) => (
              <li key={x.uri} style={{ marginBottom: 6, fontSize: 13 }}>
                <Link to={`/entity/${x.id}`}>{x.label}</Link>{" "}
                <span style={{ opacity: 0.7 }}>({x.shared} shared)</span>
              </li>
            ))}
          </ul>
        )
      )}
    </div>
  );
}
