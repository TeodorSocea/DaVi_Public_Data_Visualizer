import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getCategory, getCategoryEntities } from "../lib/api";
import type { CategoryDetails, EntityRef } from "../lib/api";
import ShareBar from "../components/ShareBar";
import { useJsonLd } from "../lib/useJsonLd";
import { getExtensions } from "../extensions/registry";

export default function CategoryPage() {
  const { id } = useParams();
  const [cat, setCat] = useState<CategoryDetails | null>(null);
  const [entities, setEntities] = useState<EntityRef[]>([]);
  const [loading, setLoading] = useState(true); // initial page load only
  const [entitiesLoading, setEntitiesLoading] = useState(false); // refetch entities only
  const [err, setErr] = useState<string | null>(null);
  const [selectedTypes, setSelectedTypes] = useState<string[]>([]);

  // listen to extension events (Apply button dispatches these)
  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent;
      setSelectedTypes(ce.detail?.types ?? []);
    };
    window.addEventListener("wadev:typeFilterChanged", handler as EventListener);
    return () =>
      window.removeEventListener("wadev:typeFilterChanged", handler as EventListener);
  }, []);

  // fetch CATEGORY only when id changes
  useEffect(() => {
    let cancelled = false;
    if (!id) return;

    setErr(null);
    setLoading(true);
    setCat(null);

    getCategory(id)
      .then((c) => {
        if (cancelled) return;
        setCat(c);
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
  }, [id]);

  // fetch ENTITIES when id OR selectedTypes change
  useEffect(() => {
    let cancelled = false;
    if (!id) return;

    setErr(null);
    setEntitiesLoading(true);

    const API_BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

    const p = selectedTypes.length
      ? fetch(
          `${API_BASE}/api/category/${encodeURIComponent(
            id
          )}/entitiesByType?types=${encodeURIComponent(
            selectedTypes.join(",")
          )}&limit=25&offset=0`
        ).then((r) => {
          if (!r.ok) throw new Error(`entitiesByType HTTP ${r.status}`);
          return r.json();
        })
      : getCategoryEntities(id, 25, 0);

    Promise.resolve(p)
      .then((ents: any) => {
        if (cancelled) return;
        setEntities(ents.results ?? []);
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e));
      })
      .finally(() => {
        if (!cancelled) setEntitiesLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [id, selectedTypes]);

  // title + JSON-LD
  useEffect(() => {
    if (!cat) return;
    document.title = `${cat.label} — DBpedia Explorer`;
  }, [cat]);

  useJsonLd(
    cat
      ? {
          "@context": "https://schema.org",
          "@type": "CollectionPage",
          name: cat.label,
          url: window.location.href,
          about: cat.uri,
        }
      : { "@context": "https://schema.org", "@type": "WebPage", name: "Loading" },
    `category-${cat?.id ?? "loading"}`
  );

  if (loading && !cat) return <div style={{ padding: 24 }}>Loading…</div>;
  if (err) return <div style={{ padding: 24, color: "crimson" }}>{err}</div>;
  if (!cat) return <div style={{ padding: 24 }}>Not found.</div>;

  return (
    <div
      prefix="
        rdfs: http://www.w3.org/2000/01/rdf-schema#
        skos: http://www.w3.org/2004/02/skos/core#
        dct:  http://purl.org/dc/terms/
      "
      style={{ maxWidth: 1000, margin: "24px auto", fontFamily: "system-ui, sans-serif" }}
    >
      <div style={{ marginBottom: 12 }}>
        <Link to="/">← Search</Link>
      </div>

      <section about={cat.uri} typeof="skos:Concept">
        <h1 style={{ marginBottom: 6 }}>
          <span property="rdfs:label">{cat.label}</span>
        </h1>

        <div style={{ fontSize: 12, opacity: 0.7, wordBreak: "break-all" }}>
          <a href={cat.uri} rel="rdfs:seeAlso">
            {cat.uri}
          </a>
        </div>

        <ShareBar title={cat.label} />
      </section>

      <div style={{ marginTop: 8, opacity: 0.85 }}>
        Entities in this category (approx): <strong>{cat.entityCount}</strong>

        {selectedTypes.length > 0 && (
          <div style={{ marginTop: 8, fontSize: 12, opacity: 0.8, wordBreak: "break-word" }}>
            Active type filter: {selectedTypes.join(", ")}
          </div>
        )}
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, marginTop: 18 }}>
        <aside style={{ display: "grid", gap: 12 }}>
          {getExtensions("category.sidebar").map((ext) => (
            <div key={ext.id}>{ext.render({ categoryId: id! })}</div>
          ))}
        </aside>

        <main>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
            <section style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
              <h3 style={{ marginTop: 0 }}>Broader (parents)</h3>
              {cat.broader.length === 0 && <div style={{ opacity: 0.7 }}>None</div>}
              <ul>
                {cat.broader.map((b) => (
                  <li key={b.uri}>
                    <Link to={`/category/${b.id}`} rel="skos:broader">
                      {b.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </section>

            <section style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
              <h3 style={{ marginTop: 0 }}>Narrower (children)</h3>
              {cat.narrower.length === 0 && <div style={{ opacity: 0.7 }}>None</div>}
              <ul>
                {cat.narrower.slice(0, 30).map((n) => (
                  <li key={n.uri}>
                    <Link to={`/category/${n.id}`} rel="skos:narrower">
                      {n.label}
                    </Link>
                  </li>
                ))}
              </ul>
            </section>
          </div>

          <section style={{ marginTop: 18, border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <h3 style={{ marginTop: 0 }}>
              Sample entities{" "}
              {entitiesLoading && <span style={{ fontSize: 12, opacity: 0.7 }}>(updating…)</span>}
            </h3>
            <ul>
              {entities.map((e) => (
                <li key={e.uri}>
                  <Link to={`/entity/${e.id}`}>{e.label}</Link>
                </li>
              ))}
            </ul>
          </section>
        </main>
      </div>
    </div>
  );
}
