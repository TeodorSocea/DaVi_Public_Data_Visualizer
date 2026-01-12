import { useEffect, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { getEntity } from "../lib/api";
import type { EntityDetails } from "../lib/api";
import ShareBar from "../components/ShareBar";
import { useJsonLd } from "../lib/useJsonLd";
import { getExtensions } from "../extensions/registry";

export default function EntityPage() {
  const { id } = useParams();
  const [ent, setEnt] = useState<EntityDetails | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (!id) return;

    setLoading(true);
    setErr(null);

    getEntity(id)
      .then((d) => {
        if (!cancelled) setEnt(d);
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

  useEffect(() => {
    if (!ent) return;
    document.title = `${ent.label} — DBpedia Explorer`;
  }, [ent]);

  useJsonLd(
    ent
      ? {
          "@context": "https://schema.org",
          "@type": "Thing",
          name: ent.label,
          url: window.location.href,
          sameAs: ent.uri,
          description: ent.abstract ?? undefined,
        }
      : { "@context": "https://schema.org", "@type": "WebPage", name: "Loading" },
    `entity-${ent?.id ?? "loading"}`
  );

  if (loading) return <div style={{ padding: 24 }}>Loading…</div>;
  if (err) return <div style={{ padding: 24, color: "crimson" }}>{err}</div>;
  if (!ent) return <div style={{ padding: 24 }}>Not found.</div>;

  return (
    <div
      prefix="
        rdfs: http://www.w3.org/2000/01/rdf-schema#
        dbo:  http://dbpedia.org/ontology/
        dct:  http://purl.org/dc/terms/
        schema: https://schema.org/
      "
      style={{ maxWidth: 1000, margin: "24px auto", fontFamily: "system-ui, sans-serif" }}
    >
      <div style={{ marginBottom: 12 }}>
        <Link to="/">← Search</Link>
      </div>

      <section about={ent.uri} typeof="schema:Thing">
        <h1 style={{ marginBottom: 6 }}>
          <span property="rdfs:label">{ent.label}</span>
        </h1>

        <div style={{ fontSize: 12, opacity: 0.7, wordBreak: "break-all" }}>
          <a href={ent.uri} rel="rdfs:seeAlso">
            {ent.uri}
          </a>
        </div>

        <ShareBar title={ent.label} />

        {ent.abstract && (
          <p style={{ marginTop: 14, lineHeight: 1.5 }}>
            <span property="dbo:abstract">{ent.abstract}</span>
          </p>
        )}
      </section>

      {/* NEW: sidebar layout */}
      <div style={{ display: "grid", gridTemplateColumns: "320px 1fr", gap: 16, marginTop: 18 }}>
        {/* LEFT: entity extensions */}
        <aside style={{ display: "grid", gap: 12 }}>
          {getExtensions("entity.sidebar").map((ext) => (
            <div key={ext.id}>{ext.render({ entityId: id! })}</div>
          ))}
        </aside>

        {/* RIGHT: existing content */}
        <main>
          <section style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
            <h3 style={{ marginTop: 0 }}>Categories</h3>
            <ul>
              {ent.categories.slice(0, 40).map((c) => (
                <li key={c.uri}>
                  <Link to={`/category/${c.id}`} rel="dct:subject">
                    {c.label}
                  </Link>
                </li>
              ))}
            </ul>
          </section>

          <section style={{ border: "1px solid #eee", borderRadius: 12, padding: 12, marginTop: 16 }}>
            <h3 style={{ marginTop: 0 }}>Types (dbo / rdf:type)</h3>
            <ul style={{ fontSize: 13 }}>
              {ent.types.slice(0, 40).map((t) => (
                <li key={t} style={{ wordBreak: "break-all" }}>
                  {t}
                </li>
              ))}
            </ul>
          </section>
        </main>
      </div>
    </div>
  );
}
