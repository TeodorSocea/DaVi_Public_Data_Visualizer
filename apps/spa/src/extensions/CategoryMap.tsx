import { Link } from "react-router-dom";
import { useEffect, useMemo, useState } from "react";
import { getCategory } from "../lib/api";
import type { CategoryDetails } from "../lib/api";

export default function CategoryMap({ categoryId }: { categoryId: string }) {
  const [cat, setCat] = useState<CategoryDetails | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setErr(null);
    setCat(null);

    getCategory(categoryId)
      .then((c) => {
        if (!cancelled) setCat(c);
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e));
      });

    return () => {
      cancelled = true;
    };
  }, [categoryId]);

  // graph sizing
  const W = 280;
  const H = 180;

  const graph = useMemo(() => {
    if (!cat) return null;

    const parents = cat.broader.slice(0, 4);
    const children = cat.narrower.slice(0, 6);

    const cx = W / 2;
    const cy = H / 2;

    const leftX = 52;
    const rightX = W - 52;

    const spreadY = (n: number) => {
      if (n <= 1) return [cy];
      const top = 26;
      const bottom = H - 26;
      const step = (bottom - top) / (n - 1);
      return Array.from({ length: n }, (_, i) => top + i * step);
    };

    const parentYs = spreadY(parents.length);
    const childYs = spreadY(children.length);

    const nodes = [
      { key: "center", label: cat.label, id: cat.id, side: "center" as const, x: cx, y: cy },
      ...parents.map((p, i) => ({ key: `p-${p.id}`, label: p.label, id: p.id, side: "left" as const, x: leftX, y: parentYs[i] })),
      ...children.map((c, i) => ({ key: `c-${c.id}`, label: c.label, id: c.id, side: "right" as const, x: rightX, y: childYs[i] })),
    ];

    const edges = [
      ...parents.map((_p, i) => ({ from: { x: leftX, y: parentYs[i] }, to: { x: cx, y: cy } })),
      ...children.map((_c, i) => ({ from: { x: cx, y: cy }, to: { x: rightX, y: childYs[i] } })),
    ];

    return { parents, children, nodes, edges };
  }, [cat]);

  return (
    <div style={{ border: "1px solid #eee", borderRadius: 12, padding: 12 }}>
      <div style={{ fontWeight: 800, marginBottom: 6 }}>Extension: Category map</div>
      <div style={{ fontSize: 12, opacity: 0.7, marginBottom: 10 }}>
        Layered mini-graph (broader → current → narrower).
      </div>

      {err && <div style={{ color: "crimson", fontSize: 13 }}>{err}</div>}
      {!cat && !err && <div style={{ fontSize: 13 }}>Loading…</div>}

      {cat && graph && (
        <>
          <svg
            width={W}
            height={H}
            viewBox={`0 0 ${W} ${H}`}
            role="img"
            aria-label="Category hierarchy mini-graph"
            style={{ display: "block", margin: "0 auto", background: "white" }}
          >
            <defs>
              <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto">
                <path d="M0,0 L9,3 L0,6 Z" fill="#bbb" />
              </marker>
            </defs>

            {/* edges */}
            {graph.edges.map((e, i) => (
              <line
                key={i}
                x1={e.from.x}
                y1={e.from.y}
                x2={e.to.x}
                y2={e.to.y}
                stroke="#ccc"
                strokeWidth={2}
                markerEnd="url(#arrow)"
              />
            ))}

            {/* nodes */}
            {graph.nodes.map((n) => {
              const r = n.side === "center" ? 18 : 10;
              const fill = n.side === "center" ? "#111" : "#fff";
              const stroke = n.side === "center" ? "#111" : "#bbb";

              return (
                <g key={n.key}>
                  <circle cx={n.x} cy={n.y} r={r} fill={fill} stroke={stroke} strokeWidth={2} />
                </g>
              );
            })}
          </svg>

          {/* Labels + links below the graph (keeps SVG simple + clickable) */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginTop: 10 }}>
            <div style={{ fontSize: 13 }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>Broader</div>
              {graph.parents.length === 0 ? (
                <div style={{ opacity: 0.7 }}>None</div>
              ) : (
                <ul style={{ margin: 0, paddingLeft: 16 }}>
                  {graph.parents.map((p) => (
                    <li key={p.uri}>
                      <Link to={`/category/${p.id}`}>{p.label}</Link>
                    </li>
                  ))}
                </ul>
              )}
            </div>

            <div style={{ textAlign: "center" }}>
              <div style={{ fontWeight: 800 }}>{cat.label}</div>
              <div style={{ fontSize: 12, opacity: 0.7, marginTop: 4 }}>
                {cat.broader.length} parent(s) · {cat.narrower.length} child(ren)
              </div>
            </div>

            <div style={{ fontSize: 13 }}>
              <div style={{ fontWeight: 700, marginBottom: 6 }}>Narrower</div>
              {graph.children.length === 0 ? (
                <div style={{ opacity: 0.7 }}>None</div>
              ) : (
                <ul style={{ margin: 0, paddingLeft: 16 }}>
                  {graph.children.map((c) => (
                    <li key={c.uri}>
                      <Link to={`/category/${c.id}`}>{c.label}</Link>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
