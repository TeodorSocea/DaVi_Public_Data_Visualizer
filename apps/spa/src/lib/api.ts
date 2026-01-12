export type SearchKind = "category" | "entity" | "all";

export type SearchResult = {
    uri: string;
    id: string;
    label: string;
    kind: "category" | "entity";
  };

export type SearchResponse = {
    query: string;
    kind: SearchKind;
    limit: number;
    offset: number;
    count: number;
    total: number;
    entityTotal: number;
    categoryTotal: number;
    results: SearchResult[];
  };
  
const API_BASE = import.meta.env.VITE_API_BASE ?? ""; // allow empty for same-origin

export async function search(q: string, kind: SearchKind, limit = 20, offset = 0): Promise<SearchResponse> {
  // If API_BASE is empty, use same-origin as base (required by URL constructor)
  const base = API_BASE ? API_BASE : window.location.origin;

  const url = new URL("/api/search", base);
  url.searchParams.set("q", q);
  url.searchParams.set("kind", kind);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`Search failed: ${res.status}`);
  return res.json();
}

  

export type CategoryRef = { uri: string; id: string; label: string };

export type CategoryDetails = {
  uri: string;
  id: string;
  label: string;
  broader: CategoryRef[];
  narrower: CategoryRef[];
  entityCount: number;
};

export type EntityRef = { uri: string; id: string; label: string };

export type CategoryEntitiesResponse = {
  categoryUri: string;
  categoryId: string;
  limit: number;
  offset: number;
  count: number;
  results: EntityRef[];
};

export type EntityDetails = {
  uri: string;
  id: string;
  label: string;
  abstract?: string | null;
  types: string[];
  categories: CategoryRef[];
};

export async function getCategory(id: string): Promise<CategoryDetails> {
  const res = await fetch(`${API_BASE}/api/category/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error(`Category failed: ${res.status}`);
  return res.json();
}

export async function getCategoryEntities(id: string, limit = 50, offset = 0): Promise<CategoryEntitiesResponse> {
  const url = new URL(`${API_BASE}/api/category/${encodeURIComponent(id)}/entities`);
  url.searchParams.set("limit", String(limit));
  url.searchParams.set("offset", String(offset));
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`Category entities failed: ${res.status}`);
  return res.json();
}

export async function getEntity(id: string): Promise<EntityDetails> {
  const res = await fetch(`${API_BASE}/api/entity/${encodeURIComponent(id)}`);
  if (!res.ok) throw new Error(`Entity failed: ${res.status}`);
  return res.json();
}

export type RelatedEntity = { uri: string; id: string; label: string; shared: number };

export type RelatedEntitiesResponse = {
  entityUri: string;
  entityId: string;
  limit: number;
  count: number;
  results: RelatedEntity[];
};

export async function getRelatedEntities(id: string, limit = 10): Promise<RelatedEntitiesResponse> {
  const url = new URL(`${API_BASE}/api/entity/${encodeURIComponent(id)}/related`);
  url.searchParams.set("limit", String(limit));
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`Related entities failed: ${res.status}`);
  return res.json();
}
