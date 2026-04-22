// ─── Couche API centralisée ──────────────────────────────────────────────────

const API_BASE = '';

async function fetchProducts(searchParams) {
  const res = await fetch(`${API_BASE}/products?${searchParams}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchProduct(code) {
  const res = await fetch(`${API_BASE}/products/${encodeURIComponent(code)}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchSimilarProducts(code, limit = 4) {
  const params = new URLSearchParams();
  params.set('limit', String(limit));

  const res = await fetch(`${API_BASE}/products/${encodeURIComponent(code)}/similar?${params}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

