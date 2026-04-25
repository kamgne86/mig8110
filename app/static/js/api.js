// ─── Couche API centralisée ──────────────────────────────────────────────────

const API_BASE =
  window.location.hostname === 'localhost'
    ? `http://127.0.0.1:${window.location.port || '8001'}`
    : '';

async function fetchProducts(searchParams) {
  const res = await fetch(`${API_BASE}/products?${searchParams}`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

async function fetchCategories() {
  const res = await fetch(`${API_BASE}/products/categories`);
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

