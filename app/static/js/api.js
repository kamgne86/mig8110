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

