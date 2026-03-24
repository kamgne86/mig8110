const API_BASE = '';  // même origine (FastAPI sert le front)

// ─── État global ──────────────────────────────────────────────────────────────
let allProducts = [];          // résultats bruts de l'API
let activeGrades = new Set();  // nutriscore sélectionnés

// ─── Helpers données ──────────────────────────────────────────────────────────
/** Retourne le grade si valide (a-e), sinon null */
function validGrade(grade) {
  if (!grade) return null;
  const g = grade.toLowerCase();
  return ['a','b','c','d','e'].includes(g) ? g : null;
}

/**
 * Parse un champ tags qui peut être :
 *   - un vrai tableau JS  : ["en:foo-bar", "fr:baz"]   (format API)
 *   - une string Python   : "['en:foo-bar', 'fr:baz']" (format CSV legacy)
 * Retourne un tableau de labels lisibles.
 */
function parseTags(raw, maxItems = null) {
  if (!raw) return [];

  let tags = [];
  if (Array.isArray(raw)) {
    tags = raw;
  } else if (typeof raw === 'string') {
    if (raw === '[]' || raw.trim() === '') return [];
    // Extrait tout ce qui est entre apostrophes (format Python)
    tags = [...raw.matchAll(/'([^']+)'/g)].map(m => m[1]);
  }

  let labels = tags.map(tag => tag.replace(/^[a-z]{2}:/, '').replace(/-/g, ' '));
  if (maxItems) labels = labels.slice(0, maxItems);
  return labels;
}

// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Nutriscore filter buttons
  document.querySelectorAll('#nutriFilters button').forEach(btn => {
    btn.addEventListener('click', () => {
      const grade = btn.dataset.grade;
      if (activeGrades.has(grade)) {
        activeGrades.delete(grade);
        btn.classList.remove('active');
      } else {
        activeGrades.add(grade);
        btn.classList.add('active');
      }
      applyFilters();
    });
  });

  // Enter key triggers search
  document.addEventListener('keypress', e => {
    if (e.key === 'Enter') fetchAndRender();
  });

  // Debounce brand input
  document.getElementById('searchBrand').addEventListener('input', debounce(fetchAndRender, 500));
});

// ─── Fetch API ────────────────────────────────────────────────────────────────
async function fetchAndRender() {
  const name  = document.getElementById('searchName').value.trim();
  const brand = document.getElementById('searchBrand').value.trim();

  showLoading();
  hideError();

  try {
    const params = new URLSearchParams();
    if (name)  params.append('q', name);
    if (brand) params.append('brand', brand);

    const res = await fetch(`${API_BASE}/products?${params}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    allProducts = await res.json();
    applyFilters();
  } catch (err) {
    showError("Erreur API : " + err.message + "<br>Vérifiez que l'API FastAPI est démarrée.");
  }
}

// ─── Filtres client-side ──────────────────────────────────────────────────────
function applyFilters() {
  const maxSalt  = parseFloat(document.getElementById('saltSlider').value);
  const maxSugar = parseFloat(document.getElementById('sugarSlider').value);
  const maxCal   = parseFloat(document.getElementById('calSlider').value);
  const maxFat   = parseFloat(document.getElementById('fatSlider').value);

  const filtered = allProducts.filter(p => {
    // Nutriscore — on ignore les produits avec grade "unknown" ou absent si un filtre est actif
    if (activeGrades.size > 0) {
      const grade = validGrade(p.nutriscore_grade);
      if (!grade || !activeGrades.has(grade)) return false;
    }
    // Sliders (on ignore si le champ est null/undefined)
    if (p.salt_100g    != null && p.salt_100g    > maxSalt)  return false;
    if (p.sugars_100g  != null && p.sugars_100g  > maxSugar) return false;
    if (p.energy_kcal_100g != null && p.energy_kcal_100g > maxCal)   return false;
    if (p.fat_100g     != null && p.fat_100g     > maxFat)  return false;
    return true;
  });

  renderProducts(filtered);
}

// ─── Rendu produits ───────────────────────────────────────────────────────────
function renderProducts(products) {
  const statsDiv   = document.getElementById('stats');
  const resultsDiv = document.getElementById('results');

  // Filtre les produits sans nom (données incomplètes)
  const displayable = products.filter(p => p.product_name && p.product_name.trim() !== '');

  if (displayable.length === 0) {
    statsDiv.style.display = 'none';
    resultsDiv.innerHTML = '<p class="empty-msg">Aucun produit trouvé 😕</p>';
    return;
  }

  statsDiv.innerHTML = `<strong>${displayable.length} produit(s) affiché(s)</strong>${allProducts.length !== displayable.length ? ` sur ${allProducts.length} récupérés` : ''}`;
  statsDiv.style.display = 'block';

  resultsDiv.innerHTML = displayable.map(p => {
    const ns = validGrade(p.nutriscore_grade);
    const es = validGrade(p.ecoscore_grade);
    return `
    <div class="product-card">
      <div class="product-check">
        <input type="checkbox" class="compare-check" value="${p.code}" data-name="${escHtml(p.product_name || '')}">
      </div>

      <div class="product-image">
        ${p.front_url
          ? `<img src="${escHtml(p.front_url)}" alt="Photo" style="width:100%;height:100%;object-fit:cover;border-radius:6px;">`
          : 'IMAGE'}
      </div>

      <div class="product-info">
        <h3>${escHtml(p.product_name)}</h3>
        <p>${escHtml(p.brands || 'Sans marque')}</p>
        <div class="product-meta">
          <span>${p.energy_kcal_100g != null ? Math.round(p.energy_kcal_100g) + ' kcal' : '—'}</span>
          ${ns ? `<span class="badge ns-${ns}">${ns.toUpperCase()}</span>` : ''}
          ${es ? `<span class="badge eco es-${es}">Éco ${es.toUpperCase()}</span>` : ''}
        </div>
      </div>

      <button class="detail-btn" onclick="voirDetail('${escAttr(p.code)}')">Voir Détail ▶</button>
    </div>
  `}).join('');
}

// ─── Navigation détail ────────────────────────────────────────────────────────
function voirDetail(code) {
  window.location.href = `/static/detail.html?code=${encodeURIComponent(code)}`;
}

// ─── Comparaison ──────────────────────────────────────────────────────────────
async function comparerProduits() {
  const checked = [...document.querySelectorAll('.compare-check:checked')];

  if (checked.length < 2) {
    alert('Sélectionnez au moins 2 produits à comparer.');
    return;
  }
  if (checked.length > 4) {
    alert('Maximum 4 produits en comparaison.');
    return;
  }

  const codes = checked.map(c => c.value);

  // Récupère les détails complets depuis l'API
  let details;
  try {
    details = await Promise.all(codes.map(async code => {
      const res = await fetch(`${API_BASE}/products/${code}`);
      if (!res.ok) throw new Error(`HTTP ${res.status} pour ${code}`);
      return res.json();
    }));
  } catch (err) {
    showError('Erreur lors de la comparaison : ' + err.message);
    return;
  }

  const fields = [
    { label: '🔥 Calories',   key: 'energy_kcal_100g',  unit: 'kcal' },
    { label: '🫒 Lipides',    key: 'fat_100g',           unit: 'g' },
    { label: '🌾 Glucides',   key: 'carbohydrates_100g', unit: 'g' },
    { label: '🍬 Sucres',     key: 'sugars_100g',        unit: 'g' },
    { label: '💪 Protéines',  key: 'proteins_100g',      unit: 'g' },
    { label: '🧂 Sel',        key: 'salt_100g',          unit: 'g' },
  ];

  const header = `
    <div class="compare-header-row">
      <div class="compare-field-col"></div>
      ${details.map(p => `
        <div class="compare-product-col">
          <strong>${escHtml(p.product_name || 'Sans nom')}</strong>
          <small>${escHtml(p.brands || '')}</small>
          ${validGrade(p.nutriscore_grade)
            ? `<span class="badge ns-${validGrade(p.nutriscore_grade)}">${validGrade(p.nutriscore_grade).toUpperCase()}</span>`
            : ''}
        </div>
      `).join('')}
    </div>
  `;

  const rows = fields.map(f => {
    const vals = details.map(p => p[f.key]);
    const numVals = vals.filter(v => v != null);
    const minVal = numVals.length ? Math.min(...numVals) : null;

    return `
      <div class="compare-row">
        <div class="compare-field-col">${f.label}</div>
        ${vals.map(v => {
          const display = v != null ? `${Math.round(v * 100) / 100} ${f.unit}` : '—';
          const isBest = v != null && v === minVal && numVals.length > 1;
          return `<div class="compare-val-col ${isBest ? 'best-val' : ''}">${display}</div>`;
        }).join('')}
      </div>
    `;
  }).join('');

  document.getElementById('compareContent').innerHTML = header + rows;
  document.getElementById('compareModal').style.display = 'flex';
}

function fermerComparaison() {
  document.getElementById('compareModal').style.display = 'none';
}

// ─── Reset ────────────────────────────────────────────────────────────────────
function resetFilters() {
  activeGrades.clear();
  document.querySelectorAll('#nutriFilters button').forEach(b => b.classList.remove('active'));

  document.getElementById('saltSlider').value  = 5;    updateSliderLabel('saltVal',  '5.00', 'g');
  document.getElementById('sugarSlider').value = 100;  updateSliderLabel('sugarVal', 100,  'g');
  document.getElementById('calSlider').value   = 1000; updateSliderLabel('calVal',   1000, 'kcal');
  document.getElementById('fatSlider').value   = 100;  updateSliderLabel('fatVal',   100,  'g');

  applyFilters();
}

// ─── Utilitaires ──────────────────────────────────────────────────────────────
function updateSliderLabel(id, value, unit) {
  document.getElementById(id).textContent = `${value} ${unit}`;
}

function showLoading() {
  document.getElementById('results').innerHTML = '<p class="empty-msg">🔄 Recherche en cours…</p>';
  document.getElementById('stats').style.display = 'none';
}

function showError(msg) {
  const el = document.getElementById('error');
  el.innerHTML = msg;
  el.style.display = 'block';
}

function hideError() {
  document.getElementById('error').style.display = 'none';
}

function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function escAttr(str) {
  return encodeURIComponent(str);
}

function debounce(fn, wait) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), wait); };
}
