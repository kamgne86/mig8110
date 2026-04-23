// ─── Page détail produit ─────────────────────────────────────────────────────

async function loadProduct() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');

  if (!code) {
    document.getElementById('name').textContent = 'Code produit manquant';
    return;
  }

  try {
    const p = await fetchProduct(code);
    fillFromProduct(p);
  } catch (e) {
    document.getElementById('name').textContent = 'Erreur de chargement';
    console.error(e);
  }
}

function getPrimaryCategory(product) {
  if (!product || !Array.isArray(product.categories)) return '';

  for (const cat of product.categories) {
    if (typeof cat === 'string' && cat.trim()) return cat.trim();
    if (cat && typeof cat === 'object') {
      const value = String(cat.child || cat.display || '').trim();
      if (value) return value;
    }
  }

  return '';
}

function getTargetCategory(product) {
  const labels = getCategoryLabels(product);
  if (!labels.length) return '';

  return labels
    .slice()
    .sort((a, b) => {
      const wordDiff = b.split(/\s+/).length - a.split(/\s+/).length;
      if (wordDiff !== 0) return wordDiff;
      return b.length - a.length;
    })[0];
}

function normalizeToken(value) {
  return String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9 ]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function getCategoryLabels(product) {
  if (!product || !Array.isArray(product.categories)) return [];

  return product.categories
    .map(cat => {
      if (typeof cat === 'string') return cat;
      if (cat && typeof cat === 'object') return cat.child || cat.display || '';
      return '';
    })
    .map(label => String(label || '').trim())
    .filter(Boolean);
}

function getTopIngredients(product, maxItems = 5) {
  const source = Array.isArray(product?.ingredients) && product.ingredients.length
    ? product.ingredients
    : getIngredientsList(product, maxItems);

  const out = [];
  const seen = new Set();

  for (const ing of source) {
    const clean = String(ing || '').trim();
    if (!clean) continue;
    const key = normalizeToken(clean);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
    if (out.length >= maxItems) break;
  }

  return out;
}

function hasSameCategory(baseProduct, candidateProduct) {
  const targetCategory = normalizeToken(getTargetCategory(baseProduct));
  if (!targetCategory) return false;

  const candidateCats = getCategoryLabels(candidateProduct).map(normalizeToken);
  return candidateCats.includes(targetCategory);
}

function getIngredientSimilarity(baseProduct, candidateProduct) {
  const baseTopIngredients = getTopIngredients(baseProduct, 5);
  const candidateTopIngredients = getTopIngredients(candidateProduct, 5);

  if (!baseTopIngredients.length || !candidateTopIngredients.length) {
    return {
      ratio: 0,
      candidateTopIngredients,
    };
  }

  let total = 0;

  baseTopIngredients.forEach((ingredient, index) => {
    const normalized = normalizeToken(ingredient);
    const candidateIndex = candidateTopIngredients.findIndex(
      candidateIngredient => normalizeToken(candidateIngredient) === normalized
    );

    if (candidateIndex === -1) return;
    if (candidateIndex === index) {
      total += 1;
      return;
    }

    const distance = Math.abs(candidateIndex - index);
    total += Math.max(0.35, 0.8 - (distance * 0.15));
  });

  return {
    ratio: total / baseTopIngredients.length,
    candidateTopIngredients,
  };
}

function getNutrimentSimilarity(baseProduct, candidateProduct) {
  const keys = [
    'energy_kcal_100g',
    'fat_100g',
    'sugars_100g',
    'salt_100g',
    'proteins_100g',
    'carbohydrates_100g'
  ];

  let total = 0;
  let count = 0;

  for (const key of keys) {
    const a = Number(baseProduct?.[key]);
    const b = Number(candidateProduct?.[key]);
    if (!Number.isFinite(a) || !Number.isFinite(b)) continue;

    const diff = Math.abs(a - b);
    const ref = Math.max(Math.abs(a), Math.abs(b), 1);
    const score = Math.max(0, 1 - (diff / ref));

    total += score;
    count += 1;
  }

  if (!count) return 0;
  return total / count;
}

function getSimilarityMeta(baseProduct, candidateProduct) {
  const sameCategory = hasSameCategory(baseProduct, candidateProduct);
  const ingredientSimilarity = getIngredientSimilarity(baseProduct, candidateProduct);
  const nutrimentSimilarity = getNutrimentSimilarity(baseProduct, candidateProduct);

  const totalScore = sameCategory
    ? (0.6 * ingredientSimilarity.ratio) + (0.4 * nutrimentSimilarity)
    : 0;

  return {
    sameCategory,
    totalScore,
    categoryLabel: getTargetCategory(candidateProduct) || '—',
    topIngredients: ingredientSimilarity.candidateTopIngredients,
    ingredientSimilarityPct: Math.round(ingredientSimilarity.ratio * 100),
    nutrimentSimilarityPct: Math.round(nutrimentSimilarity * 100)
  };
}

function renderSimilarCard(product, meta) {
  const name = escHtml(product.product_name || 'Sans nom');
  const brand = escHtml(product.brands || 'Sans marque');
  const ingredientsText = meta.topIngredients.length
    ? meta.topIngredients.map(escHtml).join(', ')
    : '—';
  const image = product.front_url
    ? `<img src="${escHtml(product.front_url)}" alt="Image produit" loading="lazy" />`
    : '<span>IMAGE</span>';

  return `
    <article class="similar-card">
      <div class="similar-media">${image}</div>
      <div class="similar-body">
        <h4 class="similar-title">${name}</h4>
        <p class="similar-brand">${brand}</p>
        <div class="similar-grid">
          <div class="similar-field">
            <span class="similar-field-label">Categorie</span>
            <span class="similar-field-value">${escHtml(meta.categoryLabel)}</span>
          </div>
          <div class="similar-field">
            <span class="similar-field-label">5 premiers ingredients</span>
            <span class="similar-field-value similar-ingredients">${ingredientsText}</span>
          </div>
          <div class="similar-field">
            <span class="similar-field-label">Ingredients proches</span>
            <span class="similar-field-value similar-score">${meta.ingredientSimilarityPct}%</span>
          </div>
          <div class="similar-field">
            <span class="similar-field-label">Nutriments similaires</span>
            <span class="similar-field-value similar-score">${meta.nutrimentSimilarityPct}%</span>
          </div>
          <div class="similar-field">
            <span class="similar-field-label">Categorie similaire</span>
            <span class="similar-field-value similar-score">${meta.categorySimilarityPct}%</span>
          </div>
          <div class="similar-field">
            <span class="similar-field-label">Score global</span>
            <span class="similar-field-value similar-score">${meta.overallSimilarityPct}%</span>
          </div>
        </div>
      </div>
      <a class="detail-btn" href="/static/detail.html?code=${escAttr(product.code)}">Voir detail</a>
    </article>
  `;
}

async function loadSimilarProducts(baseProduct) {
  const container = document.getElementById('similarProducts');
  if (!container) return;

  try {
    const ranked = await fetchSimilarProducts(baseProduct.code, 4);

    if (!ranked.length) {
      container.innerHTML = '<p class="empty-msg">Aucun produit similaire trouve.</p>';
      return;
    }

    container.innerHTML = ranked
      .map(product => renderSimilarCard(product, {
        categoryLabel: product.category_label || getTargetCategory(product) || '—',
        topIngredients: Array.isArray(product.top_ingredients) ? product.top_ingredients : [],
        ingredientSimilarityPct: Number(product.ingredient_similarity_pct || 0),
        nutrimentSimilarityPct: Number(product.nutriment_similarity_pct || 0),
        categorySimilarityPct: Number(product.category_similarity_pct || 0),
        overallSimilarityPct: Number(product.overall_similarity_pct || 0)
      }))
      .join('');
  } catch (error) {
    container.innerHTML = '<p class="empty-msg">Erreur lors du chargement des produits similaires.</p>';
    console.error(error);
  }
}

function fillFromProduct(p) {
  document.title = p.product_name || 'Détail produit';
  document.getElementById('name').textContent  = p.product_name || 'Sans nom';
  document.getElementById('brand').textContent = p.brands || 'Sans marque';

  // Nutri-Score visuel (bande A-E)
  const nsGrade = validGrade(p.nutriscore_grade);
  const nsContainer = document.getElementById('nutriScore');
  if (nsGrade) {
    nsContainer.className = '';
    nsContainer.innerHTML = renderNutriScoreBand(nsGrade);
  } else {
    nsContainer.textContent = '—';
    nsContainer.className = 'nutri-score';
  }

  // Éco-Score
  const esGrade = validGrade(p.ecoscore_grade);
  const es = document.getElementById('ecoScore');
  es.textContent = esGrade ? esGrade.toUpperCase() : '—';
  es.className   = esGrade ? `nutri-score eco es-${esGrade}` : 'nutri-score eco';

  // Valeurs nutritionnelles avec indicateurs colorés
  const nutriFields = [
    { id: 'calories',    key: 'energy_kcal_100g',    unit: 'kcal', decimals: 0 },
    { id: 'carbs',       key: 'carbohydrates_100g',  unit: 'g',    decimals: 2 },
    { id: 'sugars',      key: 'sugars_100g',         unit: 'g',    decimals: 2 },
    { id: 'fiber',       key: 'fiber_100g',          unit: 'g',    decimals: 2 },
    { id: 'fat',         key: 'fat_100g',            unit: 'g',    decimals: 2 },
    { id: 'saturated',   key: 'saturated_fat_100g',  unit: 'g',    decimals: 2 },
    { id: 'transFat',    key: 'trans_fat_100g',      unit: 'g',    decimals: 2 },
    { id: 'cholesterol', key: 'cholesterol_100g',    unit: 'mg',   decimals: 1 },
    { id: 'proteins',    key: 'proteins_100g',       unit: 'g',    decimals: 2 },
    { id: 'salt',        key: 'salt_100g',           unit: 'g',    decimals: 2 },
    { id: 'calcium',     key: 'calcium_100g',        unit: 'mg',   decimals: 1 },
    { id: 'iron',        key: 'iron_100g',           unit: 'mg',   decimals: 2 },
    { id: 'potassium',   key: 'potassium_100g',      unit: 'mg',   decimals: 1 },
  ];

  nutriFields.forEach(f => {
    document.getElementById(f.id).textContent = val(p[f.key], f.unit, f.decimals);
  });

  // Barre « faits essentiels »
  const facts = [];
  if (p.quantity) {
    facts.push({ icon: 'package', label: 'Contenance', value: escHtml(p.quantity) });
  }
  if (p.serving_size) {
    facts.push({ icon: 'utensils', label: 'Portion', value: escHtml(p.serving_size) });
  }
  const n = p.ingredients_n != null ? Math.round(p.ingredients_n) : null;
  if (n != null) {
    const sublabelColor = n <= 10 ? '#16a34a' : n <= 20 ? '#d97706' : '#dc2626';
    const label  = n <= 10 ? 'Peu transformé' : n <= 20 ? 'Modéré' : 'Ultra-transformé';
    facts.push({ icon: 'shopping-basket', label: 'Ingrédients', value: String(n), sublabel: label, sublabelColor });
  }
  const keyFactsEl = document.getElementById('keyFacts');
  if (facts.length) {
    keyFactsEl.innerHTML = facts.map(f => `
      <div class="fact-chip">
        <i data-lucide="${f.icon}" class="fact-icon"></i>
        <span class="fact-value">${f.value}</span>
        <span class="fact-label">${f.label}</span>
        ${f.sublabel ? `<span class="fact-sublabel" style="color:${f.sublabelColor};">${f.sublabel}</span>` : ''}
      </div>
    `).join('');
  } else {
    keyFactsEl.style.display = 'none';
  }

  // Ingrédients comme tags cliquables
  const ingredientsList = getIngredientsList(p);
  if (ingredientsList && ingredientsList.length) {
    document.getElementById('ingredients').innerHTML = ingredientsList
      .map(ing => `<a href="/static/index.html?ingredient=${encodeURIComponent(ing)}" class="tag-link">${escHtml(ing)}</a>`)
      .join('');
  } else {
    document.getElementById('ingredients').textContent = '—';
  }

  // Catégories comme tags cliquables avec hiérarchie
  if (p.categories && p.categories.length) {
    document.getElementById('categoriesList').innerHTML = p.categories
      .map(cat => {
        if (typeof cat === 'string') {
          return `<a href="/static/index.html?category=${encodeURIComponent(cat)}" class="tag-link">${escHtml(cat)}</a>`;
        } else if (cat && typeof cat === 'object') {
          const displayChild = cat.child || cat.display || '';
          const hasParent = cat.parent && cat.parent !== null;
          return `<a href="/static/index.html?category=${encodeURIComponent(displayChild)}" class="tag-link${hasParent ? ' category-hierarchical' : ''}">
            ${hasParent ? `<span class="cat-parent">${escHtml(cat.parent)}</span><span class="cat-separator">›</span>` : ''}
            <span class="cat-child">${escHtml(displayChild)}</span>
          </a>`;
        }
        return '';
      })
      .filter(x => x)
      .join('');
  } else {
    document.getElementById('categoriesList').textContent = '—';
  }

  if (p.front_url) {
    const box = document.getElementById('imageBox');
    const img = document.createElement('img');
    img.src = p.front_url;
    img.alt = 'Photo produit';
    img.style.cssText = 'width:100%;height:100%;object-fit:cover;border-radius:12px;';
    box.replaceChildren(img);
  }

  // Données brutes
  loadSimilarProducts(p);
  document.getElementById('rawJsonSection').style.display = 'block';
  document.getElementById('rawJson').textContent = JSON.stringify(p, null, 2);
}

function toggleRaw() {
  const pre = document.getElementById('rawJson');
  pre.style.display = pre.style.display === 'none' ? 'block' : 'none';
}

// Lancer le chargement
loadProduct().then(() => lucide.createIcons());
