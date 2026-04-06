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
  document.getElementById('rawJsonSection').style.display = 'block';
  document.getElementById('rawJson').textContent = JSON.stringify(p, null, 2);
}

function toggleRaw() {
  const pre = document.getElementById('rawJson');
  pre.style.display = pre.style.display === 'none' ? 'block' : 'none';
}

// Lancer le chargement
loadProduct().then(() => lucide.createIcons());
