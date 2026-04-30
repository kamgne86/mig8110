window.selectedSimilarProductCodes = window.selectedSimilarProductCodes || [];

function getSelectedSimilarProductCodes() {
  return Array.isArray(window.selectedSimilarProductCodes)
    ? window.selectedSimilarProductCodes
    : [];
}

function setSelectedSimilarProductCodes(codes) {
  window.selectedSimilarProductCodes = Array.from(new Set(
    (codes || [])
      .map(code => String(code || '').trim())
      .filter(Boolean)
  )).slice(0, 3);
}

function updateSimilarSelectionUI() {
  const selectedCodes = getSelectedSimilarProductCodes();
  const count = selectedCodes.length;
  const compareBtn = document.getElementById('compareSelectedSimilarBtn');
  const clearBtn = document.getElementById('clearSelectedSimilarBtn');

  if (compareBtn) compareBtn.disabled = count < 2;
  if (clearBtn) clearBtn.disabled = count === 0;

  document.querySelectorAll('.similar-select-input').forEach(input => {
    const code = String(input.value || '').trim();
    input.checked = selectedCodes.includes(code);
  });

  document.querySelectorAll('.similar-card').forEach(card => {
    const code = String(card.dataset.code || '').trim();
    card.classList.toggle('selected', selectedCodes.includes(code));
  });
}

function toggleSimilarSelection(code, shouldSelect) {
  const normalizedCode = String(code || '').trim();
  if (!normalizedCode) return;

  const selectedCodes = getSelectedSimilarProductCodes();
  const alreadySelected = selectedCodes.includes(normalizedCode);

  if (shouldSelect) {
    if (!alreadySelected && selectedCodes.length >= 3) {
      alert('Vous pouvez selectionner au maximum 3 produits similaires.');
      updateSimilarSelectionUI();
      return;
    }
    if (!alreadySelected) {
      setSelectedSimilarProductCodes([...selectedCodes, normalizedCode]);
    }
  } else {
    setSelectedSimilarProductCodes(selectedCodes.filter(item => item !== normalizedCode));
  }

  updateSimilarSelectionUI();
}

function clearSimilarSelection() {
  setSelectedSimilarProductCodes([]);
  updateSimilarSelectionUI();
}

async function compareSelectedSimilarProducts() {
  const currentCode = String(window.currentDetailProductCode || '').trim();
  const selectedCodes = getSelectedSimilarProductCodes();

  if (!currentCode) {
    alert('Produit principal introuvable pour la comparaison.');
    return;
  }

  if (selectedCodes.length < 2) {
    alert('Selectionnez 2 ou 3 produits similaires.');
    return;
  }

  if (typeof compareProductCodes !== 'function') {
    alert('La comparaison n est pas disponible sur cette page.');
    return;
  }

  await compareProductCodes([currentCode, ...selectedCodes]);
}

async function loadProduct() {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');

  if (!code) {
    document.getElementById('name').textContent = 'Code produit manquant';
    return;
  }

  try {
    const product = await fetchProduct(code);
    window.currentDetailProductCode = product.code;
    fillFromProduct(product);
  } catch (error) {
    document.getElementById('name').textContent = 'Erreur de chargement';
    console.error(error);
  }
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

function getTopIngredients(product, maxItems = 5) {
  const source = Array.isArray(product?.ingredients) && product.ingredients.length
    ? product.ingredients
    : getIngredientsList(product, maxItems);

  const out = [];
  const seen = new Set();

  for (const ingredient of source) {
    const clean = String(ingredient || '').trim();
    if (!clean) continue;

    const key = normalizeToken(clean);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(clean);
    if (out.length >= maxItems) break;
  }

  return out;
}

function renderCategoryLinks(categories) {
  if (!Array.isArray(categories) || !categories.length) {
    return '-';
  }

  return categories
    .map(cat => {
      if (typeof cat === 'string') {
        const label = String(cat || '').trim();
        if (!label) return '';
        if (label.includes('>')) {
          const parts = label.split('>').map(p => p.trim()).filter(Boolean);
          const visualPath = parts
            .map((part, index) => {
              const separator = index < parts.length - 1
                ? '<span class="cat-separator">&gt;</span>'
                : '';
              const partClass = index === parts.length - 1 ? 'cat-child' : 'cat-parent';
              return `<a href="/static/index.html?category=${encodeURIComponent(part)}" class="tag-link category-segment ${partClass}">${escHtml(part)}</a>${separator}`;
            })
            .join('');
          return `<span class="category-path">${visualPath}</span>`;
        }
        return `<a href="/static/index.html?category=${encodeURIComponent(label)}" class="tag-link">${escHtml(label)}</a>`;
      }

      if (cat && typeof cat === 'object') {
        const displayLabel = String(cat.display || cat.child || '').trim();
        if (!displayLabel) return '';
        const parts = displayLabel
          .split('>')
          .map(part => String(part || '').trim())
          .filter(Boolean);
        const visualPath = parts
          .map((part, index) => {
            const separator = index < parts.length - 1
              ? '<span class="cat-separator">&gt;</span>'
              : '';
            const partClass = index === parts.length - 1 ? 'cat-child' : 'cat-parent';
            return `<a href="/static/index.html?category=${encodeURIComponent(part)}" class="tag-link category-segment ${partClass}">${escHtml(part)}</a>${separator}`;
          })
          .join('');
        return `<span class="category-path">
          ${visualPath}
        </span>`;
      }

      return '';
    })
    .filter(Boolean)
    .join('');
}

function renderSimilarCard(product, meta) {
  const name = escHtml(product.product_name || 'Sans nom');
  const brand = escHtml(product.brands || 'Sans marque');
  const code = String(product.code || '').trim();
  const ingredientsText = meta.topIngredients.length
    ? meta.topIngredients.map(escHtml).join(', ')
    : '-';
  const image = product.front_url
    ? `<img src="${escHtml(product.front_url)}" alt="Image produit" loading="lazy" />`
    : '<span>IMAGE</span>';

  return `
    <article class="similar-card" data-code="${escAttr(code)}">
      <div class="similar-media">${image}</div>
      <div class="similar-body">
        <div class="similar-card-top">
          <label class="similar-select-control">
            <input
              class="similar-select-input"
              type="checkbox"
              value="${escAttr(code)}"
              onchange="toggleSimilarSelection('${escAttr(code)}', this.checked)"
            />
            <span>Selectionner pour comparaison</span>
          </label>
        </div>
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
            <span class="similar-field-label">Similarite semantique</span>
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
      <div class="similar-actions">
        <button class="small-btn compare-btn" onclick="compareWithCurrentProduct('${escAttr(product.code)}')">Comparer</button>
        <a class="detail-btn" href="/static/detail.html?code=${escAttr(product.code)}">Voir detail</a>
      </div>
    </article>
  `;
}

async function compareWithCurrentProduct(candidateCode) {
  const currentCode = String(window.currentDetailProductCode || '').trim();
  const selectedCode = String(candidateCode || '').trim();

  if (!currentCode || !selectedCode) {
    alert('Codes produits manquants pour la comparaison.');
    return;
  }

  if (typeof compareProductCodes !== 'function') {
    alert('La comparaison n est pas disponible sur cette page.');
    return;
  }

  await compareProductCodes([currentCode, selectedCode]);
}

async function loadSimilarProducts(baseProduct) {
  const container = document.getElementById('similarProducts');
  if (!container) return;

  try {
    clearSimilarSelection();
    const ranked = await fetchSimilarProducts(baseProduct.code, 4);

    if (!ranked.length) {
      container.innerHTML = '<p class="empty-msg">Aucun produit similaire trouve.</p>';
      return;
    }

    const cards = ranked
      .map(product => renderSimilarCard(product, {
        categoryLabel: product.category_label || getTargetCategory(product) || '-',
        topIngredients: Array.isArray(product.top_ingredients) ? product.top_ingredients : [],
        ingredientSimilarityPct: Number(product.ingredient_similarity_pct || 0),
        nutrimentSimilarityPct: Number(product.nutriment_similarity_pct || 0),
        categorySimilarityPct: Number(product.category_similarity_pct || 0),
        overallSimilarityPct: Number(product.overall_similarity_pct || 0),
      }))
      .join('');

    container.innerHTML = `
      <div class="similar-selection-toolbar">
        <div class="similar-selection-actions">
          <button
            id="compareSelectedSimilarBtn"
            class="small-btn compare-btn"
            onclick="compareSelectedSimilarProducts()"
            disabled
          >
            Comparer la selection
          </button>
          <button
            id="clearSelectedSimilarBtn"
            class="small-btn"
            onclick="clearSimilarSelection()"
            disabled
          >
            Effacer
          </button>
        </div>
      </div>
      ${cards}
    `;
    updateSimilarSelectionUI();
  } catch (error) {
    container.innerHTML = '<p class="empty-msg">Erreur lors du chargement des produits similaires.</p>';
    console.error(error);
  }
}

function fillFromProduct(product) {
  document.title = product.product_name || 'Detail produit';
  document.getElementById('name').textContent = product.product_name || 'Sans nom';
  document.getElementById('brand').textContent = product.brands || 'Sans marque';

  const nutriScoreGrade = validGrade(product.nutriscore_grade);
  const nutriScoreContainer = document.getElementById('nutriScore');
  if (nutriScoreGrade) {
    nutriScoreContainer.className = '';
    nutriScoreContainer.innerHTML = renderNutriScoreBand(nutriScoreGrade);
  } else {
    nutriScoreContainer.textContent = '-';
    nutriScoreContainer.className = 'nutri-score';
  }

  const ecoScoreGrade = validGrade(product.ecoscore_grade);
  const ecoScore = document.getElementById('ecoScore');
  ecoScore.textContent = ecoScoreGrade ? ecoScoreGrade.toUpperCase() : '-';
  ecoScore.className = ecoScoreGrade ? `nutri-score eco es-${ecoScoreGrade}` : 'nutri-score eco';

  const nutrimentFields = [
    { id: 'calories', key: 'energy_kcal_100g', unit: 'kcal', decimals: 0 },
    { id: 'carbs', key: 'carbohydrates_100g', unit: 'g', decimals: 2 },
    { id: 'sugars', key: 'sugars_100g', unit: 'g', decimals: 2 },
    { id: 'fiber', key: 'fiber_100g', unit: 'g', decimals: 2 },
    { id: 'fat', key: 'fat_100g', unit: 'g', decimals: 2 },
    { id: 'saturated', key: 'saturated_fat_100g', unit: 'g', decimals: 2 },
    { id: 'transFat', key: 'trans_fat_100g', unit: 'g', decimals: 2 },
    { id: 'cholesterol', key: 'cholesterol_100g', unit: 'mg', decimals: 1 },
    { id: 'proteins', key: 'proteins_100g', unit: 'g', decimals: 2 },
    { id: 'salt', key: 'salt_100g', unit: 'g', decimals: 2 },
    { id: 'calcium', key: 'calcium_100g', unit: 'mg', decimals: 1 },
    { id: 'iron', key: 'iron_100g', unit: 'mg', decimals: 2 },
    { id: 'potassium', key: 'potassium_100g', unit: 'mg', decimals: 1 },
  ];

  nutrimentFields.forEach(field => {
    document.getElementById(field.id).textContent = val(product[field.key], field.unit, field.decimals);
  });

  const facts = [];
  if (product.quantity) {
    facts.push({ icon: 'package', label: 'Contenance', value: escHtml(product.quantity) });
  }
  if (product.serving_size) {
    facts.push({ icon: 'utensils', label: 'Portion', value: escHtml(product.serving_size) });
  }

  const ingredientCount = product.ingredients_n != null ? Math.round(product.ingredients_n) : null;
  if (ingredientCount != null) {
    const sublabelColor = ingredientCount <= 10 ? '#16a34a' : ingredientCount <= 20 ? '#d97706' : '#dc2626';
    const label = ingredientCount <= 10 ? 'Peu transforme' : ingredientCount <= 20 ? 'Modere' : 'Ultra-transforme';
    facts.push({
      icon: 'shopping-basket',
      label: 'Ingredients',
      value: String(ingredientCount),
      sublabel: label,
      sublabelColor,
    });
  }

  const keyFacts = document.getElementById('keyFacts');
  if (facts.length) {
    keyFacts.innerHTML = facts.map(fact => `
      <div class="fact-chip">
        <i data-lucide="${fact.icon}" class="fact-icon"></i>
        <span class="fact-value">${fact.value}</span>
        <span class="fact-label">${fact.label}</span>
        ${fact.sublabel ? `<span class="fact-sublabel" style="color:${fact.sublabelColor};">${fact.sublabel}</span>` : ''}
      </div>
    `).join('');
  } else {
    keyFacts.style.display = 'none';
  }

  const ingredientsList = getIngredientsList(product);
  if (ingredientsList && ingredientsList.length) {
    document.getElementById('ingredients').innerHTML = ingredientsList
      .map(ingredient => `<a href="/static/index.html?ingredient=${encodeURIComponent(ingredient)}" class="tag-link">${escHtml(ingredient)}</a>`)
      .join('');
  } else {
    document.getElementById('ingredients').textContent = '-';
  }

  document.getElementById('categoriesList').innerHTML = renderCategoryLinks(product.categories);

  if (product.front_url) {
    const imageBox = document.getElementById('imageBox');
    const image = document.createElement('img');
    image.src = product.front_url;
    image.alt = 'Photo produit';
    image.style.cssText = 'width:100%;height:100%;object-fit:cover;border-radius:12px;';
    imageBox.replaceChildren(image);
  }

  loadSimilarProducts(product);
  document.getElementById('rawJsonSection').style.display = 'block';
  document.getElementById('rawJson').textContent = JSON.stringify(product, null, 2);
  lucide.createIcons();
}

function toggleRaw() {
  const pre = document.getElementById('rawJson');
  pre.style.display = pre.style.display === 'none' ? 'block' : 'none';
}

loadProduct();
