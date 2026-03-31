const API_BASE = '';  // mÃªme origine (FastAPI sert le front)
const comparisonMetrics = [

  { label: 'Calories', key: 'energy_kcal_100g', unit: 'kcal', icon: 'flame', better: 'lower' },

  { label: 'Lipides', key: 'fat_100g', unit: 'g', icon: 'droplets', better: 'lower' },

  { label: 'Glucides', key: 'carbohydrates_100g', unit: 'g', icon: 'wheat', better: 'lower' },

  { label: 'Sucres', key: 'sugars_100g', unit: 'g', icon: 'candy', better: 'lower' },

  { label: 'ProtÃƒÂ©ines', key: 'proteins_100g', unit: 'g', icon: 'dumbbell', better: 'higher' },

  { label: 'Sel', key: 'salt_100g', unit: 'g', icon: 'circle-dot', better: 'lower' },

];

const nutriScoreOrder = { a: 5, b: 4, c: 3, d: 2, e: 1 };

function getIngredientsListSafe(product, maxItems = null) {
  if (!product || typeof product !== 'object') return [];

  if (typeof getIngredientsList === 'function') {
    return getIngredientsList(product, maxItems);
  }

  const tagsList = parseTags(product.ingredients_tags, maxItems);
  if (tagsList.length) return tagsList;

  const preferredFields = [
    'ingredients_text',
    'ingredients_text_fr',
    'ingredients_text_en',
    'ingredients_text_de',
    'ingredients_text_es',
    'ingredients_text_it',
  ];

  let ingredientsText = null;
  for (const key of preferredFields) {
    const value = product[key];
    if (typeof value === 'string' && value.trim()) {
      ingredientsText = value;
      break;
    }
  }

  if (!ingredientsText) {
    const extraKey = Object.keys(product)
      .filter(key => /^ingredients_text_[a-z]{2}$/.test(key))
      .sort()
      .find(key => typeof product[key] === 'string' && product[key].trim());
    if (extraKey) ingredientsText = product[extraKey];
  }

  if (!ingredientsText) return [];

  const parts = ingredientsText
    .split(/[\n,;]+/)
    .map(part => part.trim())
    .filter(Boolean);
  return maxItems ? parts.slice(0, maxItems) : parts;
}




// â”€â”€â”€ Ã‰tat global â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let allProducts = [];          // rÃ©sultats bruts de l'API
let activeGrades = new Set();  // nutriscore sÃ©lectionnÃ©s

// â”€â”€â”€ Helpers donnÃ©es â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// validGrade() et parseTags() sont dÃ©finis dans utils.js

// â”€â”€â”€ Init â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
document.addEventListener('DOMContentLoaded', () => {
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

  document.addEventListener('keypress', e => {
    if (e.key === 'Enter') fetchAndRender();
  });

  document.getElementById('searchBrand').addEventListener('input', debounce(fetchAndRender, 500));
  document
    .getElementById('searchIngredient')
    .addEventListener('input', debounce(fetchAndRender, 500));
});

// â”€â”€â”€ Fetch API â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function fetchAndRender() {
  const name       = document.getElementById('searchName').value.trim();
  const brand      = document.getElementById('searchBrand').value.trim();
  const ingredient = document.getElementById('searchIngredient').value.trim();

  showLoading();
  hideError();

  try {
    const params = new URLSearchParams();
    if (name)       params.append('q', name);
    if (brand)      params.append('brand', brand);
    if (ingredient) params.append('ingredients', ingredient);

    const res = await fetch(`${API_BASE}/products?${params}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    allProducts = await res.json();
    applyFilters();
  } catch (err) {
    showError("Erreur API : " + err.message + "<br>VÃ©rifiez que l'API FastAPI est dÃ©marrÃ©e.");
  }
}

// â”€â”€â”€ Filtres client-side â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function applyFilters() {
  const maxSalt  = parseFloat(document.getElementById('saltSlider').value);
  const maxSugar = parseFloat(document.getElementById('sugarSlider').value);
  const maxCal   = parseFloat(document.getElementById('calSlider').value);
  const maxFat   = parseFloat(document.getElementById('fatSlider').value);

  const filtered = allProducts.filter(p => {
    if (activeGrades.size > 0) {
      const grade = validGrade(p.nutriscore_grade);
      if (!grade || !activeGrades.has(grade)) return false;
    }
    if (p.salt_100g        != null && p.salt_100g        > maxSalt)  return false;
    if (p.sugars_100g      != null && p.sugars_100g      > maxSugar) return false;
    if (p.energy_kcal_100g != null && p.energy_kcal_100g > maxCal)   return false;
    if (p.fat_100g         != null && p.fat_100g         > maxFat)   return false;
    return true;
  });

  renderProducts(filtered);
}

// â”€â”€â”€ Rendu produits â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function renderProducts(products) {
  const statsDiv   = document.getElementById('stats');
  const resultsDiv = document.getElementById('results');

  const displayable = products.filter(p => p.product_name && p.product_name.trim() !== '');

  if (displayable.length === 0) {
    statsDiv.style.display = 'none';
    resultsDiv.innerHTML = '<p class="empty-msg">Aucun produit trouvÃ©.</p>';
    return;
  }

  const limitWarning = allProducts.length === 500 ? ' <em>(limite de 500 atteinte, affinez votre recherche)</em>' : '';
  statsDiv.innerHTML = `<strong>${displayable.length} produit(s) affichÃ©(s)</strong>${allProducts.length !== displayable.length ? ` sur ${allProducts.length} rÃ©cupÃ©rÃ©s` : ''}${limitWarning}`;
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
            : '<i data-lucide="image" style="width:28px;height:28px;stroke:#9ca3af;"></i>'}
        </div>
        <div class="product-info">
          <h3>${escHtml(p.product_name)}</h3>
          <p>${escHtml(p.brands || 'Sans marque')}</p>
          <div class="product-meta">
            <span>${p.energy_kcal_100g != null ? Math.round(p.energy_kcal_100g) + ' kcal' : 'â€”'}</span>
            ${ns ? `<span class="badge ns-${ns}">${ns.toUpperCase()}</span>` : ''}
            ${es ? `<span class="badge eco es-${es}">Ã‰co ${es.toUpperCase()}</span>` : ''}
          </div>
        </div>
        <button class="detail-btn" onclick="voirDetail('${escAttr(p.code)}')">Voir DÃ©tail</button>
      </div>
    `;
  }).join('');

  // Obligatoire : re-traiter les <i data-lucide="..."> injectÃ©s dynamiquement
  lucide.createIcons();
}

// â”€â”€â”€ Navigation dÃ©tail â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function voirDetail(code) {
  window.location.href = `/static/detail.html?code=${encodeURIComponent(code)}`;
}

// â”€â”€â”€ Comparaison â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
async function comparerProduits() {
  const checked = [...document.querySelectorAll('.compare-check:checked')];

  if (checked.length < 2) {
    alert('SÃ©lectionnez au moins 2 produits Ã  comparer.');
    return;
  }
  if (checked.length > 4) {
    alert('Maximum 4 produits en comparaison.');
    return;
  }

  const codes = checked.map(c => c.value);

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

  const fields = comparisonMetrics;

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
    const vals    = details.map(p => p[f.key]);
    const numVals = vals.filter(v => v != null);
    const minVal  = numVals.length ? Math.min(...numVals) : null;

    return `
      <div class="compare-row">
        <div class="compare-field-col">
          <i data-lucide="${f.icon}" class="nutri-icon"></i>${f.label}
        </div>
        ${vals.map(v => {
          const display = v != null ? `${Math.round(v * 100) / 100} ${f.unit}` : 'â€”';
          const isBest  = v != null && v === minVal && numVals.length > 1;
          return `<div class="compare-val-col ${isBest ? 'best-val' : ''}">${display}</div>`;
        }).join('')}
      </div>
    `;
  }).join('');

  const vitCKeywords = ['vitamin c', 'vitamine c', 'ascorbic acid', 'acide ascorbique', 'ascorbate'];
  const hasVitaminC = tag => vitCKeywords.some(keyword => tag.toLowerCase().includes(keyword));

  const parsedIngredients = details.map(p => getIngredientsListSafe(p, 20));
  const vitCIngredients = parsedIngredients.map(list => list.filter(hasVitaminC));
  const positiveCounts = parsedIngredients.map(list => list.length).filter(count => count > 0);
  const bestIngredientCount = positiveCounts.length ? Math.min(...positiveCounts) : null;
  const maxVitaminCCount = Math.max(...vitCIngredients.map(list => list.length));

  const ingredientRow = `
    <div class="compare-row ingredient-row">
      <div class="compare-field-col">IngrÃ©dients</div>
      ${parsedIngredients
        .map((list, index) => {
          const vitc = vitCIngredients[index];
          const others = list.filter(tag => !hasVitaminC(tag));

          const parts = [];
          if (vitc.length) {
            parts.push(
              `<span class="ingredient-vitc"><strong>Vit C :</strong> ${escHtml(vitc.join(', '))}</span>`
            );
          }
          if (others.length) parts.push(escHtml(others.join(', ')));
          if (!parts.length) parts.push('â€”');

          const isBest =
            bestIngredientCount !== null &&
            parsedIngredients[index].length === bestIngredientCount &&
            parsedIngredients[index].length > 0;

          const hasVitC = vitc.length > 0;
          const vitcHighlight =
            hasVitC && vitc.length === maxVitaminCCount && maxVitaminCCount > 0 ? ' vitc' : '';

          return `<div class="compare-val-col ingredient-val${isBest ? ' best' : ''}${vitcHighlight}">${parts.join(
            '<br>'
          )}</div>`;
        })
        .join('')}
    </div>
  `;

  const metricStats = computeMetricStats(details);
  const nutriStats = computeNutriScoreStats(details);
  const healthScores = computeHealthScores(details, metricStats, nutriStats);
  const summarySection = renderSummarySection(metricStats, nutriStats, healthScores);

  document.getElementById('compareContent').innerHTML = header + rows + ingredientRow + summarySection;
  document.getElementById('compareModal').style.display = 'flex';

  lucide.createIcons();
}
function buildSummary(fields, products) {
  const lines = fields.map(field => {
    const values = products.map(p => p[field.key]).filter(v => v != null);
    if (!values.length) return `${field.label} : â€”`;
    const avg = values.reduce((sum, v) => sum + v, 0) / values.length;
    return `${field.label} (moyenne) : ${Math.round(avg * 100) / 100} ${field.unit}`;
  });
  const nutriAvg = computeAverageNutriScore(products);
  lines.push(`Nutri-score (numÃ©rique) : ${nutriAvg !== null ? nutriAvg.toFixed(2) : 'â€”'}`);

  return `
    <div class="compare-summary">
      <h3>Moyennes</h3>
      <ul>
        ${lines.map(line => `<li>${line}</li>`).join('')}
      </ul>
    </div>
  `;
}

function computeAverageNutriScore(products) {
  const mapping = { a: 5, b: 4, c: 3, d: 2, e: 1 };
  const values = products
    .map(p => (p.nutriscore_grade ? mapping[p.nutriscore_grade.toLowerCase()] : null))
    .filter(v => v != null);
  if (!values.length) return null;
  return values.reduce((sum, v) => sum + v, 0) / values.length;
}

function computeMetricStats(products) {
  return comparisonMetrics.map(metric => {
    const values = products.map(p => p[metric.key]).filter(v => v != null);
    if (!values.length) return { ...metric, min: null, max: null, avg: null };
    const sum = values.reduce((acc, v) => acc + v, 0);
    return {
      ...metric,
      min: Math.min(...values),
      max: Math.max(...values),
      avg: sum / values.length,
    };
  });
}

function computeNutriScoreStats(products) {
  const values = products
    .map(p => nutriScoreOrder[p.nutriscore_grade ? p.nutriscore_grade.toLowerCase() : null])
    .filter(v => v != null);
  if (!values.length) return { min: null, max: null, avg: null };
  const sum = values.reduce((acc, v) => acc + v, 0);
  return {
    min: Math.min(...values),
    max: Math.max(...values),
    avg: sum / values.length,
  };
}

function computeHealthScores(products, metricStats, nutriStats) {
  const statsMap = metricStats.reduce((acc, stat) => {
    acc[stat.key] = stat;
    return acc;
  }, {});
  return products.map(product => {
    const values = comparisonMetrics
      .map(metric => {
        const stat = statsMap[metric.key];
        if (!stat || stat.min == null || stat.max == null || stat.max === stat.min) return null;
        const raw = product[metric.key];
        if (raw == null) return null;
        const normalized = (raw - stat.min) / (stat.max - stat.min);
        return metric.better === 'lower' ? 1 - normalized : normalized;
      })
      .filter(v => v != null);
    const nutriRaw = nutriScoreOrder[product.nutriscore_grade ? product.nutriscore_grade.toLowerCase() : null];
    if (nutriRaw != null && nutriStats.min != null && nutriStats.max != null && nutriStats.max !== nutriStats.min) {
      const nutriNorm = (nutriRaw - nutriStats.min) / (nutriStats.max - nutriStats.min);
      values.push(nutriNorm);
    }
    const score = values.length ? (values.reduce((acc, v) => acc + v, 0) / values.length) * 100 : 0;
    return { product, score: Math.round(score * 100) / 100 };
  });
}

function renderSummarySection(metricStats, nutriStats, healthScores) {
  const rows = metricStats.map(stat => ({
    label: `${stat.label} (${stat.unit})`,
    min: stat.min != null ? Math.round(stat.min * 100) / 100 : '-',
    max: stat.max != null ? Math.round(stat.max * 100) / 100 : '-',
    avg: stat.avg != null ? Math.round(stat.avg * 100) / 100 : '-',
  }));

  rows.push({
    label: 'Nutri-score (num)',
    min: nutriStats.min != null ? nutriStats.min.toFixed(2) : '-',
    max: nutriStats.max != null ? nutriStats.max.toFixed(2) : '-',
    avg: nutriStats.avg != null ? nutriStats.avg.toFixed(2) : '-',
  });

  const ranking = [...healthScores].sort((a, b) => b.score - a.score);
  const rankingHtml = ranking.length
    ? `
      <ol>
        ${ranking
          .map((entry, index) => `<li>${index + 1}. ${escHtml(entry.product.product_name || 'Sans nom')}</li>`)
          .join('')}
      </ol>
    `
    : '<span>-</span>';

  const rowHtml = rows
    .map((row, index) => `
      <tr>
        <td>${row.label}</td>
        <td>${row.min}</td>
        <td>${row.max}</td>
        <td>${row.avg}</td>
        ${index === 0 ? `<td class="agg-ranking-cell" rowspan="${rows.length}">${rankingHtml}</td>` : ''}
      </tr>
    `)
    .join('');

  return `
    <div class="summary-section">
      <h3>Aggregation</h3>
      <div class="agg-table-wrap">
        <table class="agg-table">
          <thead>
            <tr>
              <th>Mesure</th>
              <th>Min</th>
              <th>Max</th>
              <th>Moyenne</th>
              <th>Classement sante</th>
            </tr>
          </thead>
          <tbody>${rowHtml}</tbody>
        </table>
      </div>
    </div>
  `;
}
function fermerComparaison() {
  document.getElementById('compareModal').style.display = 'none';
}

// â”€â”€â”€ Reset â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function resetFilters() {
  activeGrades.clear();
  document.querySelectorAll('#nutriFilters button').forEach(b => b.classList.remove('active'));

  document.getElementById('saltSlider').value  = 5;    updateSliderLabel('saltVal',  '5.00', 'g/100g');
  document.getElementById('sugarSlider').value = 100;  updateSliderLabel('sugarVal', 100,    'g/100g');
  document.getElementById('calSlider').value   = 1000; updateSliderLabel('calVal',   1000,   'kcal/100g');
  document.getElementById('fatSlider').value   = 100;  updateSliderLabel('fatVal',   100,    'g/100g');

  applyFilters();
}

// â”€â”€â”€ Utilitaires â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function updateSliderLabel(id, value, unit) {
  document.getElementById(id).textContent = `${value} ${unit}`;
}

function showLoading() {
  document.getElementById('results').innerHTML = '<p class="empty-msg">Recherche en cours...</p>';
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

