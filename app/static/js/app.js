const API_BASE = '';  // même origine (FastAPI sert le front)

// Debounce helper
function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func.apply(this, args), wait);
  };
}

const allComparisonMetrics = [
  // Macronutriments
  { label: 'Calories', key: 'energy_kcal_100g', unit: 'kcal', icon: 'flame', better: 'lower', category: 'macro', default: true },
  { label: 'Lipides', key: 'fat_100g', unit: 'g', icon: 'droplets', better: 'lower', category: 'macro', default: true },
  { label: 'Acides gras saturés', key: 'saturated_fat_100g', unit: 'g', icon: 'activity', better: 'lower', category: 'macro', default: false },
  { label: 'Glucides', key: 'carbohydrates_100g', unit: 'g', icon: 'wheat', better: 'lower', category: 'macro', default: true },
  { label: 'Sucres', key: 'sugars_100g', unit: 'g', icon: 'candy', better: 'lower', category: 'macro', default: true },
  { label: 'Fibres', key: 'fiber_100g', unit: 'g', icon: 'leaf', better: 'higher', category: 'macro', default: false },
  { label: 'Protéines', key: 'proteins_100g', unit: 'g', icon: 'dumbbell', better: 'higher', category: 'macro', default: true },
  { label: 'Sel', key: 'salt_100g', unit: 'g', icon: 'circle-dot', better: 'lower', category: 'macro', default: true },
  // Minéraux
  { label: 'Calcium', key: 'calcium_100g', unit: 'mg', icon: 'bone', better: 'higher', category: 'mineral', default: false },
  { label: 'Fer', key: 'iron_100g', unit: 'mg', icon: 'droplet', better: 'higher', category: 'mineral', default: false },
  { label: 'Potassium', key: 'potassium_100g', unit: 'mg', icon: 'zap', better: 'higher', category: 'mineral', default: false },
];

// Initialiser avec les métriques par défaut
const comparisonMetrics = allComparisonMetrics.filter(m => m.default);

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
let allProducts = [];          // resultats bruts de l'API
let activeGrades = new Set();  // nutriscore selectionnes

// â”€â”€â”€ Helpers donnÃ©es â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
// validGrade() et parseTags() sont dÃ©finis dans utils.js

// â”€â”€â”€ Init â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
document.addEventListener('DOMContentLoaded', () => {
  const hasUrlParams = initializeFromURL();

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

  // Si on arrive avec un paramètre URL (ingredient ou category), lancer la recherche automatiquement
  if (hasUrlParams) {
    fetchAndRender();
  }
});

// â”€â”€â”€ Fetch API â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
let tagsearch = null; // ingredient ou category passé par URL
let tagsearchType = null;   // 'ingredient' ou 'category'

// Lire les paramètres URL au chargement
function initializeFromURL() {
  const params = new URLSearchParams(window.location.search);
  const ingredient = params.get('ingredient');
  const category = params.get('category');

  if (ingredient) {
    tagsearch = ingredient;
    tagsearchType = 'ingredient';
    document.getElementById('searchName').value = ingredient;
    return true;
  } else if (category) {
    tagsearch = category;
    tagsearchType = 'category';
    document.getElementById('searchName').value = category;
    return true;
  }
  return false;
}

async function fetchAndRender() {
  const name       = document.getElementById('searchName').value.trim();
  const brand      = document.getElementById('searchBrand').value.trim();

  showLoading();
  hideError();

  try {
    const params = new URLSearchParams();
    if (name) {
      // Si on vient d'une URL (ingredient/category), envoyer le bon paramètre
      if (tagsearchType === 'ingredient') {
        params.append('ingredient', name);
      } else if (tagsearchType === 'category') {
        params.append('category', name);
      } else {
        // Sinon c'est une recherche manuelle par nom
        params.append('q', name);
      }
    }
    if (brand) params.append('brand', brand);

    const res = await fetch(`${API_BASE}/products?${params}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);

    allProducts = await res.json();
    applyFilters();
  } catch (err) {
    showError("Erreur API : " + err.message + "<br>Vérifiez que l'API FastAPI est démarrée.");
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
    resultsDiv.innerHTML = '<p class="empty-msg">Aucun produit trouvé.</p>';
    return;
  }

  const limitWarning = allProducts.length === 500 ? ' <em>(limite de 500 atteinte, affinez votre recherche)</em>' : '';
  statsDiv.innerHTML = `<strong>${displayable.length} produit(s) affiché(s)</strong>${allProducts.length !== displayable.length ? ` sur ${allProducts.length} récupérés` : ''}${limitWarning}`;
  statsDiv.style.display = 'block';

  resultsDiv.innerHTML = displayable.map(p => {
    const ns = validGrade(p.nutriscore_grade);
    const es = validGrade(p.ecoscore_grade);
    const categoryBadges = (p.categories || [])
      .slice(0, 2)
      .map(cat => {
        // Support ancien format (string) et nouveau format (objet)
        const displayName = typeof cat === 'string' ? cat : (cat.child || cat.display || '');
        return `<span class="category-badge">${escHtml(displayName)}</span>`;
      })
      .join('');

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
          ${categoryBadges ? `<div class="product-categories">${categoryBadges}</div>` : ''}
          <div class="product-meta">
            <span>${p.energy_kcal_100g != null ? Math.round(p.energy_kcal_100g) + ' kcal' : '—'}</span>
            ${ns ? `<span class="badge ns-${ns}">${ns.toUpperCase()}</span>` : ''}
            ${es ? `<span class="badge eco es-${es}">Éco ${es.toUpperCase()}</span>` : ''}
          </div>
        </div>
        <button class="detail-btn" onclick="voirDetail('${escAttr(p.code)}')">Voir Détail</button>
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
    alert('Sélectionnez au moins 2 produits à comparer.');
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
          const display = v != null ? `${Math.round(v * 100) / 100} ${f.unit}` : '—';
          const isBest  = v != null && v === minVal && numVals.length > 1;
          return `<div class="compare-val-col ${isBest ? 'best-val' : ''}">${display}</div>`;
        }).join('')}
      </div>
    `;
  }).join('');

  const categoryRow = `
    <div class="compare-row">
      <div class="compare-field-col">Catégories</div>
      ${details
        .map(p => {
          const cats = (p.categories || []).slice(0, 2).map(cat => {
            // Support ancien format (string) et nouveau format (objet)
            if (typeof cat === 'string') return cat;
            return cat.child || cat.display || cat;
          }).join(', ') || '—';
          return `<div class="compare-val-col" style="text-align: left; font-size: 13px;">${escHtml(cats)}</div>`;
        })
        .join('')}
    </div>
  `;

  const metricStats = computeMetricStats(details);
  const nutriStats = computeNutriScoreStats(details);
  const healthScores = computeHealthScores(details, metricStats, nutriStats);
  const insightsSection = renderInsightsSection(details, metricStats, healthScores);

  document.getElementById('compareContent').innerHTML = header + rows + categoryRow + insightsSection;

  // Générer le graphique radar
  createRadarChart(details, comparisonMetrics);

  document.getElementById('compareModal').style.display = 'flex';

  lucide.createIcons();
}

let radarChartInstance = null;

function createRadarChart(products, metrics) {
  const container = document.getElementById('radarChartContainer');
  const canvas = document.getElementById('radarChart');

  if (!container || !canvas) return;

  // Détruire l'ancien graphique s'il existe
  if (radarChartInstance) {
    radarChartInstance.destroy();
    radarChartInstance = null;
  }

  // Préparer les labels (noms des métriques)
  const labels = metrics.map(m => m.label);

  // Préparer les datasets (un par produit)
  const colors = [
    { bg: 'rgba(79, 70, 229, 0.2)', border: 'rgb(79, 70, 229)' },    // Indigo
    { bg: 'rgba(16, 185, 129, 0.2)', border: 'rgb(16, 185, 129)' },  // Green
    { bg: 'rgba(245, 158, 11, 0.2)', border: 'rgb(245, 158, 11)' },  // Amber
    { bg: 'rgba(239, 68, 68, 0.2)', border: 'rgb(239, 68, 68)' }     // Red
  ];

  const datasets = products.map((product, idx) => {
    // Normaliser chaque métrique sur 0-100
    const data = metrics.map(metric => {
      const value = product[metric.key];
      if (value == null) return 0;

      // Pour les métriques "lower is better", inverser le score
      // On normalise à partir des valeurs de tous les produits pour cette métrique
      const allValues = products.map(p => p[metric.key]).filter(v => v != null);
      if (allValues.length === 0) return 0;

      const min = Math.min(...allValues);
      const max = Math.max(...allValues);

      if (max === min) return 50; // Si tous identiques, 50%

      // Normaliser 0-100
      let normalized = ((value - min) / (max - min)) * 100;

      // Si "better = lower", inverser
      if (metric.better === 'lower') {
        normalized = 100 - normalized;
      }

      return Math.round(normalized);
    });

    return {
      label: product.product_name || 'Sans nom',
      data: data,
      backgroundColor: colors[idx % colors.length].bg,
      borderColor: colors[idx % colors.length].border,
      borderWidth: 2,
      pointBackgroundColor: colors[idx % colors.length].border,
      pointBorderColor: '#fff',
      pointHoverBackgroundColor: '#fff',
      pointHoverBorderColor: colors[idx % colors.length].border
    };
  });

  // Créer le graphique
  radarChartInstance = new Chart(canvas, {
    type: 'radar',
    data: {
      labels: labels,
      datasets: datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      scales: {
        r: {
          beginAtZero: true,
          max: 100,
          ticks: {
            stepSize: 20,
            callback: function(value) {
              return value + '%';
            }
          },
          pointLabels: {
            font: {
              size: 12,
              weight: 'bold'
            }
          }
        }
      },
      plugins: {
        legend: {
          position: 'bottom',
          labels: {
            padding: 15,
            font: {
              size: 13
            }
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              return context.dataset.label + ': ' + context.parsed.r + '%';
            }
          }
        }
      }
    }
  });

  container.style.display = 'block';
}
function buildSummary(fields, products) {
  const lines = fields.map(field => {
    const values = products.map(p => p[field.key]).filter(v => v != null);
    if (!values.length) return `${field.label} : —`;
    const avg = values.reduce((sum, v) => sum + v, 0) / values.length;
    return `${field.label} (moyenne) : ${Math.round(avg * 100) / 100} ${field.unit}`;
  });
  const nutriAvg = computeAverageNutriScore(products);
  lines.push(`Nutri-score (numérique) : ${nutriAvg !== null ? nutriAvg.toFixed(2) : '—'}`);

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

function renderInsightsSection(products, metricStats, healthScores) {
  const ranking = [...healthScores].sort((a, b) => b.score - a.score);

  // 1. Podium global
  let podiumHtml = '<div class="ranking-podium">';

  ranking.forEach((entry, index) => {
    const rank = index + 1;
    const medalIcons = {
      1: { icon: 'trophy', color: 'gold' },
      2: { icon: 'medal', color: 'silver' },
      3: { icon: 'award', color: 'bronze' },
    };

    if (rank <= 3) {
      const medal = medalIcons[rank];
      podiumHtml += `
        <div class="medal-slot rank-${rank}">
          <i data-lucide="${medal.icon}" class="medal-icon ${medal.color}"></i>
          <strong>${escHtml(entry.product.product_name || 'Sans nom')}</strong>
          <span class="score">${entry.score.toFixed(1)}%</span>
        </div>
      `;
    }
  });

  podiumHtml += '</div>';

  // Remaining ranks si > 3 produits
  if (ranking.length > 3) {
    podiumHtml += '<div class="ranking-rest">';
    for (let i = 3; i < ranking.length; i++) {
      const entry = ranking[i];
      podiumHtml += `<div class="rank-item">
        ${i + 1}. ${escHtml(entry.product.product_name || 'Sans nom')} (${entry.score.toFixed(1)}%)
      </div>`;
    }
    podiumHtml += '</div>';
  }

  // 2. Générer insights automatiques
  const insights = [];

  // Meilleur en protéines
  const proteinMetric = metricStats.find(m => m.key === 'proteins_100g');
  if (proteinMetric) {
    const bestProtein = products.reduce((best, p) =>
      (p.proteins_100g || 0) > (best.proteins_100g || 0) ? p : best
    , products[0]);
    if (bestProtein.proteins_100g > 0) {
      insights.push({
        icon: 'dumbbell',
        color: '#10b981',
        title: 'Meilleur en protéines',
        text: `${bestProtein.product_name || 'Sans nom'} avec ${bestProtein.proteins_100g.toFixed(1)}g/100g`
      });
    }
  }

  // Moins calorique
  const calorieMetric = metricStats.find(m => m.key === 'energy_kcal_100g');
  if (calorieMetric && calorieMetric.min != null) {
    const lowestCal = products.find(p => p.energy_kcal_100g === calorieMetric.min);
    if (lowestCal) {
      insights.push({
        icon: 'flame',
        color: '#6366f1',
        title: 'Moins calorique',
        text: `${lowestCal.product_name || 'Sans nom'} avec ${Math.round(lowestCal.energy_kcal_100g)} kcal/100g`
      });
    }
  }

  // Attention au sucre
  const sugarMetric = metricStats.find(m => m.key === 'sugars_100g');
  if (sugarMetric && sugarMetric.max != null && sugarMetric.max > 10) {
    const highestSugar = products.find(p => p.sugars_100g === sugarMetric.max);
    if (highestSugar) {
      insights.push({
        icon: 'candy',
        color: '#f59e0b',
        title: 'Attention au sucre',
        text: `${highestSugar.product_name || 'Sans nom'} contient ${highestSugar.sugars_100g.toFixed(1)}g/100g`
      });
    }
  }

  // Riche en fibres
  const fiberMetric = metricStats.find(m => m.key === 'fiber_100g');
  if (fiberMetric && fiberMetric.max != null && fiberMetric.max > 2) {
    const highestFiber = products.find(p => p.fiber_100g === fiberMetric.max);
    if (highestFiber) {
      insights.push({
        icon: 'leaf',
        color: '#22c55e',
        title: 'Riche en fibres',
        text: `${highestFiber.product_name || 'Sans nom'} avec ${highestFiber.fiber_100g.toFixed(1)}g/100g`
      });
    }
  }

  // Attention au sel
  const saltMetric = metricStats.find(m => m.key === 'salt_100g');
  if (saltMetric && saltMetric.max != null && saltMetric.max > 1) {
    const highestSalt = products.find(p => p.salt_100g === saltMetric.max);
    if (highestSalt) {
      insights.push({
        icon: 'circle-dot',
        color: '#ef4444',
        title: 'Attention au sel',
        text: `${highestSalt.product_name || 'Sans nom'} contient ${highestSalt.salt_100g.toFixed(2)}g/100g`
      });
    }
  }

  // Limiter à 4 insights
  const topInsights = insights.slice(0, 4);

  const insightsHtml = topInsights.map(insight => `
    <div class="insight-card" style="border-left: 4px solid ${insight.color}">
      <div class="insight-header">
        <i data-lucide="${insight.icon}" style="width: 20px; height: 20px; stroke: ${insight.color}"></i>
        <strong>${insight.title}</strong>
      </div>
      <div class="insight-text">${insight.text}</div>
    </div>
  `).join('');

  return `
    <div class="insights-section">
      <h3 style="margin: 24px 0 12px; font-size: 18px; color: #1f2937; display: flex; align-items: center; gap: 8px;">
        <i data-lucide="trophy" style="width: 20px; height: 20px; stroke: #f59e0b;"></i>
        Classement global
      </h3>
      ${podiumHtml}

      <h3 style="margin: 24px 0 12px; font-size: 18px; color: #1f2937; display: flex; align-items: center; gap: 8px;">
        <i data-lucide="lightbulb" style="width: 20px; height: 20px; stroke: #4f46e5;"></i>
        Points clés
      </h3>
      <div class="insights-grid">
        ${insightsHtml}
      </div>
    </div>
  `;
}
function fermerComparaison() {
  // Détruire le graphique radar
  if (radarChartInstance) {
    radarChartInstance.destroy();
    radarChartInstance = null;
  }
  document.getElementById('radarChartContainer').style.display = 'none';
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

// ─── Métrique selection ────────────────────────────────────────────────────────────────

function loadSelectedMetrics() {
  const saved = localStorage.getItem('selectedMetrics');
  if (!saved) {
    return allComparisonMetrics.filter(m => m.default).map(m => m.key);
  }
  return JSON.parse(saved);
}

function updateComparisonMetrics(selectedKeys) {
  const metrics = allComparisonMetrics.filter(m => selectedKeys.includes(m.key));
  // Replace comparisonMetrics array contents
  comparisonMetrics.length = 0;
  comparisonMetrics.push(...metrics);
  localStorage.setItem('selectedMetrics', JSON.stringify(selectedKeys));
}

function toggleMetricsSelector() {
  const modal = document.getElementById('metricsModal');
  const selected = loadSelectedMetrics();

  const metricsByCategory = {};
  allComparisonMetrics.forEach(m => {
    if (!metricsByCategory[m.category]) metricsByCategory[m.category] = [];
    metricsByCategory[m.category].push(m);
  });

  let html = '';
  for (const [category, metrics] of Object.entries(metricsByCategory)) {
    const catLabel = category === 'macro' ? 'Macronutriments' : 'Minéraux';
    html += `<div style="margin-bottom: 16px;">
      <h4 style="margin: 0 0 8px; font-size: 14px; font-weight: 600; color: #475467;">${catLabel}</h4>
      <div style="display: flex; flex-direction: column; gap: 6px;">`;

    metrics.forEach(m => {
      const checked = selected.includes(m.key) ? 'checked' : '';
      html += `<label style="display: flex; align-items: center; gap: 8px; cursor: pointer; font-size: 14px;">
        <input type="checkbox" class="metric-checkbox" value="${m.key}" ${checked} data-label="${m.label}">
        <span>${m.label} (${m.unit})</span>
      </label>`;
    });

    html += `</div></div>`;
  }

  document.getElementById('metricsContent').innerHTML = html;
  modal.style.display = 'flex';
  lucide.createIcons();
}

function fermerMetricsSelector() {
  const selected = Array.from(document.querySelectorAll('.metric-checkbox:checked'))
    .map(cb => cb.value);

  if (selected.length === 0) {
    alert('Sélectionnez au moins une métrique');
    return;
  }

  updateComparisonMetrics(selected);
  document.getElementById('metricsModal').style.display = 'none';
}

function resetMetricsSelection() {
  document.querySelectorAll('.metric-checkbox').forEach(cb => {
    const m = allComparisonMetrics.find(m => m.key === cb.value);
    cb.checked = m && m.default;
  });
}

// Init metrics
(function initMetrics() {
  const selected = loadSelectedMetrics();
  updateComparisonMetrics(selected);
})();

// Init from URL params (if coming from tag click on detail page)
(function initFromUrlParams() {
  const params = new URLSearchParams(window.location.search);
  const ingredient = params.get('ingredient');
  if (ingredient) {
    tagsearch = ingredient;
    fetchAndRender();
  }
})();
