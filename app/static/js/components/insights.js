// ─── Statistiques et insights de comparaison ────────────────────────────────

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
    const breakdown = [];
    const values = comparisonMetrics
      .map(metric => {
        const stat = statsMap[metric.key];
        if (!stat || stat.min == null || stat.max == null || stat.max === stat.min) return null;
        const raw = product[metric.key];
        if (raw == null) return null;
        const normalized = (raw - stat.min) / (stat.max - stat.min);
        const contribution = metric.better === 'lower' ? 1 - normalized : normalized;
        breakdown.push({ metric, raw, contribution, statMin: stat.min, statMax: stat.max });
        return contribution;
      })
      .filter(v => v != null);
    const nutriRaw = nutriScoreOrder[product.nutriscore_grade ? product.nutriscore_grade.toLowerCase() : null];
    if (nutriRaw != null && nutriStats.min != null && nutriStats.max != null && nutriStats.max !== nutriStats.min) {
      const nutriNorm = (nutriRaw - nutriStats.min) / (nutriStats.max - nutriStats.min);
      values.push(nutriNorm);
      breakdown.push({ metric: { key: 'nutriscore', label: 'Nutri-Score', icon: 'star', unit: '', better: 'higher' }, raw: product.nutriscore_grade?.toUpperCase(), contribution: nutriNorm, statMin: nutriStats.min, statMax: nutriStats.max });
    }
    const score = values.length ? (values.reduce((acc, v) => acc + v, 0) / values.length) * 100 : 0;
    return { product, score: Math.round(score * 100) / 100, breakdown };
  });
}

function toggleScoreBreakdown(el) {
  const panel = el.nextElementSibling;
  const chevron = el.querySelector('.breakdown-chevron');
  const open = panel.style.display !== 'none';
  panel.style.display = open ? 'none' : 'block';
  el.style.borderRadius = open ? '' : 'var(--radius-md) var(--radius-md) 0 0';
  if (chevron) chevron.style.transform = open ? '' : 'rotate(180deg)';
  if (!open) lucide.createIcons();
}

function renderInsightsSection(products, metricStats, healthScores) {
  const ranking = [...healthScores].sort((a, b) => b.score - a.score);

  // 1. Classement avec barres de score
  const rankColors = ['#4f46e5', '#10b981', '#f59e0b', '#ef4444'];

  const leaderboardHtml = `
    <div class="leaderboard">
      ${ranking.map((entry, i) => {
        const color = rankColors[i] || '#9ca3af';
        const breakdownRows = entry.breakdown.map(b => {
          const healthPct = Math.round(b.contribution * 100);
          const barColor = healthPct >= 60 ? '#22c55e' : healthPct >= 30 ? '#f59e0b' : '#ef4444';
          const label = healthPct >= 60 ? 'Bon' : healthPct >= 30 ? 'Moyen' : 'Faible';
          const rawDisplay = typeof b.raw === 'number'
            ? `${Math.round(b.raw * 100) / 100} ${b.metric.unit}`
            : escHtml(String(b.raw ?? '—'));
          const arrow = b.metric.better === 'lower' ? '↓' : '↑';
          return `
            <div class="breakdown-chip" style="border-top-color: ${barColor};">
              <div class="chip-header">
                <i data-lucide="${b.metric.icon}" style="width:13px;height:13px;stroke:${barColor};"></i>
                <span>${escHtml(b.metric.label)}</span>
              </div>
              <div class="chip-value">
                <span>${rawDisplay}</span>
                <span class="chip-arrow" style="color:${barColor};">${arrow}</span>
              </div>
            </div>
          `;
        }).join('');

        return `
          <div class="leaderboard-item">
            <div class="leaderboard-row" onclick="toggleScoreBreakdown(this)" style="cursor: pointer;">
              <div class="leaderboard-rank-badge" style="background: ${color};">${i + 1}</div>
              <div class="leaderboard-info">
                <div class="leaderboard-name">${escHtml(entry.product.product_name || 'Sans nom')}</div>
                <div class="leaderboard-bar-wrap">
                  <div class="leaderboard-bar" style="width: ${entry.score.toFixed(1)}%; background: ${color};"></div>
                </div>
              </div>
              <span class="leaderboard-score" style="color: ${color};">${entry.score.toFixed(1)}%</span>
              <i data-lucide="chevron-down" class="breakdown-chevron"></i>
            </div>
            <div class="score-breakdown" style="display: none;">
              <div class="breakdown-header">Détail du score — ${entry.breakdown.length} critère${entry.breakdown.length > 1 ? 's' : ''}</div>
              <div class="breakdown-grid">${breakdownRows}</div>
            </div>
          </div>
        `;
      }).join('')}
    </div>
  `;

  // 2. Insights automatiques
  const insights = [];

  const proteinMetric = metricStats.find(m => m.key === 'proteins_100g');
  if (proteinMetric) {
    const bestProtein = products.reduce((best, p) =>
      (p.proteins_100g || 0) > (best.proteins_100g || 0) ? p : best
    , products[0]);
    if (bestProtein.proteins_100g > 0) {
      insights.push({
        icon: 'dumbbell', color: '#10b981',
        title: 'Meilleur en protéines',
        text: `${bestProtein.product_name || 'Sans nom'} avec ${bestProtein.proteins_100g.toFixed(1)}g/100g`
      });
    }
  }

  const calorieMetric = metricStats.find(m => m.key === 'energy_kcal_100g');
  if (calorieMetric && calorieMetric.min != null) {
    const lowestCal = products.find(p => p.energy_kcal_100g === calorieMetric.min);
    if (lowestCal) {
      insights.push({
        icon: 'flame', color: '#6366f1',
        title: 'Moins calorique',
        text: `${lowestCal.product_name || 'Sans nom'} avec ${Math.round(lowestCal.energy_kcal_100g)} kcal/100g`
      });
    }
  }

  const sugarMetric = metricStats.find(m => m.key === 'sugars_100g');
  if (sugarMetric && sugarMetric.max != null && sugarMetric.max > 10) {
    const highestSugar = products.find(p => p.sugars_100g === sugarMetric.max);
    if (highestSugar) {
      insights.push({
        icon: 'candy', color: '#f59e0b',
        title: 'Attention au sucre',
        text: `${highestSugar.product_name || 'Sans nom'} contient ${highestSugar.sugars_100g.toFixed(1)}g/100g`
      });
    }
  }

  const fiberMetric = metricStats.find(m => m.key === 'fiber_100g');
  if (fiberMetric && fiberMetric.max != null && fiberMetric.max > 2) {
    const highestFiber = products.find(p => p.fiber_100g === fiberMetric.max);
    if (highestFiber) {
      insights.push({
        icon: 'leaf', color: '#22c55e',
        title: 'Riche en fibres',
        text: `${highestFiber.product_name || 'Sans nom'} avec ${highestFiber.fiber_100g.toFixed(1)}g/100g`
      });
    }
  }

  const saltMetric = metricStats.find(m => m.key === 'salt_100g');
  if (saltMetric && saltMetric.max != null && saltMetric.max > 1) {
    const highestSalt = products.find(p => p.salt_100g === saltMetric.max);
    if (highestSalt) {
      insights.push({
        icon: 'circle-dot', color: '#ef4444',
        title: 'Attention au sel',
        text: `${highestSalt.product_name || 'Sans nom'} contient ${highestSalt.salt_100g.toFixed(2)}g/100g`
      });
    }
  }

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
      <h3 class="insights-title">
        <i data-lucide="bar-chart-2" style="width: 18px; height: 18px; stroke: #4f46e5;"></i>
        Classement santé
      </h3>
      ${leaderboardHtml}

      <h3 class="insights-title" style="margin-top: 20px;">
        <i data-lucide="lightbulb" style="width: 18px; height: 18px; stroke: #f59e0b;"></i>
        Points clés
      </h3>
      <div class="insights-grid">
        ${insightsHtml}
      </div>
    </div>
  `;
}
