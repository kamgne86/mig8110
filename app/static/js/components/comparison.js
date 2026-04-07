// ─── Comparaison de produits ─────────────────────────────────────────────────

function formatCategoryDisplay(categories) {
  return (categories || []).slice(0, 2).map(cat => {
    if (typeof cat === 'string') return cat;
    return cat.child || cat.display || '';
  }).join(', ') || '—';
}

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
    details = await Promise.all(codes.map(code => fetchProduct(code)));
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
            ? renderNutriScoreBand(p.nutriscore_grade)
            : ''}
        </div>
      `).join('')}
    </div>
  `;

  const rows = fields.map(f => {
    const vals    = details.map(p => p[f.key]);
    const numVals = vals.filter(v => v != null);
    const bestVal = numVals.length
      ? (f.better === 'lower' ? Math.min(...numVals) : Math.max(...numVals))
      : null;

    return `
      <div class="compare-row">
        <div class="compare-field-col">
          <i data-lucide="${f.icon}" class="nutri-icon"></i>${f.label}
        </div>
        ${vals.map(v => {
          const display = v != null ? `${Math.round(v * 100) / 100} ${f.unit}` : '—';
          const isBest  = v != null && v === bestVal && numVals.length > 1;
          return `<div class="compare-val-col ${isBest ? 'best-val' : ''}">${display}</div>`;
        }).join('')}
      </div>
    `;
  }).join('');

  const categoryRow = `
    <div class="compare-row">
      <div class="compare-field-col">Catégories</div>
      ${details.map(p =>
        `<div class="compare-val-col" style="text-align: left; font-size: 13px;">${escHtml(formatCategoryDisplay(p.categories))}</div>`
      ).join('')}
    </div>
  `;

  const metricStats = computeMetricStats(details);
  const nutriStats = computeNutriScoreStats(details);
  const healthScores = computeHealthScores(details, metricStats, nutriStats);
  const insightsSection = renderInsightsSection(details, metricStats, healthScores);

  document.getElementById('compareContent').innerHTML = header + rows + categoryRow + insightsSection;

  createRadarChart(details, comparisonMetrics);

  document.getElementById('compareModal').style.display = 'flex';
  lucide.createIcons();
}

function createRadarChart(products, metrics) {
  const container = document.getElementById('radarChartContainer');
  const canvas = document.getElementById('radarChart');
  if (!container || !canvas) return;

  if (state.radarChartInstance) {
    state.radarChartInstance.destroy();
    state.radarChartInstance = null;
  }

  const labels = metrics.map(m => m.label);
  const colors = [
    { bg: 'rgba(79, 70, 229, 0.2)', border: 'rgb(79, 70, 229)' },
    { bg: 'rgba(16, 185, 129, 0.2)', border: 'rgb(16, 185, 129)' },
    { bg: 'rgba(245, 158, 11, 0.2)', border: 'rgb(245, 158, 11)' },
    { bg: 'rgba(239, 68, 68, 0.2)', border: 'rgb(239, 68, 68)' }
  ];

  const datasets = products.map((product, idx) => {
    const data = metrics.map(metric => {
      const value = product[metric.key];
      if (value == null) return 0;
      const allValues = products.map(p => p[metric.key]).filter(v => v != null);
      if (allValues.length === 0) return 0;
      const min = Math.min(...allValues);
      const max = Math.max(...allValues);
      if (max === min) return 50;
      let normalized = ((value - min) / (max - min)) * 100;
      if (metric.better === 'lower') normalized = 100 - normalized;
      return Math.round(normalized);
    });

    return {
      label: product.product_name || 'Sans nom',
      data,
      backgroundColor: colors[idx % colors.length].bg,
      borderColor: colors[idx % colors.length].border,
      borderWidth: 2,
      pointBackgroundColor: colors[idx % colors.length].border,
      pointBorderColor: '#fff',
      pointHoverBackgroundColor: '#fff',
      pointHoverBorderColor: colors[idx % colors.length].border
    };
  });

  state.radarChartInstance = new Chart(canvas, {
    type: 'radar',
    data: { labels, datasets },
    options: {
      responsive: true,
      maintainAspectRatio: true,
      scales: {
        r: {
          beginAtZero: true,
          max: 100,
          ticks: { stepSize: 20, callback: v => v + '%' },
          pointLabels: { font: { size: 12, weight: 'bold' } }
        }
      },
      plugins: {
        legend: { position: 'bottom', labels: { padding: 15, font: { size: 13 } } },
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.r + '%' } }
      }
    }
  });

  container.style.display = 'block';
}

function fermerComparaison() {
  if (state.radarChartInstance) {
    state.radarChartInstance.destroy();
    state.radarChartInstance = null;
  }
  document.getElementById('radarChartContainer').style.display = 'none';
  document.getElementById('compareModal').style.display = 'none';
}
