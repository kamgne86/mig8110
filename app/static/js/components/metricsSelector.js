// ─── Sélecteur de métriques de comparaison ───────────────────────────────────

function loadSelectedMetrics() {
  const saved = localStorage.getItem('selectedMetrics');
  if (!saved) {
    return allComparisonMetrics.filter(m => m.default).map(m => m.key);
  }
  return JSON.parse(saved);
}

function updateComparisonMetrics(selectedKeys) {
  const metrics = allComparisonMetrics.filter(m => selectedKeys.includes(m.key));
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

// Initialiser les métriques au chargement
(function initMetrics() {
  const selected = loadSelectedMetrics();
  updateComparisonMetrics(selected);
})();
