// ─── Rendu des cartes produit ────────────────────────────────────────────────

function formatCategoryBadges(categories, maxBadges = 2) {
  return (categories || [])
    .slice(0, maxBadges)
    .map(cat => {
      const displayName = typeof cat === 'string' ? cat : (cat.child || cat.display || '');
      return `<span class="category-badge">${escHtml(displayName)}</span>`;
    })
    .join('');
}

function renderProducts(products) {
  const statsDiv   = document.getElementById('stats');
  const resultsDiv = document.getElementById('results');

  const displayable = products.filter(p => p.product_name && p.product_name.trim() !== '');

  if (displayable.length === 0) {
    statsDiv.style.display = 'none';
    resultsDiv.innerHTML = '<p class="empty-msg">Aucun produit trouvé.</p>';
    return;
  }

  const limitWarning = state.allProducts.length === 500 ? ' <em>(limite de 500 atteinte, affinez votre recherche)</em>' : '';
  statsDiv.innerHTML = `<strong>${displayable.length} produit(s) affiché(s)</strong>${state.allProducts.length !== displayable.length ? ` sur ${state.allProducts.length} récupérés` : ''}${limitWarning}`;
  statsDiv.style.display = 'block';

  resultsDiv.innerHTML = displayable.map(p => {
    const ns = validGrade(p.nutriscore_grade);
    const es = validGrade(p.ecoscore_grade);
    const categoryBadges = formatCategoryBadges(p.categories);

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
            ${ns ? renderNutriScoreBand(ns) : ''}
            ${es ? `<span class="badge eco es-${es}">Éco ${es.toUpperCase()}</span>` : ''}
          </div>
        </div>
        <button class="detail-btn" onclick="voirDetail('${escAttr(p.code)}')">Voir Détail</button>
      </div>
    `;
  }).join('');

  lucide.createIcons();
}

function voirDetail(code) {
  window.location.href = `/static/detail.html?code=${encodeURIComponent(code)}`;
}
