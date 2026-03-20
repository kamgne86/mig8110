const API_BASE = 'http://localhost:8001';

async function searchProducts() {
    const name = document.getElementById('searchName').value.trim();
    const brand = document.getElementById('searchBrand').value.trim();
    
    showLoading();
    hideError();
    
    try {
        const params = new URLSearchParams();
        if (name) params.append('q', name);
        if (brand) params.append('brand', brand);
        
        const response = await fetch(`${API_BASE}/products?${params}`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const products = await response.json();
        displayResults(products);
    } catch (error) {
        showError('Erreur API: ' + error.message + '<br>Vérifiez que l\'API tourne sur localhost:8000');
    }
}

function displayResults(products) {
    const resultsDiv = document.getElementById('results');
    const statsDiv = document.getElementById('stats');
    
    if (products.length === 0) {
        resultsDiv.innerHTML = '<p style="text-align: center; color: #666;">Aucun produit trouvé </p>';
        statsDiv.style.display = 'none';
        return;
    }
    
    // Stats
    statsDiv.innerHTML = `<strong>${products.length} produit(s) trouvé(s)</strong>`;
    statsDiv.style.display = 'block';
    
    // Produits
    resultsDiv.innerHTML = `
        <div class="product-grid">
            ${products.map(p => `
                <div class="product-card">
                    <div class="product-name">${p.product_name || 'Sans nom'}</div>
                    <div style="color: #666; font-size: 0.95em;">${p.brands || 'Sans marque'}</div>
                    <div class="grades">
                        ${p.nutriscore_grade ? `<span class="grade nutriscore-${p.nutriscore_grade.toLowerCase()}">${p.nutriscore_grade}</span>` : ''}
                    </div>
                    <div style="margin: 10px 0;">${p.energy_kcal_100g ? Math.round(p.energy_kcal_100g) + ' kcal/100g' : 'N/A'}</div>
                    ${p.front_url ? `<img src="${p.front_url}" alt="Photo" style="width: 100%; height: 200px; object-fit: cover; border-radius: 8px;">` : ''}
                    <a href="${API_BASE}/products/${p.code}" target="_blank" style="margin-top: 10px; display: inline-block; color: #007bff; text-decoration: none;">📋 Détails complets</a>
                </div>
            `).join('')}
        </div>
    `;
}

function showLoading() {
    document.getElementById('results').innerHTML = '<div class="loading">🔄 Recherche en cours...</div>';
}

function showError(message) {
    document.getElementById('error').innerHTML = message;
    document.getElementById('error').style.display = 'block';
    document.getElementById('results').innerHTML = '<p style="text-align: center;">Essayez à nouveau</p>';
}

function hideError() {
    document.getElementById('error').style.display = 'none';
}

function clearFilters() {
    document.getElementById('searchName').value = '';
    document.getElementById('searchBrand').value = '';
    searchProducts();
}

// Événements
document.addEventListener('keypress', function(e) {
    if (e.key === 'Enter') searchProducts();
});

document.getElementById('searchBrand').addEventListener('input', debounce(searchProducts, 500));

function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}
