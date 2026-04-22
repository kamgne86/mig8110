// ─── Point d'entrée — FoodHealth Advisor ─────────────────────────────────────
// Ce fichier orchestre les modules : il branche les events et lance l'init.
// Toute la logique métier est dans les fichiers components/ et pages/.

// ─── Helpers UI ──────────────────────────────────────────────────────────────

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

// ─── Fetch & Render ──────────────────────────────────────────────────────────

function initializeFromURL() {
  const params = new URLSearchParams(window.location.search);
  const ingredient = params.get('ingredient');
  const category = params.get('category');
  const searchType = document.getElementById('searchType');

  if (ingredient) {
    state.tagsearch = ingredient;
    state.tagsearchType = 'ingredient';
    state.searchMode = 'ingredient';
    document.getElementById('searchName').value = ingredient;
    if (searchType) searchType.value = 'ingredient';
    return true;
  } else if (category) {
    state.tagsearch = category;
    state.tagsearchType = 'category';
    state.searchMode = 'category';
    document.getElementById('searchName').value = category;
    if (searchType) searchType.value = 'category';
    return true;
  }

  state.searchMode = 'product';
  if (searchType) searchType.value = 'product';
  return false;
}

async function fetchAndRender() {
  const name  = document.getElementById('searchName').value.trim();
  const brand = document.getElementById('searchBrand').value.trim();
  const searchType = document.getElementById('searchType');
  const searchMode = searchType ? searchType.value : 'product';

  state.searchMode = searchMode;
  state.tagsearchType = searchMode === 'product' ? null : searchMode;

  showLoading();
  hideError();

  try {
    const params = new URLSearchParams();
    if (name) {
      if (searchMode === 'ingredient') {
        params.append('ingredient', name);
      } else if (searchMode === 'category') {
        params.append('category', name);
      } else {
        params.append('q', name);
      }
    }
    if (brand) params.append('brand', brand);

    state.allProducts = await fetchProducts(params);
    applyFilters();
  } catch (err) {
    showError("Erreur API : " + err.message + "<br>Vérifiez que l'API FastAPI est démarrée.");
  }
}

// ─── Init ────────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  const hasUrlParams = initializeFromURL();
  const searchType = document.getElementById('searchType');

  // Filtres Nutri-Score
  initNutriScoreFilters();

  if (searchType) {
    searchType.addEventListener('change', () => {
      state.searchMode = searchType.value;
      state.tagsearchType = searchType.value === 'product' ? null : searchType.value;
    });
  }

  // Recherche au Enter
  document.addEventListener('keypress', e => {
    if (e.key === 'Enter') fetchAndRender();
  });

  // Recherche par marque avec debounce
  document.getElementById('searchBrand').addEventListener('input', debounce(fetchAndRender, 500));

  // Si on arrive avec un paramètre URL, lancer la recherche
  if (hasUrlParams) {
    fetchAndRender();
  }
});
