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

function getSearchControls() {
  return {
    searchType: document.getElementById('searchType'),
    searchName: document.getElementById('searchName'),
    searchCategory: document.getElementById('searchCategory'),
    searchBrand: document.getElementById('searchBrand'),
  };
}

function setCategorySelectValue(value) {
  const { searchCategory } = getSearchControls();
  if (!searchCategory) return;

  const normalizedValue = String(value || '').trim().toLowerCase();
  if (!normalizedValue) {
    searchCategory.value = '';
    return;
  }

  const match = Array.from(searchCategory.options).find(
    option =>
      String(option.value || '').trim().toLowerCase() === normalizedValue
      || String(option.dataset.label || '').trim().toLowerCase() === normalizedValue
      || String(option.textContent || '').trim().toLowerCase() === normalizedValue
  );
  searchCategory.value = match ? match.value : '';
}

function syncSearchModeUI() {
  const { searchType, searchName, searchCategory } = getSearchControls();
  const mode = searchType ? searchType.value : 'product';

  state.searchMode = mode;
  state.tagsearchType = mode === 'product' ? null : mode;

  if (!searchName || !searchCategory) return;

  searchCategory.style.display = 'none';
  searchCategory.disabled = true;
  searchName.style.display = '';
  searchName.disabled = false;
  searchName.placeholder =
    mode === 'category'
      ? 'Rechercher par categorie ...'
      : mode === 'ingredient'
      ? 'Rechercher par ingredient ...'
      : 'Rechercher par nom produit ...';
}

async function loadCategoryOptions() {
  if (state.categoriesLoaded) {
    syncSearchModeUI();
    return;
  }

  const { searchCategory, searchName } = getSearchControls();
  if (!searchCategory) return;

  try {
    const categories = await fetchCategories();
    state.categories = Array.isArray(categories) ? categories : [];
    state.categoriesLoaded = true;

    searchCategory.innerHTML = '';
    const placeholderOption = document.createElement('option');
    placeholderOption.value = '';
    placeholderOption.textContent = 'Choisir une categorie...';
    searchCategory.appendChild(placeholderOption);

    for (const category of state.categories) {
      const label = String(category.label || category.category_name || '').trim();
      const displayLabel = String(category.display_label || label).trim();
      const rawValue = String(category.category_name || label).trim();
      if (!label || !rawValue) continue;
      const option = document.createElement('option');
      option.value = rawValue;
      option.dataset.label = label;
      option.textContent = displayLabel;
      searchCategory.appendChild(option);
    }

    if (state.searchMode === 'category') {
      setCategorySelectValue(state.tagsearch || searchName.value);
    }
  } catch (err) {
    console.error('Impossible de charger les categories', err);
  } finally {
    syncSearchModeUI();
  }
}

function initializeFromURL() {
  const params = new URLSearchParams(window.location.search);
  const ingredient = params.get('ingredient');
  const category = params.get('category');
  const { searchType, searchName } = getSearchControls();

  if (ingredient) {
    state.tagsearch = ingredient;
    state.tagsearchType = 'ingredient';
    state.searchMode = 'ingredient';
    searchName.value = ingredient;
    if (searchType) searchType.value = 'ingredient';
    return true;
  }

  if (category) {
    state.tagsearch = category;
    state.tagsearchType = 'category';
    state.searchMode = 'category';
    searchName.value = category;
    if (searchType) searchType.value = 'category';
    return true;
  }

  state.searchMode = 'product';
  if (searchType) searchType.value = 'product';
  return false;
}

function getActiveSearchValue() {
  const { searchName } = getSearchControls();
  return searchName.value.trim();
}

async function fetchAndRender() {
  const { searchType, searchBrand } = getSearchControls();
  const name = getActiveSearchValue();
  const brand = searchBrand.value.trim();
  const searchMode = searchType ? searchType.value : 'product';

  state.searchMode = searchMode;
  state.tagsearch = name || null;
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
    showError('Erreur API : ' + err.message + '<br>Verifiez que l API FastAPI est demarree.');
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  const hasUrlParams = initializeFromURL();
  const { searchType, searchCategory, searchBrand } = getSearchControls();

  initNutriScoreFilters();
  syncSearchModeUI();
  await loadCategoryOptions();

  if (searchType) {
    searchType.addEventListener('change', async () => {
      syncSearchModeUI();
      if (searchType.value === 'category' && !state.categoriesLoaded) {
        await loadCategoryOptions();
      }
    });
  }

  if (searchCategory) {
    searchCategory.addEventListener('change', fetchAndRender);
  }

  document.addEventListener('keypress', event => {
    if (event.key === 'Enter') fetchAndRender();
  });

  searchBrand.addEventListener('input', debounce(fetchAndRender, 500));

  if (hasUrlParams) {
    fetchAndRender();
  }
});
