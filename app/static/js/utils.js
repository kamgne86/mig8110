/** Retourne le grade si valide (a-e), sinon null */
function validGrade(grade) {
  if (!grade) return null;
  const g = grade.toLowerCase();
  return ['a','b','c','d','e'].includes(g) ? g : null;
}

/** Échappe une chaîne HTML pour éviter les injections XSS */
function escHtml(str) {
  if (!str) return '';
  const map = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  };
  return str.replace(/[&<>"']/g, m => map[m]);
}

/**
 * Parse un champ tags qui peut être :
 *   - un vrai tableau JS  : ["en:foo-bar", "fr:baz"]   (format API)
 *   - une string Python   : "['en:foo-bar', 'fr:baz']" (format CSV legacy)
 * Retourne un tableau de labels lisibles.
 */
function parseTags(raw, maxItems = null) {
  if (!raw) return [];

  let tags = [];
  if (Array.isArray(raw)) {
    tags = raw;
  } else if (typeof raw === 'string') {
    if (raw === '[]' || raw.trim() === '') return [];
    tags = [...raw.matchAll(/'([^']+)'/g)].map(m => m[1]);
  }

  let labels = tags.map(tag => tag.replace(/^[a-z]{2}:/, '').replace(/-/g, ' '));
  if (maxItems) labels = labels.slice(0, maxItems);
  return labels;
}

const INGREDIENT_TEXT_PRIORITY = [
  'ingredients_text',
  'ingredients_text_fr',
  'ingredients_text_en',
  'ingredients_text_de',
  'ingredients_text_es',
  'ingredients_text_it',
];

function getFallbackIngredientText(product) {
  if (!product || typeof product !== 'object') return null;

  for (const key of INGREDIENT_TEXT_PRIORITY) {
    const value = product[key];
    if (typeof value === 'string' && value.trim()) {
      return value;
    }
  }

  const extraKey = Object.keys(product)
    .filter(key => /^ingredients_text_[a-z]{2}$/.test(key))
    .sort()
    .find(key => {
      const value = product[key];
      return typeof value === 'string' && value.trim();
    });

  return extraKey ? product[extraKey] : null;
}

function splitIngredientString(value, maxItems = null) {
  if (!value || typeof value !== 'string') return [];
  const parts = value
    .split(/[\n,;]+/)
    .map(part => part.trim())
    .filter(Boolean);
  return maxItems ? parts.slice(0, maxItems) : parts;
}

function getIngredientsList(product, maxItems = null) {
  if (!product) return [];

  // Nouveau modèle : tableau de ingredient_name renvoyé par l'API
  if (Array.isArray(product.ingredients) && product.ingredients.length) {
    // Si déjà nettoyé (pas de préfixe), retourner directement
    const firstItem = product.ingredients[0] || '';
    const alreadyCleaned = !firstItem.match(/^[a-z]{2,3}:/);

    if (alreadyCleaned) {
      return maxItems ? product.ingredients.slice(0, maxItems) : product.ingredients;
    }

    // Sinon nettoyer (pour compatibilité)
    const labels = product.ingredients.map(tag => tag.replace(/^[a-z]{2,3}:/, '').replace(/-/g, ' '));
    return maxItems ? labels.slice(0, maxItems) : labels;
  }

  // Fallback legacy : ingredients_tags (ancien modèle)
  const tagsList = parseTags(product.ingredients_tags, maxItems);
  if (tagsList.length) return tagsList;
  const text = getFallbackIngredientText(product);
  if (!text) return [];
  return splitIngredientString(text, maxItems);
}
