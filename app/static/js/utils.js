/** Retourne le grade si valide (a-e), sinon null */
function validGrade(grade) {
  if (!grade) return null;
  const g = grade.toLowerCase();
  return ['a','b','c','d','e'].includes(g) ? g : null;
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
