// ─── État global de l'application ────────────────────────────────────────────

const state = {
  allProducts: [],
  activeGrades: new Set(),
  searchMode: 'product',
  tagsearch: null,
  tagsearchType: null,
  radarChartInstance: null,
};

// ─── Définition des métriques de comparaison ────────────────────────────────

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

const comparisonMetrics = allComparisonMetrics.filter(m => m.default);

const nutriScoreOrder = { a: 5, b: 4, c: 3, d: 2, e: 1 };
