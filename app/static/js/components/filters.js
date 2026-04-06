// ─── Filtres côté client ─────────────────────────────────────────────────────

function applyFilters() {
  const maxSalt  = parseFloat(document.getElementById('saltSlider').value);
  const maxSugar = parseFloat(document.getElementById('sugarSlider').value);
  const maxCal   = parseFloat(document.getElementById('calSlider').value);
  const maxFat   = parseFloat(document.getElementById('fatSlider').value);

  const filtered = state.allProducts.filter(p => {
    if (state.activeGrades.size > 0) {
      const grade = validGrade(p.nutriscore_grade);
      if (!grade || !state.activeGrades.has(grade)) return false;
    }
    if (p.salt_100g        != null && p.salt_100g        > maxSalt)  return false;
    if (p.sugars_100g      != null && p.sugars_100g      > maxSugar) return false;
    if (p.energy_kcal_100g != null && p.energy_kcal_100g > maxCal)   return false;
    if (p.fat_100g         != null && p.fat_100g         > maxFat)   return false;
    return true;
  });

  renderProducts(filtered);
}

function resetFilters() {
  state.activeGrades.clear();
  document.querySelectorAll('#nutriFilters button').forEach(b => b.classList.remove('active'));

  document.getElementById('saltSlider').value  = 5;    updateSliderLabel('saltVal',  '5.00', 'g/100g');
  document.getElementById('sugarSlider').value = 100;  updateSliderLabel('sugarVal', 100,    'g/100g');
  document.getElementById('calSlider').value   = 1000; updateSliderLabel('calVal',   1000,   'kcal/100g');
  document.getElementById('fatSlider').value   = 100;  updateSliderLabel('fatVal',   100,    'g/100g');

  applyFilters();
}

function updateSliderLabel(id, value, unit) {
  document.getElementById(id).textContent = `${value} ${unit}`;
}

function initNutriScoreFilters() {
  document.querySelectorAll('#nutriFilters button').forEach(btn => {
    btn.addEventListener('click', () => {
      const grade = btn.dataset.grade;
      if (state.activeGrades.has(grade)) {
        state.activeGrades.delete(grade);
        btn.classList.remove('active');
      } else {
        state.activeGrades.add(grade);
        btn.classList.add('active');
      }
      applyFilters();
    });
  });
}
