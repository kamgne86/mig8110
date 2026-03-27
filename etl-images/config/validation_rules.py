import pandas as pd

# Validation rules for canada_products (initial load)
# Each rule is a function that takes a DataFrame and returns a boolean Series.
# A record is valid only if ALL rules return True.
# To add a new rule: append a lambda to the list below.

VALIDATION_RULES = [
    lambda df: df['code'].notna(),
    lambda df: df['code'].astype(str).str.strip() != '',
    lambda df: df['product_name'].notna(),
    lambda df: df['nutriments'].notna() if 'nutriments' in df.columns else pd.Series(False, index=df.index),
]
