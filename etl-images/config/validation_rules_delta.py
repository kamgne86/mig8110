# Validation rules for canada_products (weekly delta load)
# Each rule is a function that takes a DataFrame and returns a boolean Series.
# A record is valid only if ALL rules return True.
# To add a new rule: append a lambda to the list below.

VALIDATION_RULES = [
    ("code notna",         lambda df: df['code'].notna()),
    ("code not empty",     lambda df: df['code'].astype(str).str.strip() != ''),
    ("product_name notna", lambda df: df['product_name'].notna()),
    ("nutrition notna",    lambda df: df['nutrition'].notna()),
    # ("ingredients notna",    lambda df: df['ingredients'].notna()),
    ("categories_tags notna", lambda df: df['categories_tags'].notna()),
]
