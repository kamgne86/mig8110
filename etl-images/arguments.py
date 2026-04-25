import click

command = click.option(
    '--command',
    required=True,
    type=click.Choice(['extract_data', 'filter_data', 'validate_data', 'validate_delta', 'transform_data', 'load_data', 'fetch_delta_index', 'extract_delta', 'filter_delta', 'load_delta', 'transform_delta', 'merge_data', 'normalize_categories', 'normalize_ingredients', 'finalize_products']),
    help='Command to execute'
)

url = click.option(
    '--url',
    type=str,
    help='URL to fetch data from'
)

input_file_key = click.option(
    '--input_file_key',
    type=str,
    help='Input file key in S3'
)

output_file_key = click.option(
    '--output_file_key',
    type=str,
    help='Output file key in S3'
)

table_name = click.option(
    '--table_name',
    type=str,
    help='MotherDuck table name to load data into'
)

schema_name = click.option(
    '--schema_name',
    type=str,
    help='DuckDB schema name'
)

filename = click.option(
    '--filename',
    type=str,
    default=None,
    help='Delta filename to process (e.g. openfoodfacts_products_xxx.json.gz)'
)

base_url = click.option(
    '--base_url',
    type=str,
    default=None,
    help='Base URL of the delta directory (e.g. https://static.openfoodfacts.org/data/delta/)'
)

invalid_file_key = click.option(
    '--invalid_file_key',
    type=str,
    help='Output file key in S3 for invalid records (f2)'
)

country = click.option(
    '--country',
    type=str,
    default='canada',
    help='Country to filter delta records on (substring match against countries_tags, default: canada)'
)

lang = click.option(
    '--lang',
    type=str,
    default=None,
    help='Language to filter records on (exact match against lang column, e.g. fr, en)'
)

columns = click.option(
    '--columns',
    type=str,
    default=None,
    help='Comma-separated list of columns to keep (e.g. code,product_name,brands)'
)

products_output_key = click.option(
    '--products_output_key',
    type=str,
    default=None,
    help='Output file key in S3 for the products parquet (without categories_tags)'
)

categories_output_key = click.option(
    '--categories_output_key',
    type=str,
    default=None,
    help='Output file key in S3 for the categories parquet'
)

product_categories_output_key = click.option(
    '--product_categories_output_key',
    type=str,
    default=None,
    help='Output file key in S3 for the product_categories junction parquet'
)

ingredients_output_key = click.option(
    '--ingredients_output_key',
    type=str,
    default=None,
    help='Output file key in S3 for the ingredients parquet'
)

product_ingredients_output_key = click.option(
    '--product_ingredients_output_key',
    type=str,
    default=None,
    help='Output file key in S3 for the product_ingredients junction parquet'
)

key_column = click.option(
    '--key_column',
    type=str,
    default='code',
    help='Column used as key for the DELETE + INSERT upsert in load_delta (default: code)'
)
