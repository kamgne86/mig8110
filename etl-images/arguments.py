import click

command = click.option(
    '--command',
    required=True,
    type=click.Choice(['extract_data', 'filter_data', 'validate_data', 'transform_data', 'load_data', 'extract_delta', 'load_delta', 'transform_delta', 'merge_data']),
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

num_files = click.option(
    '--num_files',
    type=int,
    default=None,
    help='Number of most recent delta files to process (default: all)'
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

last_processed_file = click.option(
    '--last_processed_file',
    type=str,
    default=None,
    help='Filename of the last successfully processed delta file (Airflow Variable). Only files after this one will be processed.'
)

columns = click.option(
    '--columns',
    type=str,
    default=None,
    help='Comma-separated list of columns to keep (e.g. code,product_name,brands)'
)

