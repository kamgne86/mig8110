import click

command = click.option(
    '--command',
    required=True,
    type=click.Choice(['extract_data', 'load_data']),
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

columns = click.option(
    '--columns',
    type=str,
    help='Columns to process'
)

table_name = click.option(
    '--table_name',
    type=str,
    help='PostgreSQL table name to load data into'
)

if_exists = click.option(
    '--if_exists',
    type=click.Choice(['replace', 'append']),
    default='append',
    help='How to behave if the table exists'
)
