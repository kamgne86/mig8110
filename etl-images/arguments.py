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

table_name = click.option(
    '--table_name',
    type=str,
    help='MotherDuck table name to load data into'
)
