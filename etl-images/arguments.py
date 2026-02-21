import click

command = click.option(
    '--command',
    required=True,
    type=click.Choice(['full_load']),
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
