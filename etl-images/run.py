import logging
import click

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
from commands.load_data import handle as load_data
from commands.load_delta import handle as load_delta
from commands.extract_data import handle as extract_data
from commands.extract_delta import handle as extract_delta
from commands.filter_data import handle as filter_data
from commands.filter_delta import handle as filter_delta
from commands.validate_data import handle as validate_data
from commands.transform_data import handle as transform_data
from commands.transform_delta import handle as transform_delta
from commands.fetch_delta_index import handle as fetch_delta_index
from commands.merge_data import handle as merge_data
from arguments import command, url, output_file_key, input_file_key, table_name, schema_name, filename, base_url, invalid_file_key, country, columns


@click.command()
@command
@url
@output_file_key
@input_file_key
@invalid_file_key
@table_name
@schema_name
@filename
@base_url
@country
@columns
def main(command, output_file_key, url, input_file_key, invalid_file_key, table_name, schema_name, filename, base_url, country, columns):
    if command == "extract_data":
        extract_data(output_file_key, url)
    elif command == "filter_data":
        filter_data(input_file_key, output_file_key, columns)
    elif command == "validate_data":
        validate_data(input_file_key, output_file_key, invalid_file_key)
    elif command == "transform_data":
        transform_data(input_file_key, output_file_key)
    elif command == "load_data":
        load_data(input_file_key, table_name, schema_name)
    elif command == "extract_delta":
        extract_delta(filename, output_file_key, base_url, country)
    elif command == "filter_delta":
        filter_delta(input_file_key, output_file_key, columns)
    elif command == "load_delta":
        load_delta(input_file_key, table_name, schema_name)
    elif command == "transform_delta":
        transform_delta(input_file_key, output_file_key)
    elif command == "fetch_delta_index":
        fetch_delta_index(url)
    elif command == "merge_data":
        merge_data(input_file_key, table_name, schema_name)


if __name__ == '__main__':
    main()
