import click
from commands.load_data import handle as load_data
from commands.extract_data import handle as extract_data
from commands.extract_delta import handle as extract_delta
from arguments import command, url, output_file_key, input_file_key, table_name, schema_name, num_files


@click.command()
@command
@url
@output_file_key
@input_file_key
@table_name
@schema_name
@num_files
def main(command, output_file_key, url, input_file_key, table_name, schema_name, num_files):
    if command == "extract_data":
        extract_data(output_file_key, url)
    elif command == "load_data":
        load_data(input_file_key, table_name, schema_name)
    elif command == "extract_delta":
        extract_delta(output_file_key, url, num_files)

        
if __name__ == '__main__':
    main()
