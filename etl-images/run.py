import click
from commands.load_data import handle as load_data
from commands.extract_data import handle as extract_data
from arguments import command, url, output_file_key, input_file_key, table_name


@click.command()
@command
@url
@output_file_key
@input_file_key
@table_name
def main(command, output_file_key, url, input_file_key, table_name):
    if command == "extract_data":
        extract_data(output_file_key, url)
    elif command == "load_data":
        load_data(input_file_key, table_name)

        
if __name__ == '__main__':
    main()
