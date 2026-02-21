import click
from full_load import handle as initial_load
from arguments import command, url, output_file_key


@click.command()
@command
@url
@output_file_key
def main(command, output_file_key, url):
    if command == "full_load":
        initial_load(output_file_key, url)

        
if __name__ == '__main__':
    main()
