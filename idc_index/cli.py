import click
from .index import IDCClient

@click.group()
def main():
    pass

@main.command()
def get_collections():
    client = IDCClient()
    collections = client.get_collections()
    click.echo(collections)

if __name__ == "__main__":
    main()
