import typer

from .academic_apis import app as academic_apis_app
from .importer import app as importer_app
from .migrations import main as migrate

app = typer.Typer()
app.add_typer(academic_apis_app, name='apis', help='Academic API wrappers to download and translate data')
app.add_typer(importer_app, help='Import data into the platform')

app.command('migrate', help='Run database migrations')(migrate)


def run():
    app()


if __name__ == '__main__':
    run()
