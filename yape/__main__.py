from . import (
    cli,
    ty,
)

def run() -> ty.NoReturn:
    exit(cli.run())

if __name__ == '__main__':
    run()
