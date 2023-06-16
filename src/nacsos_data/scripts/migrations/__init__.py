from typing import Optional

import typer
from enum import Enum
from pathlib import Path

from alembic.config import Config
from alembic import command


class Command(str, Enum):
    # Display current database revision
    current = 'current'
    # Upgrade to a later database revision
    upgrade = 'upgrade'
    # Revert to a previous database revision
    downgrade = 'downgrade'
    # Autogenerate/create a new revision
    revision = 'revision'


ROOT_PATH = Path(__file__).parent.parent.parent.parent.parent


def main(cmd: Command,
         revision: Optional[str] = None,
         verbose: bool = True,
         root_path: Path = ROOT_PATH,
         ini_file: Optional[Path] = None,
         autogenerate: bool = False,
         message: Optional[str] = None):
    """
    Alembic database migration wrapper.

    :param cmd:
    :param revision: typically `head` for upgrade, specific version for downgrade
    :param verbose:
    :param root_path: absolute path to the package base
    :param ini_file: absolute path to the alembic.ini file (defaults to 'alembic.ini' in ROOT_PATH)
    :param message: used as `-m` in `alembic revision --autogenerate -m "message"`
    :param autogenerate: used as `--autogenerate` in `alembic revision --autogenerate -m "message"`
    :return:
    """
    if ini_file is None:
        ini_file = root_path / 'alembic.ini'

    print(f'Using alembic config at: {ini_file}')
    if not ini_file.is_file():
        raise FileNotFoundError('Config does not exist!')

    ALEMBIC_CFG = Config(ini_file,
                         config_args={
                             'script_location': str(root_path / 'scripts/migrations'),
                             'prepend_sys_path': str(root_path)
                         })

    if cmd == Command.current:
        command.current(ALEMBIC_CFG, verbose=verbose)
    elif cmd == Command.upgrade:
        command.upgrade(ALEMBIC_CFG, revision)
    elif cmd == Command.downgrade:
        command.downgrade(ALEMBIC_CFG, revision)
    elif cmd == Command.revision:
        command.revision(ALEMBIC_CFG, autogenerate=autogenerate, message=message)


def run():
    typer.run(main)


if __name__ == '__main__':
    run()
