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


ROOT_PATH = Path(__file__).parent.parent.parent.parent.parent
ALEMBIC_INI = ROOT_PATH / 'alembic.ini'


def main(cmd: Command,
         revision: str | None = None,
         verbose: bool = True,
         ini_file: Path = ALEMBIC_INI):
    """
    Alembic database migration wrapper.

    :param cmd:
    :param revision: typically `head` for upgrade, specific version for downgrade
    :param verbose:
    :param ini_file: absolute path to the alembic.ini file
    :return:
    """

    print(f'Using alembic config at: {ini_file}')
    if not ini_file.is_file():
        raise FileNotFoundError('Config does not exist!')

    ALEMBIC_CFG = Config(ini_file,
                         config_args={
                             'script_location': str(ROOT_PATH / 'src/nacsos_data/scripts/migrations'),
                             'prepend_sys_path': str(ROOT_PATH)
                         })

    if cmd == Command.current:
        command.current(ALEMBIC_CFG, verbose=verbose)
    elif cmd == Command.upgrade:
        command.upgrade(ALEMBIC_CFG, revision)
    elif cmd == Command.downgrade:
        command.downgrade(ALEMBIC_CFG, revision)


def run():
    typer.run(main)


if __name__ == '__main__':
    run()