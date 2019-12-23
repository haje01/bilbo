"""명령행 인터페이스."""

import click

from bilbo.version import VERSION
from bilbo.util import set_log_verbosity


@click.group()
@click.option('-v', '--verbose', count=True, help="Increase message verbosity.")
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    set_log_verbosity(verbose)


@main.command(help="Create cluster.")
@click.pass_context
def create(ctx):
    """클러스터 생성."""
    print("Create cluster")


@main.command(help="Destroy cluster.")
@click.pass_context
def destroy(ctx):
    """클러스터 파괴."""
    print("Destroy cluster")


@main.command(help='Show bilbo version.')
def version():
    """버전을 출력."""
    print(VERSION)
