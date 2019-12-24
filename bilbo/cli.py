"""명령행 인터페이스."""
import json

import click

from bilbo.version import VERSION
from bilbo.util import (set_log_verbosity, iter_profiles)


@click.group()
@click.option('-v', '--verbose', count=True, help="Increase message verbosity.")
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    set_log_verbosity(verbose)


@main.command(help="Create cluster.")
@click.argument('PROFILE')
@click.option('--dry', help="Dryp run for test")
def create(profile, dry):
    """클러스터 생성."""
    from bilbo.cluster import create_cluster
    create_cluster(profile, dry)


@main.group(help="List things [..]")
def list():
    pass


@list.command('clusters', help="List active clusters.")
def list_clusters():
    """모든 클러스터를 리스트."""
    print("List clusters")


@list.command('profiles', help='List profiles.')
def list_profiles():
    """모든 프로파일을 리스트."""
    for prof in iter_profiles():
        print(prof)


@list.command('instances', help="List cluster instances.")
@click.option('-c', '--cluster-id', help='Cluster ID')
def list_instances(cluster_id):
    """클러스터 내 인스턴스를 리스트."""
    print("List cluster instances")


@main.command(help="Destroy cluster.")
@click.option('-c', '--cluster-id', help='Cluster ID')
def destroy(ctx, cluster_id):
    """클러스터 파괴."""
    print("Destroy cluster {}".format(cluster_id))


@main.group(help="Describe things [..]")
def desc():
    pass


@desc.command('profile', help='Describe profile.')
@click.argument('PROFILE')
def desc_profile(profile):
    """프로파일을 설명."""
    from bilbo.profile import read_profile
    pro = read_profile(profile)
    print(json.dumps(pro, indent=4, sort_keys=True))


@main.command(help='Show bilbo version.')
def version():
    """버전을 출력."""
    print(VERSION)


if __name__ == '__main__':
    main()