"""명령행 인터페이스."""
import os
import json

import click
import botocore

from bilbo.version import VERSION
from bilbo.util import set_log_verbosity, iter_profiles, clust_dir
from bilbo.cluster import create_cluster, show_plan, show_cluster, \
    destroy_cluster, show_clusters


@click.group()
@click.option('-v', '--verbose', count=True, help="Increase message verbosity.")
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    set_log_verbosity(verbose)


@main.command(help="Create cluster.")
@click.argument('PROFILE')
@click.option('-n', '--name', help="Cluster name (Default: profile name)")
@click.option('--dry', is_flag=True, help="Dry run for test")
def create(profile, name, dry):
    """클러스터 생성."""
    try:
        create_cluster(profile, name, dry)
    except botocore.exceptions.ClientError as e:
        if "Request would have succeeded" in str(e):
            print("Create cluster succeded in dry mode.")
        else:
            raise(e)


@main.command(help="Show create cluster plan.")
@click.argument('PROFILE')
@click.option('-n', '--name', help="Cluster name")
def plan(profile, name):
    """클러스터 생성 계획 표시."""
    show_plan(profile, name)


@main.group(help="List things [..]")
def list():
    pass


@list.command('clusters', help="List active clusters.")
def list_clusters():
    """모든 클러스터를 리스트."""
    show_clusters()


@list.command('profiles', help='List profiles.')
def list_profiles():
    """모든 프로파일을 리스트."""
    for prof in iter_profiles():
        print(prof)


@list.command('instances', help="List cluster instances.")
@click.argument('CLUSTER')
def list_instances(cluster):
    """클러스터 내 인스턴스를 리스트."""
    print("List cluster instances")


@main.command(help="Destroy cluster.")
@click.argument('CLUSTER')
@click.option('--dry', is_flag=True, help="Dry run for test")
def destroy(cluster, dry):
    """클러스터 파괴."""
    destroy_cluster(cluster, dry)


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


@desc.command('cluster', help='Describe cluster.')
@click.argument('CLUSTER')
def desc_cluster(cluster):
    """프로파일을 설명."""
    assert not cluster.lower().endswith('.json')
    show_cluster(cluster)


@main.command(help='Show bilbo version.')
def version():
    """버전을 출력."""
    print(VERSION)


if __name__ == '__main__':
    main()