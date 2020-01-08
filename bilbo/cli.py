"""명령행 인터페이스."""
import json

import click
import botocore

from bilbo.version import VERSION
from bilbo.util import set_log_verbosity, iter_profiles
from bilbo.cluster import create_cluster, show_cluster, \
    destroy_cluster, show_all_cluster, send_instance_cmd, \
    find_cluster_instance_by_public_ip, stop_cluster, start_cluster, \
    open_dashboard, save_cluster_info, open_notebook
from bilbo.profile import check_profile, show_plan


@click.group()
@click.option('-v', '--verbose', count=True, help="Increase message verbosity.")
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    set_log_verbosity(verbose)


@main.command(help="Create cluster.")
@click.argument('PROFILE')
@click.option('-n', '--name', help="Cluster name (Default: profile name)")
def create(profile, name):
    """클러스터 생성."""
    check_profile(profile)
    clinfo = create_cluster(profile, name)
    if name is None:
        name = clinfo['name']
    start_cluster(clinfo)
    save_cluster_info(name, clinfo)
    show_cluster(name)


@main.command(help="Show create cluster plan.")
@click.argument('PROFILE')
@click.option('-n', '--name', help="Cluster name")
def plan(profile, name):
    """클러스터 생성 계획 표시."""
    show_plan(profile, name)


@main.command(help="List active clusters.")
def clusters():
    """모든 클러스터를 리스팅."""
    show_all_cluster()


@main.command('profiles', help='List all profiles.')
def profiles():
    """모든 프로파일을 리스팅."""
    for prof in iter_profiles():
        print(prof)


@main.command(help="Destroy cluster.")
@click.argument('CLUSTER')
def destroy(cluster):
    """클러스터 파괴."""
    destroy_cluster(cluster)


@main.command(help="Describe a cluster.")
@click.argument('CLUSTER')
@click.option('-d', '--detail', is_flag=True, help="Show detailed information.")
def desc(cluster, detail):
    show_cluster(cluster, detail)


# @desc.command('profile', help='Describe profile.')
# @click.argument('PROFILE')
# def desc_profile(profile):
#     """프로파일을 설명."""
#     check_profile(profile)
#     from bilbo.profile import read_profile
#     pro = read_profile(profile)
#     print(json.dumps(pro, indent=4, sort_keys=True))


# @desc.command('cluster', help='Describe cluster.')
# @click.argument('CLUSTER')
# @click.option('--detail', is_flag=True, help="Show detailed information.")
# def desc_cluster(cluster, detail):
#     """프로파일을 설명."""
#     show_cluster(cluster, detail)


@main.command(help="Restart cluster.")
@click.argument('CLUSTER')
def restart(cluster):
    clinfo = stop_cluster(cluster)
    start_cluster(clinfo)


@main.command(help="Command to a cluster instance.")
@click.argument('CLUSTER')
@click.argument('PUBLIC_IP')
@click.argument('CMD')
def rcmd(cluster, public_ip, cmd):
    # 존재하는 클러스터에서 인스턴스 IP로 정보를 찾음
    info = find_cluster_instance_by_public_ip(cluster, public_ip)
    if info is None:
        print("Can not find instance by ip {} in '{}'".
              format(public_ip, cluster))
        return
    ssh_user = info['ssh_user']
    ssh_private_key = info['ssh_private_key']
    stdout, _ = send_instance_cmd(ssh_user, ssh_private_key, public_ip, cmd)

    if len(stdout) > 0:
        print(stdout.decode('utf-8'))


@main.command(help="Open dashboard.")
@click.argument('CLUSTER')
def dashboard(cluster):
    open_dashboard(cluster)


@main.command(help="Open notebook.")
@click.argument('CLUSTER')
def notebook(cluster):
    open_notebook(cluster)


@main.command(help='Show bilbo version.')
def version():
    """버전을 출력."""
    print(VERSION)



if __name__ == '__main__':
    main()
