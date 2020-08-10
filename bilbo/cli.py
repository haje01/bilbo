"""명령행 인터페이스."""
import json

import click
import botocore

from bilbo.version import VERSION
from bilbo.util import set_log_verbosity, iter_profiles
from bilbo.profile import check_profile
from bilbo.cluster import create_cluster, show_cluster, \
    destroy_cluster, show_all_cluster, send_instance_cmd, \
    find_cluster_instance_by_public_ip, stop_cluster, start_cluster, \
    open_dashboard, open_notebook, run_notebook_or_python, \
    stop_notebook_or_python, pause_cluster, resume_cluster, show_plan, \
    start_services, save_cluster_info


@click.group()
@click.option('-v', '--verbose', count=True, help="Increase message verbosity.")
@click.pass_context
def main(ctx, verbose):
    ctx.ensure_object(dict)
    set_log_verbosity(verbose)


def _after_create(clinfo, open_nb, open_db):
    # 인스턴스 정보만 저장
    save_cluster_info(clinfo)
    name = clinfo['name']
    remote_nb = start_services(clinfo)
    show_cluster(name)

    if open_nb:
        if remote_nb:
            open_notebook(name)
        else:
            print("There is no remote notebook in the cluster.")

    if open_db:
        open_dashboard(name, False)


@main.command(help="Create a cluster.")
@click.argument('PROFILE')
@click.option('-c', '--cluster', "name", help="Cluster name (Default: "
              "Profile name).")
@click.option('-p', '--param', multiple=True,
              help="Override profile by parameter.")
@click.option('-n', '--notebook', 'open_nb', is_flag=True, help="Open remote "
              "notebook when cluster is ready.")
@click.option('-d', '--dashboard', 'open_db', is_flag=True, help="Open remote "
              "dashboard when cluster is ready.")
def create(profile, name, param, open_nb, open_db):
    """클러스터 생성."""
    check_profile(profile)
    clinfo = create_cluster(profile, name, param)
    _after_create(clinfo, open_nb, open_db)


@main.command(help="Pause a cluster.")
@click.argument('CLUSTER')
def pause(cluster):
    """클러스터 정지."""
    pause_cluster(cluster)


@main.command(help="Resume a cluster.")
@click.argument('CLUSTER')
@click.option('-n', '--notebook', 'open_nb', is_flag=True, help="Open remote "
              "notebook when cluster is ready.")
@click.option('-d', '--dashboard', 'open_db', is_flag=True, help="Open remote "
              "dashboard when cluster is ready.")
def resume(cluster, open_nb, open_db):
    """클러스터 재개."""
    clinfo = resume_cluster(cluster)
    _after_create(clinfo, open_nb, open_db)


@main.command(help="Show cluster creation plan.")
@click.argument('PROFILE')
@click.option('-n', '--name', help="Cluster name.")
@click.option('-p', '--param', multiple=True,
              help="Override profile by parameter.")
def plan(profile, name, param):
    """클러스터 생성 계획 표시."""
    show_plan(profile, name, param)


@main.command(help="List active clusters.")
def ls():
    """모든 클러스터를 리스팅."""
    show_all_cluster()


@main.command('profiles', help='List all profiles.')
def profiles():
    """모든 프로파일을 리스팅."""
    for prof in iter_profiles():
        print(prof)


@main.command(help="Destroy a cluster.")
@click.argument('CLUSTER')
@click.option('-f', '--force', is_flag=True, help="Destroy without check.")
def destroy(cluster, force):
    """클러스터 파괴."""
    destroy_cluster(cluster, force)


@main.command(help="Describe a cluster.")
@click.argument('CLUSTER')
@click.option('-d', '--detail', is_flag=True,
              help="Show detailed information.")
def desc(cluster, detail):
    show_cluster(cluster, detail)


def _restart(cluster):
    clinfo = stop_cluster(cluster)
    start_cluster(clinfo)


@main.command(help="Restart a cluster service.")
@click.argument('CLUSTER')
def restart(cluster):
    _restart(cluster)


@main.command(help="Command to a cluster instance.")
@click.argument('CLUSTER')
@click.argument('PUBLIC_IP')
@click.argument('CMD')
@click.option('-n', '--no-interact', is_flag=True, help="Non-Interactive mode.")
def rcmd(cluster, public_ip, cmd, no_interact):
    # 존재하는 클러스터에서 인스턴스 IP로 정보를 찾음
    ret = find_cluster_instance_by_public_ip(cluster, public_ip)
    if ret is None:
        print("Can not find instance by ip {} in '{}'".
              format(public_ip, cluster))
        return
    inst, ssh_user, ssh_private_key = ret
    send_instance_cmd(ssh_user, ssh_private_key, public_ip, cmd, show_stdout=True, 
        no_interact=no_interact)


@main.command(help="Open dashboard.")
@click.argument('CLUSTER')
@click.option('-u', '--url-only', is_flag=True, help="Show URL only.")
def dashboard(cluster, url_only):
    open_dashboard(cluster, url_only)


@main.command(help="Open notebook.")
@click.argument('CLUSTER')
@click.option('-u', '--url-only', is_flag=True, help="Show URL only.")
def notebook(cluster, url_only):
    open_notebook(cluster, url_only)


@main.command(help="Run remote notebook or python file.")
@click.argument('CLUSTER')
@click.argument('FILE')
@click.option('-p', '--param', multiple=True,
              help="Parameter to run with")
@click.option('-r', '--restart', '_restart_after', is_flag=True,
              help="Restart cluster when after running.")
def run(cluster, file, param, _restart_after):
    try:
        run_notebook_or_python(cluster, file, param)
    except KeyboardInterrupt:
        print("Interrupt received, stopping...")
        stop_notebook_or_python(cluster, file, param)
    finally:
        if _restart_after:
            _restart(cluster)
        print("Finished.")


@main.command(help='Show bilbo version.')
def version():
    """버전을 출력."""
    print(VERSION)


if __name__ == '__main__':
    main()
