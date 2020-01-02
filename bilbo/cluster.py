"""클러스터 모듈."""
import os
from os.path import expanduser
import json
import datetime
import warnings

import boto3
import paramiko

from bilbo.profile import read_profile, DaskProfile
from bilbo.util import critical, warning, error, clust_dir, iter_clusters


def show_plan(profile, clname):
    pcfg = read_profile(profile)
    if clname is None:
        clname = profile.lower().split('.')[0]
    ccfg = pcfg['cluster']
    cltype = ccfg['type']

    print("Bilbo will create '{}' cluster with following options:".
          format(clname))
    if cltype == 'dask':
        pobj = DaskProfile(pcfg)
        return show_dask_plan(clname, pobj)
    else:
        raise NotImplementedError(cltype)


def show_dask_plan(clname, pobj):
    """클러스터 생성 계획 표시."""
    print("")
    print("  Cluster Type: Dask")

    print("")
    print("  Scheduler:")
    print("    AMI: {}".format(pobj.scd_node.ami))
    print("    Instance Type: {}".format(pobj.scd_node.instype))
    print("    Security Group: {}".format(pobj.scd_node.secgroup))
    print("    Key Name: {}".format(pobj.scd_node.keyname))

    print("")
    print("  Worker:")
    print("    AMI: {}".format(pobj.wrk_node.ami))
    print("    Instance Type: {}".format(pobj.wrk_node.instype))
    print("    Security Group: {}".format(pobj.wrk_node.secgroup))
    print("    Key Name: {}".format(pobj.wrk_node.keyname))
    print("    Count: {}".format(pobj.wrk_cnt))

    print("")


def cluster_info_exists(clname):
    """클러스터 정보가 존재하는가?"""
    path = os.path.join(clust_dir, clname + '.json')
    return os.path.isfile(path)


def create_dask_cluster(clname, pcfg, ec2, dry):
    """Dask 클러스터 생성.

    Args:
        clname (str): 클러스터 이름. 이미 존재하면 에러
        pcfg (dict): 프로파일 설정 정보
        ec2 (botocore.client.EC2): boto EC2 client
        dry: (bool): Dry run 여부
    """
    critical("Create dask cluster '{}'.".format(clname))
    warning("===========================")
    pretty = json.dumps(pcfg, indent=4, sort_keys=True)
    warning(pretty)
    warning("===========================")

    # 기존 클러스터가 있으면 에러
    if cluster_info_exists(clname):
        error("Cluster '{}' already exists.".format(clname))
        return

    pobj = DaskProfile(pcfg)
    clinfo = {'name': clname, 'type': 'dask', 'instances': []}

    # create scheduler
    scd_name = '{}-dask-scheduler'.format(clname)
    ins = ec2.create_instances(ImageId=pobj.scd_node.ami,
                               InstanceType=pobj.scd_node.instype,
                               MinCount=pobj.scd_cnt, MaxCount=pobj.scd_cnt,
                               KeyName=pobj.scd_node.keyname,
                               SecurityGroupIds=[pobj.scd_node.secgroup],
                               TagSpecifications=[
                                   {
                                       'ResourceType': 'instance',
                                       'Tags': [
                                           {'Key': 'Name', 'Value': scd_name}
                                       ]
                                   }
                               ],
                               DryRun=dry)

    scd = ins[0]
    clinfo['instances'].append(scd.instance_id)
    clinfo['launch_time'] = datetime.datetime.now()

    # create worker
    wrk_name = '{}-dask-worker'.format(clname)
    ins = ec2.create_instances(ImageId=pobj.wrk_node.ami,
                               InstanceType=pobj.wrk_node.instype,
                               MinCount=pobj.wrk_cnt, MaxCount=pobj.wrk_cnt,
                               KeyName=pobj.wrk_node.keyname,
                               SecurityGroupIds=[pobj.wrk_node.secgroup],
                               TagSpecifications=[
                                   {
                                       'ResourceType': 'instance',
                                       'Tags': [
                                           {'Key': 'Name', 'Value': wrk_name}
                                       ]
                                   }
                               ],
                               DryRun=dry)

    clinfo['workers'] = []
    for wrk in ins:
        clinfo['instances'].append(wrk.instance_id)

    def get_node_info(ec2):
        info = {}
        info['image_id'] = ec2.image_id
        info['instance_id'] = ec2.instance_id
        info['public_ip'] = ec2.public_ip_address
        info['private_dns_name'] = ec2.private_dns_name
        info['key_name'] = ec2.key_name
        return info


    # 사용 가능 상태까지 기다린 후 정보 얻기.
    scd.wait_until_running()
    scd.load()
    clinfo['scheduler'] = get_node_info(scd)

    for wrk in ins:
        wrk.wait_until_running()
        wrk.load()
        winfo = get_node_info(wrk)
        clinfo['workers'].append(winfo)

    # Dask 클러스터를 위한 원격 명령어 실행
    # run_remote_commands(['touch /tmp/foo'], [scd.instance_id])

    # 성공. 클러스터 정보 저장
    clinfo['ready_time'] = datetime.datetime.now()
    save_cluster_info(clname, clinfo)


def save_cluster_info(clname, clinfo):
    """클러스터 정보파일 쓰기."""

    def json_default(value):
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        raise TypeError('not JSON serializable')

    warning("save_cluster_info: '{}'".format(clname))
    path = os.path.join(clust_dir, clname + '.json')
    with open(path, 'wt') as f:
        body = json.dumps(clinfo, default=json_default, indent=4,
                          sort_keys=True)
        f.write(body)


def load_cluster_info(clname):
    """클러스터 정보파일 읽기."""
    warning("load_cluster_info: '{}'".format(clname))
    path = os.path.join(clust_dir, clname + '.json')
    with open(path, 'rt') as f:
        body = f.read()
        clinfo = json.loads(body)
    return clinfo


def create_cluster(profile, clname, dry):
    """클러스터 생성."""
    pcfg = read_profile(profile)
    if clname is None:
        clname = profile.lower().split('.')[0]
    ec2 = boto3.resource('ec2')
    cltype = pcfg['cluster']['type']
    if cltype == 'dask':
        create_dask_cluster(clname, pcfg, ec2, dry)
        show_cluster(clname)
    else:
        raise NotImplementedError(cltype)


def show_all_cluster():
    """모든 클러스터를 표시."""
    for cl in iter_clusters():
        name = cl.split('.')[0]
        print("{}".format(name))


def check_cluster(clname):
    """프로파일을 확인.

    Args:
        clname (str): 클러스터명 (.json 확장자 제외)
    """
    if clname.lower().endswith('.json'):
        rname = clname.split('.')[0]
        error("Wrong cluster name '{}'. Use '{}' instead ".
              format(clname, rname))
        raise NameError(clname)

    # file existence
    path = os.path.join(clust_dir, clname + '.json')
    if not os.path.isfile(path):
        error("Cluster '{}' does not exist.".format(path))
        raise(FileNotFoundError(path))

    return path


def show_cluster(clname):
    """클러스터 정보를 표시."""
    info = load_cluster_info(clname)
    ctype = info['type']

    print("")
    print("Name: {}".format(info['name']))
    print("Type: {}".format(info['type']))
    print("Time: {}".format(info['ready_time']))

    if ctype == 'dask':
        show_dask_cluster(info)
    else:
        raise NotImplementedError()


def show_dask_cluster(info):
    print("")
    print("Scheduler:")
    scd = info['scheduler']
    print("  instance_id: {}, public_ip: {}".format(scd['instance_id'],
          scd['public_ip']))

    print("")
    print("Workers:")
    wrks = info['workers']
    for wi, wrk in enumerate(wrks):
        print("  [{}] instance_id: {}, public_ip: {}".
              format(wi + 1, wrk['instance_id'], wrk['public_ip']))
    print("")


def destroy_cluster(clname, dry):
    """클러스터 제거."""
    critical("Destroy cluster '{}'.".format(clname))
    info = load_cluster_info(clname)

    ec2 = boto3.client('ec2')
    ec2.terminate_instances(InstanceIds=info['instances'], DryRun=dry)

    path = os.path.join(clust_dir, clname + '.json')
    os.unlink(path)


def send_instance_cmd(profile, public_ip, cmd):
    """인스턴스에 SSH 명령어 실행

    https://stackoverflow.com/questions/42645196/how-to-ssh-and-run-commands-in-ec2-using-boto3

    Args:
        profile (str): 프로파일명
        public_ip (str): 대상 인스턴스의 IP
        cmd (list): 커맨드 문자열 리스트

    Returns:
        send_command 함수의 결과
    """
    warnings.filterwarnings("ignore")

    pcfg = read_profile(profile)
    ssh = pcfg['ssh']
    user = ssh['user']
    private_key = expanduser(ssh['private_key'])

    key = paramiko.RSAKey.from_private_key_file(private_key)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=public_ip, username=user, pkey=key)

    stdin, stdout, stderr = client.exec_command(cmd)
    result = stdout.read()
    if len(result) > 0:
        print(result)

    client.close()
