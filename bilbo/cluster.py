"""클러스터 모듈."""
import os
from os.path import expanduser
import json
import datetime
import warnings
import time
import webbrowser

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
    print("    AMI: {}".format(pobj.scd_inst.ami))
    print("    Instance Type: {}".format(pobj.scd_inst.ec2type))
    print("    Security Group: {}".format(pobj.scd_inst.secgroup))
    print("    Key Name: {}".format(pobj.scd_inst.keyname))

    print("")
    print("  Worker:")
    print("    AMI: {}".format(pobj.wrk_inst.ami))
    print("    Instance Type: {}".format(pobj.wrk_inst.ec2type))
    print("    Security Group: {}".format(pobj.wrk_inst.secgroup))
    print("    Key Name: {}".format(pobj.wrk_inst.keyname))
    print("    Count: {}".format(pobj.wrk_cnt))

    print("")


def cluster_info_exists(clname):
    """클러스터 정보가 존재하는가?"""
    path = os.path.join(clust_dir, clname + '.json')
    return os.path.isfile(path)


def _build_tag_spec(name, _tags):
    tags = [{'Key': 'Name', 'Value': name}]
    for _tag in _tags:
        tag = dict(Key=_tag[0], Value=_tag[1])
        tags.append(tag)

    tag_spec = [
        {
            'ResourceType': 'instance',
            'Tags': tags
        }
    ]
    return tag_spec


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
    scd_tag_spec = _build_tag_spec(scd_name, pobj.scd_inst.tags)
    ins = ec2.create_instances(ImageId=pobj.scd_inst.ami,
                               InstanceType=pobj.scd_inst.ec2type,
                               MinCount=pobj.scd_cnt, MaxCount=pobj.scd_cnt,
                               KeyName=pobj.scd_inst.keyname,
                               SecurityGroupIds=[pobj.scd_inst.secgroup],
                               TagSpecifications=scd_tag_spec,
                               DryRun=dry)

    scd = ins[0]
    clinfo['instances'].append(scd.instance_id)
    clinfo['launch_time'] = datetime.datetime.now()

    # create worker
    wrk_name = '{}-dask-worker'.format(clname)
    wrk_tag_spec = _build_tag_spec(wrk_name, pobj.wrk_inst.tags)
    ins = ec2.create_instances(ImageId=pobj.wrk_inst.ami,
                               InstanceType=pobj.wrk_inst.ec2type,
                               MinCount=pobj.wrk_cnt, MaxCount=pobj.wrk_cnt,
                               KeyName=pobj.wrk_inst.keyname,
                               SecurityGroupIds=[pobj.wrk_inst.secgroup],
                               TagSpecifications=wrk_tag_spec,
                               DryRun=dry)

    clinfo['workers'] = []
    for wrk in ins:
        clinfo['instances'].append(wrk.instance_id)

    def get_inst_info(cobj, inst):
        info = {}
        info['image_id'] = inst.image_id
        info['instance_id'] = inst.instance_id
        info['public_ip'] = inst.public_ip_address
        info['private_dns_name'] = inst.private_dns_name
        info['key_name'] = inst.key_name
        info['cpu_options'] = inst.cpu_options
        info['ssh_user'] = cobj.ssh_user
        info['ssh_private_key'] = cobj.ssh_private_key
        return info

    # 사용 가능 상태까지 기다린 후 정보 얻기.
    scd.wait_until_running()
    scd.load()
    clinfo['scheduler'] = get_inst_info(pobj.scd_inst, scd)

    for wrk in ins:
        wrk.wait_until_running()
        wrk.load()
        winfo = get_inst_info(pobj.wrk_inst, wrk)
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
    check_cluster(clname)

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
    inst_idx = 0
    print("")
    print("Scheduler:")
    scd = info['scheduler']
    print("  [{}] instance_id: {}, public_ip: {}".
          format(inst_idx, scd['instance_id'], scd['public_ip']))
    inst_idx += 1

    print("")
    print("Workers:")
    wrks = info['workers']
    for wrk in wrks:
        print("  [{}] instance_id: {}, public_ip: {}".
              format(inst_idx, wrk['instance_id'], wrk['public_ip']))
        inst_idx += 1
    print("")


def destroy_cluster(clname, dry):
    """클러스터 제거."""
    check_cluster(clname)

    critical("Destroy cluster '{}'.".format(clname))
    info = load_cluster_info(clname)

    ec2 = boto3.client('ec2')
    ec2.terminate_instances(InstanceIds=info['instances'], DryRun=dry)

    path = os.path.join(clust_dir, clname + '.json')
    os.unlink(path)


def send_instance_cmd(ssh_user, ssh_private_key, public_ip, cmd,
                      show_error=True):
    """인스턴스에 SSH 명령어 실행

    https://stackoverflow.com/questions/42645196/how-to-ssh-and-run-commands-in-ec2-using-boto3

    Args:
        ssh_user (str): SSH 유저
        ssh_private_key (str): SSH Private Key 경로
        public_ip (str): 대상 인스턴스의 IP
        cmd (list): 커맨드 문자열 리스트

    Returns:
        tuple: send_command 함수의 결과 (stdout, stderr)
    """
    warnings.filterwarnings("ignore")

    key_path = expanduser(ssh_private_key)

    key = paramiko.RSAKey.from_private_key_file(key_path)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    client.connect(hostname=public_ip, username=ssh_user, pkey=key)

    stdin, stdout, stderr = client.exec_command(cmd)
    stdout = stdout.read()
    stderr = stderr.read()
    if len(stderr) > 0:
        print(stderr.decode('utf-8'))

    client.close()

    return stdout, stderr


def find_cluster_instance_by_public_ip(cluster, public_ip):
    """Public IP로 클러스터 인스턴스 정보 찾기."""
    clpath = check_cluster(cluster)

    with open(clpath, 'rt') as f:
        body = f.read()
        data = json.loads(body)

    if data['type'] == 'dask':
        scd = data['scheduler']
        if scd['public_ip'] == public_ip:
            return scd
        wrks = data['workers']
        for wrk in wrks:
            if wrk['public_ip'] == public_ip:
                return wrk
    else:
        raise NotImplementedError()


def start_cluster(clname):
    """클러스터 마스터/워커를 시작."""
    check_cluster(clname)

    clpath = check_cluster(clname)

    with open(clpath, 'rt') as f:
        body = f.read()
        data = json.loads(body)

    if data['type'] == 'dask':
        critical("Start dask scheduler & workers.")
        # 스케쥴러 시작
        scd = data['scheduler']
        user, private_key = scd['ssh_user'], scd['ssh_private_key']
        public_ip = scd['public_ip']
        scd_dns = scd['private_dns_name']
        cmd = "screen -S 'bilbo' -d -m dask-scheduler"
        send_instance_cmd(user, private_key, public_ip, cmd)
        time.sleep(3)

        for wrk in data['workers']:
            # 워커 재시작
            user, private_key = wrk['ssh_user'], wrk['ssh_private_key']
            public_ip = wrk['public_ip']
            cmd = "screen -S 'bilbo' -d -m dask-worker {}:8786".format(scd_dns)
            send_instance_cmd(user, private_key, public_ip, cmd)
    else:
        raise NotImplementedError()


def stop_cluster(clname):
    """클러스터 마스터/워커를 중지."""
    clpath = check_cluster(clname)

    with open(clpath, 'rt') as f:
        body = f.read()
        data = json.loads(body)

    if data['type'] == 'dask':
        critical("Stop dask scheduler & workers.")
        # 스케쥴러 중지
        scd = data['scheduler']
        user, private_key = scd['ssh_user'], scd['ssh_private_key']
        public_ip = scd['public_ip']
        cmd = "screen -X -S 'bilbo' quit"
        send_instance_cmd(user, private_key, public_ip, cmd)

        for wrk in data['workers']:
            # 워커 중지
            user, private_key = wrk['ssh_user'], wrk['ssh_private_key']
            public_ip = wrk['public_ip']
            cmd = "screen -X -S 'bilbo' quit"
            send_instance_cmd(user, private_key, public_ip, cmd)
    else:
        raise NotImplementedError()


def open_dashboard(clname):
    """클러스터의 대쉬보드 열기."""
    clpath = check_cluster(clname)

    with open(clpath, 'rt') as f:
        body = f.read()
        data = json.loads(body)

    if data['type'] == 'dask':
        # 스케쥴러 중지
        scd = data['scheduler']
        public_ip = scd['public_ip']
        url = "http://{}:8787".format(public_ip)
        webbrowser.open(url)
    else:
        raise NotImplementedError()
