"""클러스터 모듈."""
import os
from os.path import expanduser
import re
import json
import datetime
import warnings
import time
import webbrowser
import tempfile
from urllib.request import urlopen
from urllib.error import URLError

import botocore
import boto3
import paramiko

from bilbo.profile import read_profile
from bilbo.util import critical, warning, error, clust_dir, iter_clusters, \
    info, get_aws_config, PARAM_PTRN, pprint

warnings.filterwarnings("ignore")

NB_WORKDIR = "~/works"
TRY_SLEEP = 10


def cluster_info_exists(clname):
    """클러스터 정보가 존재하는가?"""
    path = os.path.join(clust_dir, clname + '.json')
    return os.path.isfile(path)


def _build_tag_spec(name, desc, _tags):
    tags = [{'Key': 'Name', 'Value': name}]
    if desc is not None:
        tags.append({'Key': 'Description', 'Value': desc})

    if _tags is not None:
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


def create_ec2_instances(ec2, tpl, cnt, tag_spec):
    """EC2 인스턴스 생성."""
    rdm = get_root_dm(ec2, tpl)

    try:
        ins = ec2.create_instances(ImageId=tpl['ami'],
                                   InstanceType=tpl['ec2type'],
                                   MinCount=cnt, MaxCount=cnt,
                                   KeyName=tpl['keyname'],
                                   BlockDeviceMappings=rdm,
                                   SecurityGroupIds=[tpl['security_group']],
                                   TagSpecifications=tag_spec)
        return ins
    except botocore.exceptions.ClientError as e:
        error("create_ec2_instances - {}".format(str(e)))
        if 'Request would have succeeded' not in str(e):
            raise e


def get_type_instance_info(pobj, only_inst=None):
    """인스턴스 종류별 공통 정보."""
    info = {}
    info['image_id'] = pobj.ami
    info['key_name'] = pobj.keyname
    info['ssh_user'] = pobj.ssh_user
    info['ssh_private_key'] = pobj.ssh_private_key
    info['ec2type'] = pobj.ec2type

    if only_inst is not None:
        info['instance_id'] = only_inst.instance_id
        info['public_ip'] = only_inst.public_ip_address
        info['private_ip'] = only_inst.private_ip_address
        info['private_dns_name'] = only_inst.private_dns_name
        if only_inst.tags is not None:
            info['tags'] = only_inst.tags
    return info


def get_inst_name(clname, role, prefix):
    if prefix is None:
        return '{}-{}'.format(clname, role)
    else:
        return '{}{}-{}'.format(prefix, clname, role)


def create_inst(ec2, tpl, role, clname, prefix):
    tpl = tpl[role]
    name = get_inst_name(clname, role, prefix)
    desc = tpl.get('description')
    tags = tpl.get('tags')
    tag_spec = _build_tag_spec(name, desc, tags)
    cnt = tpl['count'] if 'count' in tpl else 1
    ins = create_ec2_instances(ec2, tpl, cnt, tag_spec)
    return ins


def instance_info(inst):
    """실행 인스턴스 정보."""
    info = {}
    info['instance_id'] = inst.instance_id
    info['public_ip'] = inst.public_ip_address
    info['private_ip'] = inst.private_ip_address
    info['private_dns_name'] = inst.private_dns_name
    return info


def create_notebook(ec2, clinfo):
    """노트북 생성."""
    critical("Create notebook.")
    clname = clinfo['name']
    prefix = clinfo['profile'].get('instance_prefix')
    tpl = clinfo['template']
    nb = create_inst(ec2, tpl, 'notebook', clname, prefix)[0]

    info("Wait for notebook instance to be running.")
    nb.wait_until_running()
    nb.load()
    clinfo['instance']['notebook'] = instance_info(nb)


def create_dask_cluster(ec2, clinfo):
    """Dask 클러스터 생성.

    Args:
        ec2 (botocore.client.EC2): boto EC2 client
        clinfo (dict): 클러스터 정보
    """
    clname = clinfo['name']
    prefix = clinfo['profile'].get('instance_prefix')
    critical("Create dask cluster '{}'.".format(clname))
    clinfo['type'] = 'dask'

    # 스케쥴러/워커 생성
    tpl = clinfo['template']
    scd = create_inst(ec2, tpl, 'scheduler', clname, prefix)[0]
    wrks = create_inst(ec2, tpl, 'worker', clname, prefix)
    winsts = clinfo['instance']['workers'] = []

    # 사용 가능 상태까지 기다린 후 추가 정보 얻기.
    info("Wait for instance to be running.")
    scd.wait_until_running()
    scd.load()
    clinfo['instance']['scheduler'] = instance_info(scd)
    for wrk in wrks:
        wrk.wait_until_running()
        wrk.load()
        winsts.append(instance_info(wrk))

    # ec2 생성 후 반환값의 `ncpu_options` 가 잘못오고 있어 여기서 요청.
    if len(wrks) > 0:
        # 첫 번째 워커의 ip
        wip = _get_ip(winsts[0], clinfo['profile'].get('private_command'))
        tpl['worker']['cpu_info'] = get_cpu_info(tpl['worker'], wip)


def get_cpu_info(tpl, ip):
    """생성된 인스턴스에서 lscpu 명령으로 CPU 정보 얻기."""
    info("get_cpu_info")
    user = tpl['ssh_user']
    private_key = tpl['ssh_private_key']
    # Cores
    cmd = "lscpu | grep -e ^CPU\(s\): | awk '{print $2}'"
    res, _ = send_instance_cmd(user, private_key, ip, cmd)
    num_core = int(res[0])
    # Threads per core
    cmd = "lscpu | grep Thread | awk '{print $4}'"
    res, _ = send_instance_cmd(user, private_key, ip, cmd)
    threads_per_core = int(res[0])
    cpu_info = {'CoreCount': num_core, 'ThreadsPerCore': threads_per_core}
    return cpu_info


def save_cluster_info(clinfo):
    """클러스터 정보파일 쓰기."""
    clname = clinfo['name']

    def json_default(value):
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        raise TypeError('not JSON serializable')

    warning("save_cluster_info: '{}'".format(clname))
    clinfo['saved_time'] = str(datetime.datetime.now())

    path = os.path.join(clust_dir, clname + '.json')
    with open(path, 'wt') as f:
        body = json.dumps(clinfo, default=json_default, indent=4,
                          sort_keys=True, ensure_ascii=False)
        f.write(body)


def load_cluster_info(clname):
    """클러스터 정보파일 읽기."""
    warning("load_cluster_info: '{}'".format(clname))
    path = os.path.join(clust_dir, clname + '.json')
    with open(path, 'rt') as f:
        body = f.read()
        clinfo = json.loads(body)
    return clinfo


def wait_until_connect(url, retry_count=10):
    """URL 접속이 가능할 때까지 기다림."""
    info("wait_until_connect: {}".format(url))
    for i in range(retry_count):
        try:
            urlopen(url, timeout=5)
            return
        except URLError:
            info("Can not connect to dashboard. Wait for a while.")
            time.sleep(TRY_SLEEP)
    raise ConnectionError()


def get_root_dm(ec2, iinfo):
    """AMI 별 원하는 크기의 디바이스 매핑 얻기."""
    volsize = iinfo.get('vol_size')
    if volsize is None:
        return []
    imgs = list(ec2.images.filter(ImageIds=[iinfo['ami']]))
    if len(imgs) == 0:
        raise ValueError("AMI {} does not exist.".format(iinfo['ami']))
    rdev = imgs[0].root_device_name
    dm = [{"DeviceName": rdev, "Ebs": {"VolumeSize": volsize}}]
    info("get_root_dm: {}".format(dm))
    return dm


def validate_inst(role, inst):
    if len(inst) == 0:
        raise RuntimeError("No instance config available for '{}'.".
                           format(role))

    def _raise(vtype, role):
        raise RuntimeError("No '{}' value for '{}'.".format(vtype, role))

    if 'ami' not in inst:
        _raise('ami', role)
    if 'ec2type' not in inst:
        _raise('ec2type', role)
    if 'keyname' not in inst:
        _raise('keyname', role)
    if 'security_group' not in inst:
        _raise('security_group', role)
    if 'ssh_user' not in inst:
        _raise('ssh_user', role)
    if 'ssh_private_key' not in inst:
        _raise('ssh_private_key', role)


def create_cluster(profile, clname, params):
    """클러스터 생성."""
    if clname is None:
        clname = '.'.join(profile.lower().split('.')[0:-1])
    critical("Create cluster '{}'.".format(clname))

    check_dup_cluster(clname)

    pro = read_profile(profile, params)
    ec2 = boto3.resource('ec2')

    #
    # 클러스터 정보
    clinfo = init_clinfo(clname)
    clinfo['profile'] = pro
    clinfo = resolve_instances(clinfo)

    # 클러스터 인스턴스 생성
    if 'dask' in clinfo['profile']:
        create_dask_cluster(ec2, clinfo)

    # 노트북 생성
    if 'notebook' in pro:
        create_notebook(ec2, clinfo)

    return clinfo


def pause_instance(inst_ids):
    """인스턴스 정지."""
    warning("pause_instance: '{}'".format(inst_ids))
    ec2 = boto3.client('ec2')

    # 권한 확인
    try:
        ec2.stop_instances(InstanceIds=inst_ids, DryRun=True)
    except botocore.exceptions.ClientError as e:
        if 'DryRunOperation' not in str(e):
            error(str(e))
            raise

    # 정지
    try:
        response = ec2.stop_instances(InstanceIds=inst_ids, DryRun=False)
        info(response)
    except botocore.exceptions.ClientError as e:
        error(str(e))


def collect_cluster_instances(clinfo):
    """클러스터내 인스턴스들 모움."""
    inst_ids = []
    insts = clinfo['instance']
    if 'notebook' in insts:
        inst_ids.append(insts['notebook']['instance_id'])

    if 'type' in clinfo:
        cltype = clinfo['type']
        if cltype == 'dask':
            if 'scheduler' in insts:
                inst_ids.append(insts['scheduler']['instance_id'])
            wrks = insts['workers']
            for wrk in wrks:
                inst_ids.append(wrk['instance_id'])
        else:
            raise NotImplementedError()
    return inst_ids


def pause_cluster(clname):
    """클러스터 정지."""
    check_cluster(clname)
    info = load_cluster_info(clname)

    print()
    print("Pause Cluster: {}".format(info['name']))

    inst_ids = collect_cluster_instances(info)
    pause_instance(inst_ids)


def resume_instance(inst_ids, ec2):
    """인스턴스 재개."""
    warning("resume_instance: '{}'".format(inst_ids))

    # 권한 확인
    try:
        ec2.start_instances(InstanceIds=inst_ids, DryRun=True)
    except botocore.exceptions.ClientError as e:
        if 'DryRunOperation' not in str(e):
            error(str(e))
            raise

    # 재개
    while True:
        try:
            response = ec2.start_instances(InstanceIds=inst_ids, DryRun=False)
            info(response)
        except botocore.exceptions.ClientError as e:
            msg = str(e)
            if 'is not in a state' not in msg:
                error(msg)
                raise
            time.sleep(5)
        else:
            break


def _update_cluster_info(ec2, clname, inst_ids, clinfo):
    # 정보 갱신 대기
    while True:
        ready = True
        warning("Wait until available.")
        time.sleep(5)
        res = ec2.describe_instances(InstanceIds=inst_ids)
        for inst in res['Reservations'][0]['Instances']:
            if 'PublicIpAddress' not in inst:
                ready = False
                print(inst)
                break
        if ready:
            break

    # 바뀐 정보 갱신
    for reserv in res['Reservations']:
        inst = reserv['Instances'][0]
        insts = clinfo['instance']
        if 'notebook' in insts:
            nb = insts['notebook']
            if nb['instance_id'] == inst['InstanceId']:
                new_ip = inst['PublicIpAddress']
                nb['public_ip'] = new_ip
        if 'scheduler' in insts:
            scd = insts['scheduler']
            if scd['instance_id'] == inst['InstanceId']:
                scd['public_ip'] = inst['PublicIpAddress']
        if 'workers' in insts:
            wrks = insts['workers']
            for wrk in wrks:
                if wrk['instance_id'] == inst['InstanceId']:
                    wrk['public_ip'] = inst['PublicIpAddress']

    save_cluster_info(clinfo)
    return clinfo


def resume_cluster(clname):
    """클러스터 재개."""
    check_cluster(clname)
    ec2 = boto3.client('ec2')
    clinfo = load_cluster_info(clname)

    print()
    print("Resume Cluster: {}".format(clinfo['name']))

    inst_ids = collect_cluster_instances(clinfo)
    resume_instance(inst_ids, ec2)

    # 바뀐 정보 갱신
    return _update_cluster_info(ec2, clname, inst_ids, clinfo)


def show_all_cluster():
    """모든 클러스터를 표시."""
    for clname in iter_clusters():
        clinfo = load_cluster_info(clname)
        name = clinfo['name']
        desc = clinfo.get('description')
        if desc is not None:
            msg = '{} : {}'.format(name, desc)
        else:
            msg = name
        print(msg)


def check_cluster(clname):
    """프로파일을 확인.

    Args:
        clname (str): 클러스터명 (.json 확장자 제외)
    """
    if clname.lower().endswith('.json'):
        rname = '.'.join(clname.split('.')[0:-1])
        msg = "Wrong cluster name '{}'. Use '{}' instead.". \
              format(clname, rname)
        raise NameError(msg)

    # file existence
    path = os.path.join(clust_dir, clname + '.json')
    if not os.path.isfile(path):
        error("Cluster '{}' does not exist.".format(path))
        raise(FileNotFoundError(path))

    return path


def show_cluster(clname, detail=False):
    """클러스터 정보를 표시."""
    check_cluster(clname)
    if detail:
        clinfo = load_cluster_info(clname)
        pprint(clinfo)
        return

    clinfo = load_cluster_info(clname)

    print()
    print("Cluster Name: {}".format(clinfo['name']))
    print("Ready Time: {}".format(clinfo['saved_time']))

    insts = clinfo['instance']
    idx = 1
    if 'notebook' in insts:
        print()
        print("Notebook:")
        idx = show_instance(idx, insts['notebook'])
        print()

    if 'type' in clinfo:
        cltype = clinfo['type']
        print("Cluster Type: {}".format(cltype))
        if cltype == 'dask':
            show_dask_cluster(idx, clinfo)
        else:
            raise NotImplementedError()
    print()


def show_instance(idx, inst):
    print("  [{}] instance_id: {}, public_ip: {}, private_ip: {}".
          format(idx, inst['instance_id'], inst['public_ip'],
                 inst['private_ip']))
    return idx + 1


def show_dask_cluster(idx, clinfo):
    """Dask 클러스터 표시."""
    print()
    insts = clinfo['instance']
    print("Scheduler:")
    scd = insts['scheduler']
    idx = show_instance(idx, scd)
    print("       {}".format(_get_dask_scheduler_address(clinfo)))

    print()
    print("Workers:")
    wrks = insts['workers']
    for wrk in wrks:
        idx = show_instance(idx, wrk)


def check_git_modified(clinfo):
    """로컬 git 저장소 변경 여부.

    Commit 되지 않거나, Push 되지 않은 내용이 있으면 경고

    Returns:
        bool: 변경이 없거나, 유저가 확인한 경우 True

    """
    nip = _get_ip(
        clinfo['instance']['notebook'],
        clinfo['profile'].get('private_command')
    )
    user = clinfo['template']['notebook']['ssh_user']
    private_key = clinfo['template']['notebook']['ssh_private_key']
    git_dirs = clinfo['git_cloned_dir']

    uncmts = []
    unpushs = []
    uncmt_cnt = unpush_cnt = 0
    for git_dir in git_dirs:
        cmd = "cd {} && git status --porcelain | grep '^ M.*'".format(git_dir)
        _uncmts, _, = send_instance_cmd(user, private_key, nip, cmd)
        uncmts += [os.path.join(git_dir, u) for u in _uncmts]
        uncmt_cnt += len(_uncmts)

        cmd = "cd {} && git cherry -v".format(git_dir)
        _unpushs, _, = send_instance_cmd(user, private_key, nip, cmd)
        unpushs += [os.path.join(git_dir, u) for u in _unpushs]
        unpush_cnt += len(_unpushs)

    if uncmt_cnt > 0 or unpush_cnt > 0:
        print()
        print("There are {} uncommitted file(s) and {} unpushed commits(s)!".
              format(uncmt_cnt, unpush_cnt))

        if uncmt_cnt > 0:
            print()
            print("Uncommitted file(s)")
            print("-------------------")
            for f in uncmts:
                print(f.strip())

        if unpush_cnt > 0:
            print()
            print("Unpushed commit(s)")
            print("-------------------")
            for f in unpushs:
                print(f.strip())

        print()
        ans = ''
        while ans.lower() not in ('y', 'n'):
            ans = input("Are you sure to destroy this cluster? (y/n): ")
        return ans == 'y'

    return True


def destroy_cluster(clname, force):
    """클러스터 제거."""
    check_cluster(clname)
    clinfo = load_cluster_info(clname)

    if 'git_cloned_dir' in clinfo and not force:
        if not check_git_modified(clinfo):
            print("Canceled.")
            return

    critical("Destroy cluster '{}'.".format(clname))

    # 인스턴스 제거
    ec2 = boto3.client('ec2')
    inst_ids = []
    for k, v in clinfo['instance'].items():
        if k == 'workers':
            inst_ids += [w['instance_id'] for w in v]
        else:
            inst_ids.append(v['instance_id'])
    if len(inst_ids) > 0:
        ec2.terminate_instances(InstanceIds=inst_ids)

    # 클러스터 파일 제거
    path = os.path.join(clust_dir, clname + '.json')
    os.unlink(path)


def send_instance_cmd(ssh_user, ssh_private_key, ip, cmd,
                      show_stdout=False, show_stderr=True, retry_count=10):
    """인스턴스에 SSH 명령어 실행

    https://stackoverflow.com/questions/42645196/how-to-ssh-and-run-commands-in-ec2-using-boto3

    Args:
        ssh_user (str): SSH 유저
        ssh_private_key (str): SSH Private Key 경로
        ip (str): 대상 인스턴스의 IP
        cmd (list): 커맨드 문자열 리스트
        show_stdout (bool): 표준 출력 메시지 출력 여부
        show_stderr (bool): 에러 메시지 출력 여부
        retry_count (int): 재시도 횟수

    Returns:
        tuple: send_command 함수의 결과 (stdout, stderr)
    """
    info('send_instance_cmd - user: {}, key: {}, ip {}, cmd {}'
         .format(ssh_user, ssh_private_key, ip, cmd))

    key_path = expanduser(ssh_private_key)

    key = paramiko.RSAKey.from_private_key_file(key_path)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connected = False
    for i in range(retry_count):
        try:
            client.connect(hostname=ip, username=ssh_user, pkey=key)
        except (paramiko.ssh_exception.NoValidConnectionsError, TimeoutError):
            warning("Connection failed to '{}'. Retry after a while.".
                    format(ip))
            time.sleep(TRY_SLEEP)
        else:
            connected = True
            break

    if not connected:
        error("Connection failed to '{}'".format(ip))
        return

    stdin, stdout, stderr = client.exec_command(cmd, get_pty=show_stdout)
    if show_stdout:
        stdouts = []
        for line in iter(stdout.readline, ""):
            stdouts.append(line)
            print(line, end="")
    else:
        stdouts = stdout.readlines()
    err = stderr.read()
    if show_stderr and len(err) > 0:
        error(err.decode('utf-8'))

    client.close()

    return stdouts, err


def find_cluster_instance_by_public_ip(clname, public_ip):
    """Public IP로 클러스터 인스턴스 정보 찾기."""
    check_cluster(clname)
    clinfo = load_cluster_info(clname)

    def _res(inst, role):
        tpl = clinfo['template']
        ssh_user = tpl[role]['ssh_user']
        ssh_private_key = tpl[role]['ssh_private_key']
        return inst, ssh_user, ssh_private_key

    insts = clinfo['instance']
    if 'notebook' in insts:
        if insts['notebook']['public_ip'] == public_ip:
            return _res(insts['notebook'], 'notebook')

    if clinfo['type'] == 'dask':
        scd = insts['scheduler']
        if scd['public_ip'] == public_ip:
            return _res(scd, 'scheduler')
        wrks = insts['workers']
        for wrk in wrks:
            if wrk['public_ip'] == public_ip:
                return _res(wrk, 'worker')
    else:
        raise NotImplementedError()


def dask_worker_options(wtpl, memory):
    """Dask 클러스터 워커 인스턴스 정보에서 워커 옵션 구하기."""
    co = wtpl['cpu_info']
    nproc = wtpl.get('nproc', co['CoreCount'])
    nthread = wtpl.get('nthread', co['ThreadsPerCore'])
    return nproc, nthread, memory // nproc


def start_cluster(clinfo):
    """클러스터 마스터 & 워커를 시작."""
    assert 'type' in clinfo
    if clinfo['type'] == 'dask':
        start_dask_cluster(clinfo)
    else:
        raise NotImplementedError()


def git_clone_cmd(repo, user, passwd, workdir):
    """Git 클론 명령 구성"""
    warning("git clone: {}".format(repo))
    protocol, address = repo.split('://')
    url = "{}://{}:{}@{}".format(protocol, user, passwd, address)
    cmd = "cd {} && git clone {}".format(workdir, url)
    return cmd


def setup_aws_creds(user, private_key, ip):
    """AWS 크레덴셜 설치."""
    cmds = [
        'mkdir -p ~/.aws',
        'cd ~/.aws',
        'echo [default] > credentials',
        'echo [default] > config'
    ]

    ak, sk, dr = get_aws_config()
    cmd = 'echo "aws_access_key_id = {}" >> credentials'.format(ak)
    cmds.append(cmd)
    cmd = 'echo "aws_secret_access_key = {}" >> credentials'.format(sk)
    cmds.append(cmd)
    cmd = 'echo "region = {}" >> config'.format(dr)
    cmds.append(cmd)

    cmd = '; '.join(cmds)
    send_instance_cmd(user, private_key, ip, cmd)


def _get_dask_scheduler_address(clinfo):
    dns = clinfo['instance']['scheduler']['private_dns_name']
    return "DASK_SCHEDULER_ADDRESS=tcp://{}:8786".format(dns)


def _get_ip(inst, private_command):
    assert type(private_command) == bool or private_command is None
    return inst['private_ip'] if private_command else inst['public_ip']


def start_notebook(clinfo, retry_count=10):
    """노트북 시작.

    Args:
        clinfo (dict): 클러스터 생성 정보
        retry_count (int): 접속 URL 얻기 재시도 수. 기본 10

    Raises:
        TimeoutError: 재시도 수가 넘을 때

    """
    critical("Start notebook.")

    tpl = clinfo['template']['notebook']
    pro = clinfo['profile']
    user, private_key = tpl['ssh_user'], tpl['ssh_private_key']
    inst = clinfo['instance']['notebook']
    ip = _get_ip(inst, pro.get('private_command'))

    # AWS 크레덴셜 설치
    setup_aws_creds(user, private_key, ip)

    # 작업 폴더
    nb_workdir = tpl.get('workdir', NB_WORKDIR)
    cmd = "mkdir -p {}".format(nb_workdir)
    send_instance_cmd(user, private_key, ip, cmd)

    # git 설정이 있으면 설정
    if 'notebook' in pro and 'git' in pro['notebook']:
        nb_git = pro['notebook']['git']
        cloned_dir = setup_git(nb_git, user, private_key, ip, nb_workdir)
        clinfo['git_cloned_dir'] = cloned_dir

    # 클러스터 타입별 노트북 설정
    vars = ''
    if 'type' in clinfo:
        if clinfo['type'] == 'dask':
            # dask-labextension을 위한 대쉬보드 URL
            sip = clinfo['instance']['scheduler']['public_ip']
            cmd = "mkdir -p ~/.jupyter/lab/user-settings/dask-labextension; "
            cmd += 'echo \'{{ "defaultURL": "http://{}:8787" }}\' > ' \
                   '~/.jupyter/lab/user-settings/dask-labextension/' \
                   'plugin.jupyterlab-settings'.format(sip)
            send_instance_cmd(user, private_key, ip, cmd)
            # 스케쥴러 주소
            vars = _get_dask_scheduler_address(clinfo)
        else:
            raise NotImplementedError()

    # Jupyter 시작
    ncmd = "cd {} && {} jupyter lab --ip 0.0.0.0".format(nb_workdir, vars)
    cmd = "screen -S bilbo -d -m bash -c '{}'".format(ncmd)
    send_instance_cmd(user, private_key, ip, cmd)

    # 접속 URL 얻기
    cmd = "jupyter notebook list | awk '{print $1}'"
    for i in range(retry_count):
        stdouts, _ = send_instance_cmd(user, private_key, ip, cmd)
        # url을 얻었으면 기록
        if len(stdouts) > 1:
            url = stdouts[1].strip().replace('0.0.0.0', inst['public_ip'])
            clinfo['notebook_url'] = url
            return
        info("Can not fetch notebook list. Wait for a while.")
        time.sleep(TRY_SLEEP)
    raise TimeoutError("Can not get notebook url.")


def setup_git(nb_git, user, private_key, ip, nb_workdir):
    """Git 설정 및 클론."""
    guser = nb_git['user']
    email = nb_git['email']

    # config
    cmd = "git config --global user.name '{}'; ".format(guser)
    cmd += "git config --global user.email '{}'".format(email)
    send_instance_cmd(user, private_key, ip, cmd)

    # 클론 (작업 디렉토리에)
    repo = nb_git['repository']
    passwd = nb_git['password']
    repos = [repo] if type(repo) is str else repo
    cdirs = []
    for repo in repos:
        cmd = git_clone_cmd(repo, guser, passwd, nb_workdir)
        send_instance_cmd(user, private_key, ip, cmd, show_stderr=False)
        gcdir = repo.split('/')[-1].replace('.git', '')
        cdirs.append("{}/{}".format(nb_workdir, gcdir))
    return cdirs


def start_dask_cluster(clinfo):
    """Dask 클러스터 마스터/워커를 시작."""
    critical("Start dask scheduler & workers.")
    private_command = clinfo.get('private_command')

    # 스케쥴러 시작
    stpl = clinfo['template']['scheduler']
    user, private_key = stpl['ssh_user'], stpl['ssh_private_key']
    scd = clinfo['instance']['scheduler']
    sip = _get_ip(scd, private_command)
    scd_dns = scd['private_dns_name']
    cmd = "screen -S bilbo -d -m dask-scheduler"
    send_instance_cmd(user, private_key, sip, cmd)

    # AWS 크레덴셜 설치
    setup_aws_creds(user, private_key, sip)

    # 워커 실행 옵션 구하기
    wrks = clinfo['instance']['workers']
    wip = _get_ip(wrks[0], private_command)
    info("  Get worker memory from '{}'".format(wip))
    cmd = "free -b | grep 'Mem:' | awk '{print $2}'"
    stdouts, _ = send_instance_cmd(user, private_key, wip, cmd)
    memory = int(stdouts[0])
    wtpl = clinfo['template']['worker']
    nproc, nthread, memory = dask_worker_options(wtpl, memory)
    # 결정된 옵션 기록
    wtpl = clinfo['template']['worker']
    wtpl['nproc'] = nproc
    wtpl['nthread'] = nthread
    wtpl['memory'] = memory

    # 모든 워커들에 대해
    user, private_key = wtpl['ssh_user'], wtpl['ssh_private_key']
    for wrk in wrks:
        wip = _get_ip(wrk, private_command)
        # AWS 크레덴셜 설치
        setup_aws_creds(user, private_key, wip)

        # 워커 시작
        opts = "--nprocs {} --nthreads {} --memory-limit {}".\
            format(nproc, nthread, memory)
        cmd = "screen -S bilbo -d -m dask-worker {}:8786 {}".\
            format(scd_dns, opts)
        warning("  Worker options: {}".format(opts))
        send_instance_cmd(user, private_key, wip, cmd)

    # Dask 스케쥴러의 대쉬보드 기다림
    dash_url = 'http://{}:8787'.format(sip)
    clinfo['dask_dashboard_url'] = dash_url
    critical("Wait for Dask dashboard ready.")
    wait_until_connect(dash_url)


def stop_cluster(clname):
    """클러스터 마스터/워커를 중지.

    Returns:
        dict: 클러스터 정보(재시작 용)
    """
    check_cluster(clname)
    clinfo = load_cluster_info(clname)
    private_command = clinfo.get('private_command')

    if clinfo['type'] == 'dask':
        critical("Stop dask scheduler & workers.")
        # 스케쥴러 중지
        stpl = clinfo['template']['scheduler']
        scd = clinfo['instance']['scheduler']
        user, private_key = stpl['ssh_user'], stpl['ssh_private_key']
        sip = _get_ip(scd, private_command)
        cmd = "screen -X -S 'bilbo' quit"
        send_instance_cmd(user, private_key, sip, cmd)

        wtpl = clinfo['template']['worker']
        user, private_key = wtpl['ssh_user'], wtpl['ssh_private_key']
        wrks = clinfo['instance']['workers']
        for wrk in wrks:
            # 워커 중지
            wip = _get_ip(wrk, private_command)
            cmd = "screen -X -S 'bilbo' quit"
            send_instance_cmd(user, private_key, wip, cmd)
    else:
        raise NotImplementedError()

    return clinfo


def open_url(url, cldata):
    """지정된 또는 기본 브라우저로 URL 열기."""
    info("open_url")
    wb = webbrowser
    if 'webbrowser' in cldata:
        path = cldata['webbrowser']
        info("  Using explicit web browser: {}".format(path))
        webbrowser.register('explicit', None,
                            webbrowser.BackgroundBrowser(path))
        wb = webbrowser.get('explicit')
    wb.open(url)


def open_dashboard(clname, url_only):
    """클러스터의 대쉬보드 열기."""
    check_cluster(clname)
    clinfo = load_cluster_info(clname)

    if clinfo['type'] == 'dask':
        scd = clinfo['instance']['scheduler']
        public_ip = scd['public_ip']
        url = "http://{}:8787".format(public_ip)
        if url_only:
            print(url)
        else:
            open_url(url, clinfo)
    else:
        raise NotImplementedError()


def open_notebook(clname, url_only=False):
    """노트북 열기."""
    check_cluster(clname)
    clinfo = load_cluster_info(clname)

    if 'notebook_url' in clinfo:
        url = clinfo['notebook_url']
        if url_only:
            print(url)
        else:
            open_url(url, clinfo)
    else:
        raise Exception("No notebook instance.")


def stop_notebook_or_python(clname, path, params):
    """실행한 노트북/파이썬 파일을 중단."""
    info("stop_notebook_or_python: {} - {}".format(clname, path))
    check_cluster(clname)
    clinfo = load_cluster_info(clname)
    private_command = clinfo.get('private_command')

    if 'notebook' not in clinfo:
        raise RuntimeError("No notebook instance.")

    ncfg = clinfo['notebook']
    user, private_key = ncfg['ssh_user'], ncfg['ssh_private_key']
    ip = _get_ip(ncfg, private_command)

    ext = path.split('.')[-1].lower()

    dask_scd_addr = _get_dask_scheduler_address(clinfo)
    # 노트북 파일
    if ext == 'ipynb':
        # Run by papermill
        _cmd, _ = _get_run_notebook(path, params, [dask_scd_addr])
        _cmd = _cmd.replace('papermill', '[p]apermill')
    # 파이썬 파일
    elif ext == 'py':
        params = list(params)
        params.insert(0, dask_scd_addr)
        _cmd = _get_run_python(path, params)
        _cmd = _cmd.replace('python', '[p]ython')
    else:
        raise RuntimeError("Unsupported file type: {}".format(path))

    # 실행된 프로세스 찾기
    cmd = "ps auxww | grep '{}' | awk '{{print $2}}' | head -n 1".format(_cmd)
    res, _ = send_instance_cmd(user, private_key, ip, cmd, show_stdout=False,
                               show_stderr=False)

    # 프로세스가 있으면 삭제
    if len(res) > 0:
        proc = res[0].strip()
        cmd = "pkill -P {}".format(proc)
        info("Delete process: {}".format(cmd))
        res, _ = send_instance_cmd(user, private_key, ip, cmd,
                                   show_stderr=False)
    else:
        info("No process exists.")


def _iter_run_param(params):
    for param in params:
        match = re.search(PARAM_PTRN, param)
        if match is None:
            raise RuntimeError("Parameter syntax error: '{}'".format(param))
        key, value = match.groups()
        yield key, value


def _get_run_notebook(path, nb_params, cmd_params=None):
    assert type(nb_params) in [list, tuple]

    tname = next(tempfile._get_candidate_names())
    tmp = '/tmp/{}'.format(tname)
    elms = path.split('.')
    out_path = '.'.join(elms[:-1]) + '.out.' + elms[-1]
    cmd = "cd {} && ".format(NB_WORKDIR)

    if cmd_params is not None:
        assert type(cmd_params) in [list, tuple]
        for key, value in _iter_run_param(cmd_params):
            cmd += "{}={} ".format(key, value)

    cmd += "papermill --cwd {} --no-progress-bar --stdout-file " \
        "{} {} {}".format(NB_WORKDIR, tmp, path, out_path)

    for key, value in _iter_run_param(nb_params):
        cmd += " -p {} {}".format(key, value)

    info("_get_run_notebook : {}".format(cmd))
    return cmd, tmp


def _get_run_python(path, params):
    cmd = 'cd {} && '.format(NB_WORKDIR)

    for key, value in _iter_run_param(params):
        cmd += "{}={} ".format(key, value)

    cmd += "python {}".format(path)
    info("_get_run_python : {}".format(cmd))
    return cmd


def run_notebook_or_python(clname, path, params):
    """원격 노트북 인스턴스에서 노트북 또는 파이썬 파일 실행."""
    info("run_notebook_or_python: {} - {}".format(clname, path))

    check_cluster(clname)
    clinfo = load_cluster_info(clname)
    private_command = clinfo.get('private_command')

    if 'notebook' not in clinfo['instance']:
        raise RuntimeError("No notebook instance.")

    ntpl = clinfo['template']['notebook']
    user, private_key = ntpl['ssh_user'], ntpl['ssh_private_key']
    nb = clinfo['instance']['notebook']
    nip = _get_ip(nb, private_command)

    ext = path.split('.')[-1].lower()

    dask_scd_addr = None
    if 'scheduler' in clinfo['instance']:
        dask_scd_addr = _get_dask_scheduler_address(clinfo)
    else:
        for param in params:
            if param.startswith('DASK_SCHEDULER_ADDRESS'):
                dask_scd_addr = param
    assert dask_scd_addr is not None, "No Dask scheduler address available."

    # 노트북 파일
    if ext == 'ipynb':
        # Run by papermill
        cmd, tmp = _get_run_notebook(path, params, [dask_scd_addr])
        res, _ = send_instance_cmd(user, private_key, nip, cmd,
                                   show_stdout=True, show_stderr=False)
        cmd = 'cat {}'.format(tmp)
        res, _ = send_instance_cmd(user, private_key, nip, cmd)
    # 파이썬 파일
    elif ext == 'py':
        params = list(params)
        params.insert(0, dask_scd_addr)
        cmd = _get_run_python(path, params)
        res, _ = send_instance_cmd(user, private_key, nip, cmd,
                                   show_stdout=True)
    else:
        raise RuntimeError("Unsupported file type: {}".format(path))

    return res


def show_plan(profile, clname, params):
    """실행 계획 표시"""
    if clname is None:
        clname = '.'.join(profile.lower().split('.')[0:-1])
    print("\nCluster name: {}\n".format(clname))

    pro = read_profile(profile, params)
    if 'dask' in pro:
        print("Bilbo will create Dask cluster with following options:")

    clinfo = init_clinfo(clname)
    clinfo['profile'] = pro
    clinfo = resolve_instances(clinfo)
    has_instance = False
    ntpl = clinfo['template']['notebook']
    if ntpl is not None:
        print("")
        print("  Notebook:")
        show_instance_plan(ntpl)
        has_instance = True
        print("")

    if 'dask' in pro:
        show_dask_plan(clinfo)
        has_instance = True

    if not has_instance:
        print("\nNothing to do.\n")


def show_instance_plan(tpl):
    """인스턴스 플랜."""
    print("    AMI: {}".format(tpl['ami']))
    print("    Instance Type: {}".format(tpl['ec2type']))
    print("    Security Group: {}".format(tpl['security_group']))
    print("    Volume Size: {}".format(tpl['vol_size']))
    print("    Key Name: {}".format(tpl['keyname']))
    if tpl.get('tags') is not None:
        print("    Tags:")
        for tag in tpl['tags']:
            print("        {}: {}".format(tag[0], tag[1]))


def show_dask_plan(clinfo):
    """클러스터 생성 계획 표시."""
    print("  Cluster Type: Dask")

    print("")
    print("  1 Scheduler:")
    stpl = clinfo['template']['scheduler']
    show_instance_plan(stpl)

    wtpl = clinfo['template']['worker']
    print("")
    print("  {} Worker(s):".format(wtpl['count']))
    show_instance_plan(wtpl)
    print("")


def init_clinfo(clname):
    clinfo = {'name': clname, 'template': {}, 'instance': {}}
    return clinfo


def resolve_instances(clinfo):
    """프로파일에서 생성할 인스턴스 정보 결정."""
    pro = clinfo['profile']
    resolved = {}
    pinst = pro['instance'] if 'instance' in pro else None

    def _merge(inst1, inst2):
        minst = {**inst1, **inst2}
        if 'tags' in inst1 and 'tags' in inst2:
            tags1 = dict(inst1['tags'])
            tags2 = dict(inst2['tags'])
            minst['tags'] = {**tags1, **tags2}
        return minst

    def _resolve(role):
        _pinst = {} if pinst is None else dict(pinst)

        if role in pro and 'instance' in pro[role]:
            _inst = _merge(_pinst, pro[role]['instance'])
        else:
            _inst = dict(_pinst)

        validate_inst(role, _inst)
        return _inst

    tpl = clinfo['template']
    if 'notebook' in pro:
        tpl['notebook'] = _resolve('notebook')
    if 'dask' in pro:
        tpl['scheduler'] = _resolve('scheduler')
        tpl['worker'] = _resolve('worker')
        if 'worker' in pro['dask']:
            dworker = pro['dask']['worker']
            wcnt = dworker['count'] if 'count' in dworker else 1
            tpl['worker']['count'] = wcnt
    return clinfo


def check_dup_cluster(clname):
    """클러스터 이름이 겹치는지 검사."""
    path = os.path.join(clust_dir, clname + '.json')
    if os.path.isfile(path):
        raise NameError("Cluster '{}' already exist.".format(clname))


def start_services(clinfo):
    critical("start_services")
    remote_nb = 'notebook' in clinfo['profile']
    if remote_nb:
        start_notebook(clinfo)
    if 'type' in clinfo:
        start_cluster(clinfo)
    # 서비스 정보 추가 후 다시 저장
    save_cluster_info(clinfo)
    return remote_nb
