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

from bilbo.profile import read_profile, DaskProfile, Profile
from bilbo.util import critical, warning, error, clust_dir, iter_clusters, \
    info, get_aws_config, PARAM_PTRN

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


def create_ec2_instances(ec2, inst, cnt, tag_spec, clinfo=None):
    """EC2 인스턴스 생성."""
    rdm = get_root_dm(ec2, inst)

    try:
        ins = ec2.create_instances(ImageId=inst.ami,
                                   InstanceType=inst.ec2type,
                                   MinCount=cnt, MaxCount=cnt,
                                   KeyName=inst.keyname,
                                   BlockDeviceMappings=rdm,
                                   SecurityGroupIds=[inst.secgroup],
                                   TagSpecifications=tag_spec)
        return ins
    except botocore.exceptions.ClientError as e:
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


def create_dask_cluster(clname, pobj, ec2, clinfo):
    """Dask 클러스터 생성.

    Args:
        clname (str): 클러스터 이름. 이미 존재하면 에러
        pobj (bilbo.profile.Profile): 프로파일 정보
        ec2 (botocore.client.EC2): boto EC2 client
    """
    critical("Create dask cluster '{}'.".format(clname))

    # 기존 클러스터가 있으면 에러
    if cluster_info_exists(clname):
        raise Exception("Cluster '{}' already exists.".format(clname))

    clinfo['type'] = 'dask'

    # create scheduler
    scd_name = pobj.scd_inst.get_name(clname)
    scd_tag_spec = _build_tag_spec(scd_name, pobj.desc, pobj.scd_inst.tags)
    ins = create_ec2_instances(ec2, pobj.scd_inst, 1, scd_tag_spec)
    scd = ins[0]
    clinfo['instances'].append(scd.instance_id)
    clinfo['launch_time'] = datetime.datetime.now()

    # create workers
    wrk_name = pobj.wrk_inst.get_name(clname)
    wrk_tag_spec = _build_tag_spec(wrk_name, pobj.desc, pobj.wrk_inst.tags)
    ins = create_ec2_instances(ec2, pobj.wrk_inst, pobj.wrk_cnt, wrk_tag_spec)
    inst = pobj.wrk_inst
    winfo = get_type_instance_info(inst)
    winfo['count'] = pobj.wrk_cnt
    # 프로파일에서 지정된 thread/proc 수
    winfo['nthread'] = pobj.wrk_nthread
    winfo['nproc'] = pobj.wrk_nproc
    winfo['instances'] = []
    clinfo['worker'] = winfo
    for wrk in ins:
        clinfo['instances'].append(wrk.instance_id)

    # 사용 가능 상태까지 기다린 후 추가 정보 얻기.
    info("Wait for instance to be running.")
    scd.wait_until_running()
    scd.load()

    inst = pobj.scd_inst
    sinfo = get_type_instance_info(inst, scd)
    clinfo['scheduler'] = sinfo

    for wrk in ins:
        wrk.wait_until_running()
        wrk.load()
        wi = {}
        wi['instance_id'] = wrk.instance_id
        wi['public_ip'] = wrk.public_ip_address
        wi['private_ip'] = wrk.private_ip_address
        wi['private_dns_name'] = wrk.private_dns_name
        winfo['instances'].append(wi)

    # ec2 생성 후 반환값의 `ncpu_options` 가 잘못오고 있어 여기서 요청.
    if len(ins) > 0:
        # 첫 번째 워커의 ip
        wip = _get_ip(winfo['instances'][0], pobj.private_command)
        winfo['cpu_info'] = get_cpu_info(pobj, wip)


def get_cpu_info(pobj, ip):
    """생성된 인스턴스에서 lscpu 명령으로 CPU 정보 얻기."""
    info("get_cpu_info")
    user = pobj.wrk_inst.ssh_user
    private_key = pobj.wrk_inst.ssh_private_key
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


def save_cluster_info(clname, clinfo):
    """클러스터 정보파일 쓰기."""
    def json_default(value):
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d %H:%M:%S')
        raise TypeError('not JSON serializable')

    warning("save_cluster_info: '{}'".format(clname))
    clinfo['ready_time'] = datetime.datetime.now()

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


def get_root_dm(ec2, inst):
    """AMI 별 원하는 크기의 디바이스 매핑 얻기."""
    if inst.volsize is None:
        return []
    imgs = list(ec2.images.filter(ImageIds=[inst.ami]))
    rdev = imgs[0].root_device_name
    dm = [{"DeviceName": rdev, "Ebs": {"VolumeSize": inst.volsize}}]
    info("get_root_dm: {}".format(dm))
    return dm


def create_notebook(clname, pobj, ec2, clinfo):
    """노트북 생성."""
    critical("Create notebook.")
    nb_name = pobj.nb_inst.get_name(clname)
    nb_tag_spec = _build_tag_spec(nb_name, pobj.desc, pobj.nb_inst.tags)
    ins = create_ec2_instances(ec2, pobj.nb_inst, 1, nb_tag_spec)
    nb = ins[0]
    info("Wait for notebook instance to be running.")
    nb.wait_until_running()
    nb.load()
    clinfo['instances'].append(nb.instance_id)
    ninfo = get_type_instance_info(pobj.nb_inst, nb)
    clinfo['notebook'] = ninfo


def check_dup_cluster(clname):
    """클러스터 이름이 겹치는지 검사."""
    path = os.path.join(clust_dir, clname + '.json')
    if os.path.isfile(path):
        raise NameError("Cluster '{}' already exist.".format(clname))


def create_cluster(profile, clname, params):
    """클러스터 생성."""

    if clname is None:
        clname = '.'.join(profile.lower().split('.')[0:-1])

    check_dup_cluster(clname)

    pcfg = read_profile(profile, params)
    ec2 = boto3.resource('ec2')

    # 클러스터 생성
    clinfo = {'name': clname, 'instances': []}

    # 다스크 프로파일
    if 'dask' in pcfg:
        pobj = DaskProfile(pcfg)
        pobj.validate()
        create_dask_cluster(clname, pobj, ec2, clinfo)
    # 공통 프로파일 (테스트용)
    else:
        pobj = Profile(pcfg)
        pobj.validate()

    # 클러스터 정보에 필요한 프로파일 정보 복사
    if 'description' in pcfg:
        clinfo['description'] = pcfg['description']
    if 'webbrowser' in pcfg:
        clinfo['webbrowser'] = pcfg['webbrowser']
    clinfo['private_command'] = pcfg.get('private_command', False)

    # 노트북 생성
    if 'notebook' in pcfg:
        create_notebook(clname, pobj, ec2, clinfo)

    return pobj, clinfo


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
    path = check_cluster(clname)
    if detail:
        with open(path, 'rt') as f:
            body = f.read()
            print(body)
        return

    info = load_cluster_info(clname)

    print()
    print("Cluster Name: {}".format(info['name']))
    print("Ready Time: {}".format(info['ready_time']))
    print("Use Private IP: {}".format(info['private_command']))

    idx = 1
    if 'notebook' in info:
        print()
        print("Notebook:")
        idx = show_instance(idx, info['notebook'])
        print()

    if 'type' in info:
        cltype = info['type']
        print("Cluster Type: {}".format(cltype))
        if cltype == 'dask':
            show_dask_cluster(idx, info)
        else:
            raise NotImplementedError()
    print()


def show_instance(idx, inst):
    print("  [{}] instance_id: {}, public_ip: {}, private_ip: {}".
          format(idx, inst['instance_id'], inst['public_ip'],
                 inst['private_ip']))
    return idx + 1


def show_dask_cluster(idx, info):
    """Dask 클러스터 표시."""
    print()
    print("Scheduler:")
    scd = info['scheduler']
    idx = show_instance(idx, scd)

    print()
    print("Workers:")
    winfo = info['worker']
    for wrk in winfo['instances']:
        idx = show_instance(idx, wrk)


def check_git_modified(clinfo):
    """로컬 git 저장소 변경 여부.

    Commit 되지 않거나, Push 되지 않은 내용이 있으면 경고

    Returns:
        bool: 변경이 없거나, 유저가 확인한 경우 True

    """
    nip = _get_ip(clinfo['notebook'], clinfo['private_command'])
    user = clinfo['notebook']['ssh_user']
    private_key = clinfo['notebook']['ssh_private_key']
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
    info = load_cluster_info(clname)

    if 'git_cloned_dir' in info and not force:
        if not check_git_modified(info):
            print("Canceled.")
            return

    critical("Destroy cluster '{}'.".format(clname))
    # 인스턴스 제거
    ec2 = boto3.client('ec2')
    instances = info['instances']
    if len(instances) > 0:
        ec2.terminate_instances(InstanceIds=info['instances'])

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


def find_cluster_instance_by_public_ip(cluster, public_ip):
    """Public IP로 클러스터 인스턴스 정보 찾기."""
    clpath = check_cluster(cluster)

    with open(clpath, 'rt') as f:
        body = f.read()
        clinfo = json.loads(body)

    if 'notebook' in clinfo:
        if clinfo['notebook']['public_ip'] == public_ip:
            return clinfo['notebook']

    if clinfo['type'] == 'dask':
        scd = clinfo['scheduler']
        if scd['public_ip'] == public_ip:
            return scd
        winfo = clinfo['worker']
        for wrk in winfo['instances']:
            if wrk['public_ip'] == public_ip:
                return wrk
    else:
        raise NotImplementedError()


def dask_worker_options(winfo, memory):
    """Dask 클러스터 워커 인스턴스 정보에서 워커 옵션 구하기."""
    co = winfo['cpu_info']
    nproc = winfo['nproc'] or co['CoreCount']
    nthread = winfo['nthread'] or co['ThreadsPerCore']
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
    dns = clinfo['scheduler']['private_dns_name']
    return "DASK_SCHEDULER_ADDRESS=tcp://{}:8786".format(dns)


def _get_ip(cfg, private_command):
    assert type(private_command) == bool or private_command is None
    return cfg['private_ip'] if private_command else cfg['public_ip']


def start_notebook(pobj, clinfo, retry_count=10):
    """노트북 시작.

    Args:
        clinfo (dict): 클러스터 생성 정보
        retry_count (int): 접속 URL 얻기 재시도 수. 기본 10

    Raises:
        TimeoutError: 재시도 수가 넘을 때

    """
    critical("Start notebook.")

    ncfg = clinfo['notebook']
    user, private_key = ncfg['ssh_user'], ncfg['ssh_private_key']
    ip = _get_ip(ncfg, pobj.private_command)

    # AWS 크레덴셜 설치
    setup_aws_creds(user, private_key, ip)

    # 작업 폴더
    nb_workdir = pobj.nb_workdir or NB_WORKDIR
    cmd = "mkdir -p {}".format(nb_workdir)
    send_instance_cmd(user, private_key, ip, cmd)

    # git 설정이 있으면 설정
    if pobj.nb_git is not None:
        setup_git(pobj, user, private_key, ip, nb_workdir, clinfo)

    # 클러스터 타입별 노트북 설정
    vars = ''
    if 'type' in clinfo:
        if clinfo['type'] == 'dask':
            # dask-labextension을 위한 대쉬보드 URL
            sip = clinfo['scheduler']['public_ip']
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
            url = stdouts[1].strip().replace('0.0.0.0', ncfg['public_ip'])
            clinfo['notebook_url'] = url
            return
        info("Can not fetch notebook list. Wait for a while.")
        time.sleep(TRY_SLEEP)
    raise TimeoutError("Can not get notebook url.")


def setup_git(pobj, user, private_key, ip, nb_workdir, clinfo):
    """Git 설정 및 클론."""
    # config
    cmd = "git config --global user.name '{}'; ".format(pobj.nb_git.user)
    cmd += "git config --global user.email '{}'".format(pobj.nb_git.email)
    send_instance_cmd(user, private_key, ip, cmd)

    # 클론 (작업 디렉토리에)
    gobj = pobj.nb_git
    grepo = gobj.repository
    guser = gobj.user
    gpasswd = gobj.password
    repos = [grepo] if type(grepo) is str else grepo
    cdirs = []
    for repo in repos:
        cmd = git_clone_cmd(repo, guser, gpasswd, nb_workdir)
        send_instance_cmd(user, private_key, ip, cmd, show_stderr=False)
        gcdir = repo.split('/')[-1].replace('.git', '')
        cdirs.append("{}/{}".format(nb_workdir, gcdir))
    clinfo['git_cloned_dir'] = cdirs


def start_dask_cluster(clinfo):
    """Dask 클러스터 마스터/워커를 시작."""
    critical("Start dask scheduler & workers.")
    private_command = clinfo['private_command']

    # 스케쥴러 시작
    scd = clinfo['scheduler']
    user, private_key = scd['ssh_user'], scd['ssh_private_key']
    sip = _get_ip(scd, private_command)
    scd_dns = scd['private_dns_name']
    cmd = "screen -S bilbo -d -m dask-scheduler"
    send_instance_cmd(user, private_key, sip, cmd)

    # AWS 크레덴셜 설치
    setup_aws_creds(user, private_key, sip)

    winfo = clinfo['worker']
    # 워커 실행 옵션
    wip = _get_ip(winfo['instances'][0], private_command)
    info("  Get worker memory from '{}'".format(wip))
    cmd = "free -b | grep 'Mem:' | awk '{print $2}'"
    stdouts, _ = send_instance_cmd(user, private_key, wip, cmd)
    memory = int(stdouts[0])
    nproc, nthread, memory = dask_worker_options(winfo, memory)
    # 결정된 옵션 기록
    winfo['nproc'] = nproc
    winfo['nthread'] = nthread
    winfo['memory'] = memory

    # 모든 워커들에 대해
    user, private_key = winfo['ssh_user'], winfo['ssh_private_key']
    for wrk in winfo['instances']:
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
    critical("Waiting for Dask dashboard ready.")
    wait_until_connect(dash_url)


def stop_cluster(clname):
    """클러스터 마스터/워커를 중지.

    Returns:
        dict: 클러스터 정보(재시작 용)
    """
    clpath = check_cluster(clname)

    with open(clpath, 'rt') as f:
        body = f.read()
        clinfo = json.loads(body)
    private_command = clinfo['private_command']

    if clinfo['type'] == 'dask':
        critical("Stop dask scheduler & workers.")
        # 스케쥴러 중지
        scd = clinfo['scheduler']
        user, private_key = scd['ssh_user'], scd['ssh_private_key']
        sip = _get_ip(scd, private_command)
        cmd = "screen -X -S 'bilbo' quit"
        send_instance_cmd(user, private_key, sip, cmd)

        worker = clinfo['worker']
        user, private_key = worker['ssh_user'], worker['ssh_private_key']
        for wrk in worker['instances']:
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
        scd = clinfo['scheduler']
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
    private_command = clinfo['private_command']

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
    private_command = clinfo['private_command']

    if 'notebook' not in clinfo:
        raise RuntimeError("No notebook instance.")

    ncfg = clinfo['notebook']
    user, private_key = ncfg['ssh_user'], ncfg['ssh_private_key']
    nip = _get_ip(ncfg, private_command)

    ext = path.split('.')[-1].lower()

    dask_scd_addr = _get_dask_scheduler_address(clinfo)
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
