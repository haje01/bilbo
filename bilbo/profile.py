"""프로파일 모듈."""
import os
import json
from copy import copy

import boto3
import jsonschema

from bilbo.util import error, prof_dir, mod_dir, info

DEFAULT_WORKER = 1


def get_latest_schema():
    """최신 프로파일 json schema를 얻음."""
    scm_dir = os.path.join(mod_dir, '..', 'schemas')
    schemas = []
    for scm in os.listdir(scm_dir):
        # skip test
        if scm.startswith('_'):
            continue
        schemas.append(scm)
    assert len(schemas)
    schemas = sorted(schemas)
    path = os.path.join(scm_dir, schemas[-1])
    with open(path, 'rt') as f:
        parsed = json.loads(f.read())
    return parsed


def check_profile(proname):
    """프로파일을 확인.

    Args:
        proname (str): 프로파일명 (.json 확장자 포함)
    """
    if not proname.lower().endswith('.json'):
        msg = "Wrong profile name '{}'. Use '{}.json' instead.". \
              format(proname, proname)
        raise NameError(msg)

    # file existence
    path = os.path.join(prof_dir, proname)
    if not os.path.isfile(path):
        error("Profile '{}' does not exist.".format(path))
        raise(FileNotFoundError(path))

    return path


def validate_by_schema(pcfg):
    """프로파일을 스키마로 점검."""
    schema = get_latest_schema()
    jsonschema.validate(pcfg, schema)


def read_profile(profile):
    """프로파일 읽기."""
    info("read_profile {}".format(profile))
    path = check_profile(profile)
    with open(path, 'rt') as f:
        body = f.read()
        pcfg = json.loads(body)

    validate_by_schema(pcfg)
    return pcfg


class Instance:
    """프로파일 내 노드 정보."""

    def __init__(self, icfg):
        """설정에서 멤버 초기화."""
        self.ami = icfg.get('ami')
        self.ec2type = icfg.get('ec2type')
        self.keyname = icfg.get('keyname')
        self.secgroup = icfg.get('security_group')
        self.ssh_user = icfg.get('ssh_user')
        self.ssh_private_key = icfg.get('ssh_private_key')
        self.tags = icfg.get('tags')

    def overwrite(self, icfg):
        """다른 설정으로 멤버 덮어쓰기."""
        self.ami = icfg.get('ami', self.ami)
        self.ec2type = icfg.get('ec2type', self.ec2type)
        self.keyname = icfg.get('keyname', self.keyname)
        self.secgroup = icfg.get('security_group', self.secgroup)
        self.ssh_user = icfg.get('ssh_user', self.ssh_user)
        self.ssh_private_key = icfg.get('ssh_private_key',
                                        self.ssh_private_key)
        self.tags = icfg.get('tags', self.tags)

    def validate(self):
        """인스턴스 유효성 점검."""
        if self.ami is None:
            raise ValueError("No 'ami' value.")
        if self.ec2type is None:
            raise ValueError("No 'ec2type' value.")
        if self.keyname is None:
            raise ValueError("No 'keyname' value.")
        if self.secgroup is None:
            raise ValueError("No 'secrurity_group' value.")
        if self.ssh_user is None:
            raise ValueError("No 'ssh_user' value.")
        if self.ssh_private_key is None:
            raise ValueError("No 'ssh_private_key' value.")


class Profile:
    """프로파일 기본 객체."""

    def __init__(self, pcfg):
        """초기화 및 검증."""
        validate_by_schema(pcfg)
        # 공통 노드 정보
        self.inst = None
        if 'instance' in pcfg:
            self.inst = Instance(pcfg['instance'])

        # 노트북 정보
        self.nb_inst = None
        ncfg = pcfg.get('notebook')
        if ncfg is not None:
            self.nb_inst = copy(self.inst)
            nicfg = ncfg.get('instance')
            if nicfg is not None:
                self.nb_inst.overwrite(nicfg)

        self.clcfg = pcfg.get('dask')
        if self.clcfg is not None:
            self.type = self.clcfg.get('type')

    def validate(self):
        """프로파일 유효성 점검."""
        if self.nb_inst is not None:
            self.nb_inst.validate()


class DaskProfile(Profile):
    """다스크 프로파일."""

    def __init__(self, pcfg):
        pretty = json.dumps(pcfg, indent=4, sort_keys=True)
        info("Create DaskProfile from config:\n{}".format(pretty))
        super(DaskProfile, self).__init__(pcfg)
        self.type = 'dask'
        self.clcfg = pcfg.get('dask')

        # 스케쥴러
        self.scd_inst = copy(self.inst)
        self.scd_cnt = 1
        scfg = self.clcfg.get('scheduler')
        if scfg is not None:
            sicfg = scfg.get('instance')
            if sicfg is not None:
                self.scd_inst.overwrite(sicfg)

        # 워커
        self.wrk_inst = copy(self.inst)
        wcfg = self.clcfg.get('worker')
        self.wrk_cnt = DEFAULT_WORKER
        if wcfg is not None:
            wicfg = wcfg.get('instance')
            if wicfg is not None:
                self.wrk_inst.overwrite(wicfg)
            self.wrk_cnt = wcfg.get('count', self.wrk_cnt)
            self.wrk_nthread = wcfg.get('nthread')
            self.wrk_nproc = wcfg.get('nproc')

    def validate(self):
        """프로파일 유효성 점검."""
        super(DaskProfile, self).validate()
        if self.scd_inst is not None:
            self.scd_inst.validate()
        if self.wrk_inst is not None:
            self.wrk_inst.validate()


def show_plan(profile, clname):
    """실행 계획 표시"""
    pcfg = read_profile(profile)
    if clname is None:
        clname = profile.lower().split('.')[0]

    if 'dask' not in pcfg:
        pobj = Profile(pcfg)
    else:
        print("Bilbo will create Dask cluster with following options:")
        pobj = DaskProfile(pcfg)

    has_instance = False
    if pobj.nb_inst is not None:
        print("")
        print("  Notebook:")
        show_instance_plan(pobj.nb_inst)
        has_instance = True
        print("")

    if hasattr(pobj, 'dask'):
        show_dask_plan(clname, pobj)
        has_instance = True

    if not has_instance:
        print("\nNothing to do.\n")


def show_instance_plan(inst):
    """인스턴스 플랜."""
    print("    AMI: {}".format(inst.ami))
    print("    Instance Type: {}".format(inst.ec2type))
    print("    Security Group: {}".format(inst.secgroup))
    print("    Key Name: {}".format(inst.keyname))


def show_dask_plan(clname, pobj):
    """클러스터 생성 계획 표시."""
    print("  Cluster Type: Dask")

    print("")
    print("  1 Scheduler:")
    show_instance_plan(pobj.scd_inst)

    print("")
    print("  {} Worker(s):".format(pobj.wrk_cnt))
    show_instance_plan(pobj.wrk_inst)
    print("")
