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


def validate(pcfg):
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

    validate(pcfg)
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


class Profile:
    """프로파일 기본 객체."""

    def __init__(self, pcfg):
        """초기화 및 검증."""
        validate(pcfg)
        # 공통 노드 정보
        self.inst = None
        if 'instance' in pcfg:
            self.inst = Instance(pcfg['instance'])

        self.cluster = pcfg.get('cluster')
        self.cluster_type = self.cluster.get('type')


class DaskProfile(Profile):
    """다스크 프로파일."""

    def __init__(self, pcfg):
        pretty = json.dumps(pcfg, indent=4, sort_keys=True)
        info("Create DaskProfile from config:\n{}".format(pretty))
        super(DaskProfile, self).__init__(pcfg)
        self.cluster = pcfg.get('cluster')

        # 스케쥴러
        self.scd_inst = copy(self.inst)
        self.scd_cnt = 1
        scfg = self.cluster.get('scheduler')
        if scfg is not None:
            sicfg = scfg.get('instance')
            if sicfg is not None:
                self.scd_inst.overwrite(sicfg)

        # 워커
        self.wrk_inst = copy(self.inst)
        wcfg = self.cluster.get('worker')
        self.wrk_cnt = DEFAULT_WORKER
        if wcfg is not None:
            wicfg = wcfg.get('instance')
            if wicfg is not None:
                self.wrk_inst.overwrite(wicfg)
            self.wrk_cnt = wcfg.get('count', self.wrk_cnt)
            self.wrk_nthread = wcfg.get('nthread')
            self.wrk_nproc = wcfg.get('nproc')