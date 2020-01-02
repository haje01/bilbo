"""프로파일 모듈."""
import os
import json
from copy import copy

import boto3
import jsonschema

from bilbo.util import error, prof_dir, mod_dir

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
        error("Wrong profile name '{}'. Use only filename without "
              "extension.".format(proname))
        raise NameError(proname)

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
    path = check_profile(profile)
    with open(path, 'rt') as f:
        body = f.read()
        pcfg = json.loads(body)

    validate(pcfg)
    return pcfg


class Node:
    """프로파일 내 노드 정보."""

    def __init__(self, ncfg):
        self.ami = ncfg.get('ami')
        self.instype = ncfg.get('instance_type')
        self.keyname = ncfg.get('keyname')
        self.secgroup = ncfg.get('security_group')

    def overwrite(self, ncfg):
        self.ami = ncfg.get('ami', self.ami)
        self.instype = ncfg.get('instance_type', self.instype)
        self.keyname = ncfg.get('keyname', self.keyname)
        self.secgroup = ncfg.get('security_group', self.secgroup)


class Profile:
    """프로파일 기본 객체."""

    def __init__(self, pcfg):
        """초기화 및 검증."""
        validate(pcfg)
        # 공통 노드 정보
        self.node = Node(pcfg)

        self.cluster = pcfg.get('cluster')
        self.cluster_type = self.cluster.get('type')


class DaskProfile(Profile):
    """다스크 프로파일."""

    def __init__(self, pcfg):
        super(DaskProfile, self).__init__(pcfg)
        self.cluster = pcfg.get('cluster')

        # 스케쥴러
        self.scd_node = copy(self.node)
        self.scd_cnt = 1
        scfg = self.cluster.get('scheduler')
        if scfg is not None:
            self.scd_node.overwrite(scfg)

        # 워커
        self.wrk_node = copy(self.node)
        wcfg = self.cluster.get('worker')
        self.wrk_cnt = DEFAULT_WORKER
        if wcfg is not None:
            self.wrk_node.overwrite(wcfg)
            self.wrk_cnt = wcfg.get('count', self.wrk_cnt)