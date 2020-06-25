"""프로파일 모듈."""
import os
import json
import re
from copy import copy
import codecs

import boto3
import jsonschema

from bilbo.util import error, prof_dir, mod_dir, info, PARAM_PTRN

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


def override_cfg_by_params(cfg, params):
    """CLI 패러미터로 프로파일 설정을 덮어씀."""

    def get_list_index(s):
        try:
            return int(s)
        except ValueError:
            raise RuntimeError("Illegal list index: {}".format(s))

    def get_typed_value(s):
        try:
            return int(s)
        except ValueError:
            return s

    for param in params:
        match = re.search(PARAM_PTRN, param)
        if match is None:
            raise RuntimeError("Parameter syntax error: '{}'".format(param))
        key, value = match.groups()
        value = get_typed_value(value)

        kelms = key.split('.')
        target = cfg

        # 대상 dict 찾기
        for ke in kelms[:-1]:
            if ke in target:
                target = target[ke]
            elif type(target) is list:
                idx = get_list_index(ke)
                target = target[idx]
            else:
                target[ke] = {}
                target = target[ke]

        # 값을 쓰기
        ke = kelms[-1]
        if type(target) is list:
            idx = get_list_index(ke)
            target[idx] = value
        else:
            target[ke] = value


def read_profile(profile, params=None):
    """프로파일 읽기.

    Args:
        profile: 프로파일 명
        params: 덮어쓸 패러미터 정보

    Returns:
        dict: 프로파일 정보

    """
    info("read_profile {}".format(profile))
    path = check_profile(profile)
    with codecs.open(path, 'rb', encoding='utf-8') as f:
        body = f.read()
        pro = json.loads(body)

    # 프로파일 내용 검증
    validate_by_schema(pro)

    # Override 패러미터가 있으면 적용
    if params is not None:
        override_cfg_by_params(pro, params)
        # 덮어쓴 내용 검증
        try:
            validate_by_schema(pro)
        except jsonschema.exceptions.ValidationError:
            msgs = ["There is an incorrect parameter:"]
            for param in params:
                msgs.append('  {}'.format(param))
            raise RuntimeError('\n'.join(msgs))

    pro['name'] = os.path.basename(path)
    return pro
