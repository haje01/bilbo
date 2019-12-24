"""프로파일 모듈."""
import os
import json

import boto3
import jsonschema

from bilbo.util import error, prof_dir, mod_dir


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
        proname (str): 프로파일명 (.json 파일명 제외)
    """
    if proname.lower().endswith('.json'):
        error("Wrong profile name '{}'. Use only filename without "
              "extension.".format(proname))
        raise NameError(proname)

    # file existence
    path = "{}/{}.json".format(prof_dir, proname)
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


class Profile:
    def __init__(self, pro):
        self.pro = pro
        cluster = pro['cluster']

        if 'common_instance' in pro:
            com = pro['common_instance']
            self.com_ami = getattr(com, 'ami', None)
            self.com_cnt = getattr(com, 'count', None)

        if 'worker' in cluster:
            worker = cluster['worker']
            self.worker_ami = getattr(worker, 'ami', None)
            self.worker_cnt = getattr(worker, 'count', None)
