"""클러스터 모듈."""
import json

import boto3

from bilbo.profile import read_profile
from bilbo.util import critical, warning


def create_dask_cluster(pcfg, ec2, dry):
    """Dask 클러스터 생성."""
    critical("Create dask cluster.")
    warning("===========================")
    pretty = json.dumps(pcfg, indent=4, sort_keys=True)
    warning(pretty)
    warning("===========================")

    pobj = DaskProfile(pcfg).resolve()

    # create scheduler
    ec2.create_instances(ImageId=pobj.scheduler_ami, MinCount=1)
    # create worker
    ec2.create_instances(ImageId=pobj.worker_ami, MinCount=worker_cnt)


def create_cluster(proname, dry):
    """클러스터 생성."""
    pcfg = read_profile(proname)
    ec2 = boto3.client('ec2')
    cltype = pcfg['cluster']['type']
    if cltype == 'dask':
        return create_dask_cluster(pro, ec2, dry)
    else:
        raise NotImplementedError(cltype)
