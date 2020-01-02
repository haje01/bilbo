"""클러스터 모듈."""
import os
import json
import datetime

import boto3

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
    res = ec2.run_instances(ImageId=pobj.scd_node.ami,
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

    ins = res['Instances'][0]
    clinfo['instances'].append(ins['InstanceId'])
    clinfo['scheduler'] = ins

    # create worker
    wrk_name = '{}-dask-worker'.format(clname)
    res = ec2.run_instances(ImageId=pobj.wrk_node.ami,
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
    for ins in res['Instances']:
        clinfo['instances'].append(ins['InstanceId'])
        clinfo['workers'].append(ins)

    save_cluster_info(clname, clinfo)


def save_cluster_info(clname, clinfo):
    """클러스터 정보파일 쓰기."""

    def json_default(value):
        if isinstance(value, datetime.date):
            return value.strftime('%Y-%m-%d')
        raise TypeError('not JSON serializable')

    warning("save_cluster_info: '{}'".format(clname))
    path = os.path.join(clust_dir, clname + '.json')
    with open(path, 'wt') as f:
        body = json.dumps(clinfo, default=json_default)
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
    ec2 = boto3.client('ec2')
    cltype = pcfg['cluster']['type']
    if cltype == 'dask':
        return create_dask_cluster(clname, pcfg, ec2, dry)
    else:
        raise NotImplementedError(cltype)


def show_clusters():
    """모든 클러스터를 표시."""
    for cl in iter_clusters():
        name = cl.split('.')[0]
        print("{}".format(name))


def show_cluster(clname):
    """클러스터 정보를 표시."""
    info = load_cluster_info(clname)

    print("")
    print("Name: {}".format(info['name']))
    print("Type: {}".format(info['type']))

    print("")
    print("Scheduler:")
    scd = info['scheduler']
    print("  Instance Id: {}".format(scd['InstanceId']))

    print("")
    print("Workers:")
    wrks = info['workers']
    for wrk in wrks:
        print("  Instance Id: {}".format(wrk['InstanceId']))
    print("")


def destroy_cluster(clname, dry):
    """클러스터 제거."""
    critical("Destroy cluster '{}'.".format(clname))
    info = load_cluster_info(clname)

    ec2 = boto3.client('ec2')
    ec2.terminate_instances(InstanceIds=info['instances'], DryRun=dry)

    path = os.path.join(clust_dir, clname + '.json')
    os.unlink(path)