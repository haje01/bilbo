"""프로파일 테스트."""

from bilbo.profile import DaskProfile


def test1():
    cfg = {
        "instance": {
            'ami': 'ami-000',
            "ec2type": "base-ec2type",
            "keyname": "base-key",
            "ssh_user": "ubuntu",
            "ssh_private_key": "base-key.pem",
            "tags": [
                ["Owner", "BaseOwner"],
                ["Service", "BaseService"]
            ]
        },
        "cluster": {
            "type": "dask"
        }
    }
    pro = DaskProfile(cfg)
    assert pro.inst.ami == 'ami-000'
    assert pro.inst.ec2type == 'base-ec2type'
    assert pro.inst.keyname == 'base-key'
    assert pro.inst.ssh_user == 'ubuntu'
    assert pro.inst.ssh_private_key == 'base-key.pem'
    assert pro.inst.secgroup is None
    assert pro.cluster_type == 'dask'
    assert len(pro.inst.tags) == 2
    assert len(pro.inst.tags[0]) == 2

    assert pro.scd_inst.ami == 'ami-000'
    assert pro.scd_inst.ec2type == 'base-ec2type'
    assert pro.scd_inst.keyname == 'base-key'
    assert pro.scd_inst.secgroup is None
    assert pro.scd_cnt == 1

    assert pro.wrk_inst.ami == 'ami-000'
    assert pro.wrk_inst.ec2type == 'base-ec2type'
    assert pro.wrk_inst.keyname == 'base-key'
    assert pro.wrk_inst.secgroup is None
    assert pro.wrk_cnt == 1


def test2():
    cfg = {
        "instance": {
            'ami': 'ami-000',
            "ec2type": "base-ec2type",
            "security_group": "sg-000",
            "keyname": "base-key",
            "ssh_user": "ubuntu",
            "ssh_private_key": "~/.ssh/base-key.pem",
            "tags": [
                ["Owner", "BaseOwner"],
                ["Service", "BaseService"]
            ]
        },
        "cluster": {
            "type": "dask",
            'scheduler': {
                "instance": {
                    'ami': 'ami-001',
                    "ec2type": "scd-ec2type",
                    "security_group": "sg-001",
                    "keyname": "scd-key",
                    "ssh_user": "ec2-user",
                    "ssh_private_key": "scd-key.pem",
                    "tags": [
                        ["Owner", "ScdOwner"],
                        ["Service", "ScdService"]
                    ]
                }
            },
            'worker': {
                "instance": {
                    'ami': 'ami-002',
                    "ec2type": "wrk-ec2type",
                    "keyname": "wrk-key",
                    "ssh_private_key": "wrk-key.pem",
                    "tags": [
                        ["Owner", "WrkOwner"],
                        ["Service", "WrkService"]
                    ]
                },
                "count": 2
            }
        }
    }
    pro = DaskProfile(cfg)
    assert pro.inst.ami == 'ami-000'
    assert pro.inst.ec2type == 'base-ec2type'
    assert pro.inst.secgroup == 'sg-000'
    assert pro.cluster_type == 'dask'

    assert pro.scd_inst.ami == 'ami-001'
    assert pro.scd_inst.ec2type == 'scd-ec2type'
    assert pro.scd_inst.keyname == 'scd-key'
    assert pro.scd_inst.ssh_user == 'ec2-user'
    assert pro.scd_inst.ssh_private_key == 'scd-key.pem'
    assert pro.scd_inst.secgroup == 'sg-001'
    assert pro.scd_cnt == 1
    assert len(pro.scd_inst.tags) == 2
    assert pro.scd_inst.tags[0][1] == "ScdOwner"

    assert pro.wrk_inst.ami == 'ami-002'
    assert pro.wrk_inst.ec2type == 'wrk-ec2type'
    assert pro.wrk_inst.keyname == 'wrk-key'
    assert pro.wrk_inst.secgroup == 'sg-000'
    assert pro.wrk_inst.ssh_user == 'ubuntu'
    assert pro.wrk_inst.ssh_private_key == 'wrk-key.pem'
    assert pro.wrk_cnt == 2
    assert len(pro.wrk_inst.tags) == 2
    assert pro.wrk_inst.tags[0][1] == "WrkOwner"

