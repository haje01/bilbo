"""프로파일 테스트."""

from bilbo.profile import DaskProfile


def test1():
    cfg = {
        'ami': 'ami-000',
        "instance_type": "base-instype",
        "keyname": "base-key",
        "cluster": {
            "type": "dask"
        }
    }
    pro = DaskProfile(cfg)
    assert pro.node.ami == 'ami-000'
    assert pro.node.instype == 'base-instype'
    assert pro.node.keyname == 'base-key'
    assert pro.node.secgroup is None
    assert pro.cluster_type == 'dask'

    assert pro.scd_node.ami == 'ami-000'
    assert pro.scd_node.instype == 'base-instype'
    assert pro.scd_node.keyname == 'base-key'
    assert pro.scd_node.secgroup is None
    assert pro.scd_cnt == 1

    assert pro.wrk_node.ami == 'ami-000'
    assert pro.wrk_node.instype == 'base-instype'
    assert pro.wrk_node.keyname == 'base-key'
    assert pro.wrk_node.secgroup is None
    assert pro.wrk_cnt == 1


def test2():
    cfg = {
        'ami': 'ami-000',
        "instance_type": "base-instype",
        "security_group": "sg-000",
        "cluster": {
            "type": "dask",
            'scheduler': {
                'ami': 'ami-001',
                'keyname': 'scd-key',
                "security_group": "sg-001",
            },
            'worker': {
                'ami': 'ami-002',
                'keyname': 'wrk-key',
                "count": 2
            }
        }
    }
    pro = DaskProfile(cfg)
    assert pro.node.ami == 'ami-000'
    assert pro.node.instype == 'base-instype'
    assert pro.node.secgroup == 'sg-000'
    assert pro.cluster_type == 'dask'

    assert pro.scd_node.ami == 'ami-001'
    assert pro.scd_node.instype == 'base-instype'
    assert pro.scd_node.keyname == 'scd-key'
    assert pro.scd_node.secgroup == 'sg-001'
    assert pro.scd_cnt == 1

    assert pro.wrk_node.ami == 'ami-002'
    assert pro.wrk_node.instype == 'base-instype'
    assert pro.wrk_node.keyname == 'wrk-key'
    assert pro.wrk_node.secgroup == 'sg-000'
    assert pro.wrk_cnt == 2

