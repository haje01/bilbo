# bilbo

bilbo(빌보)는 AWS상에서 파이썬 데이터 과학용 클러스터를 관리해주는 툴이다. 현재 [Dask](https://dask.org) 를 지원하고, 앞으로 [Ray](https://github.com/ray-project/ray) 도 지원할 예정이다.

다음과 같은 일을 할 수 있다.

* 설정에 맞게 AWS에 클러스터 생성
* 노트북/대쉬보드를 브라우저로 오픈
* 클러스터 재시작
* 생성된 클러스터 제거

bilbo는 Linux, Window, macOS 에서 사용 가능하며, Python 3.6 이상 버전을 필요로 한다.

## 준비 작업

### bilbo 설치

아래와 같이 코드를 설치하고

    $ git clone https://github.com/haje01/bilbo.git

클론한 디렉토리로 이동 후 `pip` 로 설치한다.

    $ pip install -e .

설치가 잘 되었으면 bilbo에 어떤 명령이 있는지 살펴보자.

    $ bilbo

    Usage: bilbo [OPTIONS] COMMAND [ARGS]...

    Options:
    -v, --verbose  Increase message verbosity.
    --help         Show this message and exit.

    Commands:
    clusters   List active clusters.
    create     Create cluster.
    dashboard  Open dashboard.
    desc       Describe a cluster.
    destroy    Destroy cluster.
    notebook   Open notebook.
    plan       Show create cluster plan.
    profiles   List all profiles.
    rcmd       Command to a cluster instance.
    restart    Restart cluster.
    version    Show bilbo version.

다양한 명령들이 있는 데 하나씩 살펴볼 것이다. 간단히 bilbo의 버전을 알아보자.

    $ bilbo version

    0.0.1

빌보를 최초로 실행하면 설치된 OS의 유저 홈 디렉토리 아래에 `.bilbo` 라는 디렉토리가 생성되는데, 이것을 **빌보 홈 디렉토리**로 부르겠다. OS 별 위치는 다음과 같다.

* Linux - `/home/<UserName>/.bilbo`
* macOS - `/Users/<UserName>/.bilbo`
* Windows - `C:\Users\<UserName>\.bilbo`

편의상 경로는 Linux/macOS 기준으로 설명하겠다.

### AMI (Amazon Machine Image) 만들기

데이터 과학에 필요한 여러 패키지를 설치한 AMI를 미리 만들어 두면 필요할 때 바로 띄워서 쓸 수 있다. AMI는 VM 인스턴스를 만들어서 직접 설치 후 `이미지 생성` 을 통해 만들 수도 있으나, 형상 관리가 힘든 문제점이 있다.

재현 가능한 데이터 과학을 위해서는 이미지 명세를 남기고 git 등으로 관리하는 것이 좋은데, 이를 위해 HashiCorp의 [Packer](https://www.packer.io)를 사용하는 것을 추천한다. 이를 위해 먼저 [Packer Install](https://www.packer.io/intro/getting-started/install.html)을 참고하여 Packer를 설치하자.

설명에서는 ML을 위한 이미지를 가정해, AWS에서 제공하는 `Deep Learning AMI (Ubuntu 16.04) Version 26.0`을 소스 AMI로 하겠다. 필수적으로 설치되어야 하는 것들은 다음과 같다.

* 클러스터 - Dask 관련 패키지
* 노트북 - Jupyter Lab을 기준으로 한다.

필요에 따라 패키지를 추가적으로 설치해 이용하면 된다.

#### Packer 설정 파일 만들기

Packer의 설정파일은 `.json` 형식으로 기술한다. 아래의 내용을 `my-image.json`으로 저장한다.

```json
{
    "variables": {
        "aws_access_key": "{{env `AWS_ACCESS_KEY_ID`}}",
        "aws_secret_key": "{{env `AWS_SECRET_ACCESS_KEY`}}",
        "region":         "ap-northeast-2"
    },
    "builders": [
        {
            "type": "amazon-ebs",
            "access_key": "{{user `aws_access_key`}}",
            "ami_name": "my-image",
            "source_ami": "ami-099a57eaf71294a34",
            "instance_type": "t2.micro",
            "region": "{{user `region`}}",
            "secret_key": "{{user `aws_secret_key`}}",
            "ssh_username": "ubuntu"
        }
    ],
    "provisioners": [
        {
            "type": "shell",
            "inline": [
                "sleep 60",
                "sudo apt-get update"
            ]
        },
        {
            "type": "shell",
            "script": "setup.sh"
        }
    ]
}
```

* AWS의 접속 및 비밀 키는 각각 환경 변수 `AWS_ACCESS_KEY_ID`와 `AWS_SECRET_ACCESS_KEY`에 저장되어 있다고 가정한다.
* `instance-type`은 이미지를 만들기 위한 VM의 타입이기에 `t2.micro`로 충분하다.
* `provisioners` 아래에 설치 스크립트가 온다.
* 첫 번째 쉘 스크립트에 `sleep 60`은 OS의 초기 작업이 끝나기를 기다리기 위한 것이다.

용도별로 필요한 패키지의 설치는 `setup.sh`에 별도로 기술한다.

#### 설치 스크립트

아래의 내용을 참고하여, 필요에 맞게 변경해 `setup.sh`으로 저장한다.

```sh
export DEBIAN_FRONTEND noninteractive
sudo apt-get install -y graphviz git nodejs

pip install blosc
pip install lz4
pip install python-snappy
pip install dask==2.9.0
pip install numpy==1.17.3
pip install pandas==0.25.
pip install jupyter
pip install jupyterlab
jupyter labextension install @jupyterlab/toc
pip install dask-labextension==1.0.3
pip install graphviz
pip install pyarrow
pip install fsspec>=0.3.3
pip install s3fs>=0.4.0
```

#### Packer로 AMI 만들기

이제 아래의 명령으로 AMI을 만든다.

    $ packer build my-image.json

다음과 같은 메시지가 출력되면서 이미지가 만들어진다.

```
amazon-ebs output will be in this color.

==> amazon-ebs: Prevalidating AMI Name: my-image
    amazon-ebs: Found Image ID: ami-099a57eaf71294a34
==> amazon-ebs: Creating temporary keypair: packer_5df1eac8-747c-6001-e73c-69e4ac04a837
==> amazon-ebs: Creating temporary security group for this instance: packer_5df1eac9-a29b-6906-c99d-5b7a96ceb707
==> amazon-ebs: Authorizing access to port 22 from [0.0.0.0/0] in the temporary security groups...
==> amazon-ebs: Launching a source AWS instance...
==> amazon-ebs: Adding tags to source instance
    amazon-ebs: Adding tag: "Name": "Packer Builder"
    amazon-ebs: Instance ID: i-03006f5bbab3e077c
==> amazon-ebs: Waiting for instance (i-03006f5bbab3e077c) to become ready...

.
.
.

==> amazon-ebs: Waiting for AMI to become ready...
==> amazon-ebs: Terminating the source AWS instance...
==> amazon-ebs: Cleaning up any extra volumes...
==> amazon-ebs: No volumes to clean up, skipping
==> amazon-ebs: Deleting temporary security group...
==> amazon-ebs: Deleting temporary keypair...
Build 'amazon-ebs' finished.

==> Builds finished. The artifacts of successful builds are:
--> amazon-ebs: AMIs were created:
ap-northeast-2: ami-043c907754421d916
```

성공하면 최종적으로 `ami-043c907754421d916`와 같은 AMI ID가 출력된다. 이 값을 기록해 두고 이후 bilbo 에서 사용한다.

이미지 만들기에 사용된 인스턴스(`t2.micro`)는 자동으로 삭제되며, AWS EC2 대쉬보드 왼쪽 `AMI` 메뉴에서 AMI ID 또는 이름으로 제대로 등록되었는지 확인할 수 있다.

위 과정에서 생성된 Packer 설정 파일(`my-image.json`) 및 설치 스크립트(`setup.sh`)를 코드 저장소에 추가하여 관리하면 될 것이다.

### EC2 보안 그룹 생성하기

클러스터의 용도에 맞도록 VM 에 접근 제한을 하면 더 안전하게 클러스터를 사용할 수 있다. 여기서는 Dask 클러스터와 Jupyter 노트북을 사용하는 것을 가정하고 보안 그룹을 만들어 보겠다. 필요에 따라 설정을 변경/추가하여 사용하기 바란다.

* AWS EC2 대쉬보드로 이동후 `보안 그룹 생성`을 누른다.
* 보안 그룹 이름과 설명을 적절히 입력 (영어로).
* VPC는 기본값을 선택
* 다음과 같이 인바운드 규칙을 추가한다.
  * 유형 `SSH`, 포트 범위 `22`, 소스 `내 IP`, 설명 `SSH`
  * 유형 `사용자 지정 TCP`, 포트 범위 `8786`, 소스 `내 IP`, 설명 `Dask Scheduler`
  * 유형 `사용자 지정 TCP`, 포트 범위 `8787`, 소스 `내 IP`, 설명 `Dask Dashboard`
  * 유형 `사용자 지정 TCP`, 포트 범위 `8888`, 소스 `내 IP`, 설명 `Jupyter Notebook`
* `생성` 버튼을 눌러 보안 그룹을 만들고 대쉬보드에서 확인

추가로 클러스터 인스턴스들 사이의 통신을 위해 아래의 작업을 해야 한다.

* 생성된 보안 그룹의 ID(`sg-`로 시작하는)를 복사한다.
* 생성된 보안 그룹에 대해 `작업` / `인바운드 규칙 편집` 을 누른다.
  * 유형 `모든 TCP`, 소스 `사용자 지정` 후 복사해둔 보안 그룹ID 기입, 설명 `Dask Cluster`

최종적으로는 다음과 같은 모습이 될 것이다. 이 보안 그룹의 ID를 이후 bilbo 에서 사용한다.

![보안 그룹](/assets/2020-01-08-13-43-54.png)

## 시작하기

모든 준비가 완료되었으면, 이제 bilbo의 설정 파일을 만들어 보자. 클러스터를 만들기 위한 이 설정 파일을 **프로파일**이라고 부르겠다. 다양한 설정의 프로파일을 만들어 두고, 그것에 기반해 필요에 따라 클러스터를 만들어 사용하게 된다.

bilbo 의 프로파일은 `.json` 형식으로 기술하는데, 이를 위한 [JSON Schema](https://json-schema.org) 를 제공한다. [VS Code](https://code.visualstudio.com)처럼 JSON Schema를 지원하는 에디터를 사용하면 인텔리센스와 검증을 기능이 있어 편리하다.

프로파일은 `~/.bilbo/profiles` 디렉토리에 저장한다.

### 가장 간단한 프로파일

아래는 가장 단순한 프로파일의 예이다. 이 내용을 `~/.bilbo/profiles/test.json` 으로 저장하자.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "my-keypair",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/my-private.pem",
    }
}
```

* `$schema`로 bilbo 프로파일의 스키마를 지정한다.
* `instance` 에 클러스터에서 사용할 공통 장비의 사양을 명시한다.
  * `ami` - 미리 만들어 둔 AMI의 ID
  * `security_group` - 미리 만들어 둔 보안 그룹의 ID
  * `ec2type` - 사용할 EC2 인스턴스의 타입
  * `keyname` - 사용할 AWS Key Pair의 이름
  * `ssh_user` - 인스턴스 생성 후 SSH 로그인할 유저 (우분투는 `ubuntu`, Amazon Linux면 `ec2-user`)
  * `ssh_private_key` - Key Pair의 프라이빗키 경로

이 프로파일은 단순히 공통 인스턴스의 정보를 명시할 뿐 실제로 어떤 인스턴스도 만들지 않는다. `plan` 명령으로 인스턴스를 만들기전 미리 확인할 수 있다. 명령행 인자에서 **프로파일은 확장자를 포함한 파일명**(`test.json`)으로 사용한다.

    $ bilbo plan test.json

    Nothing to do.

### 노트북 인스턴스 명시하기

bilbo 는 분석을 위한 노트북이 사용자의 로컬 머신에 있을 수도 있고 클라우드에 만들 수도 있다고 가정한다. 여기에서는 위의 파일을 수정하여 AWS에 노트북 인스턴스가 만들어지도록 해보겠다.

> 주의: 분석 노트북은 Jupyter Lab을 기준으로 한다. bilbo는 AMI에 Jupyter Lab이 설치된 것을 가정한다.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "my-keypair",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/my-keypair.pem",
    },
    "notebook": {}
}
```

`notebook` 요소는 비어있는데, 여기에 `instance`를 명시하면 공용 인스턴스 설정이 아닌 노트북 전용의 설정을 명시할 수 있다. 여기에서는 빈 값으로 하여 공용 인스턴스 그대로 노트북 인스턴스를 만들겠다.

계획을 다시 확인해보면 노트북 인스턴스가 만들어질 것을 확인할 수 있다.

    $ bilbo plan test.json

      Notebook:
        AMI: ami-0f49fa254e1806b72
        Instance Type: t3.micro
        Security Group: sg-0bc538e0a7c089b4d
        Key Name: my-keypair


### 클러스터 만들기

프로파일 정보를 참고하여 구체화된 인스턴스의 그룹을 **클러스터**로 부른다. 앞에서 만든 프로파일을 이용해, 실제 클러스터를 만들어보자.

    $ bilbo create test.json

    CRITICAL: Create notebook.
    CRITICAL: Start notebook.

    Name: test
    Ready Time: 2020-01-08 15:45:29

    Notebook:
    [1] instance_id: i-0a6dc0cca2f9aaa22, public_ip: 13.125.147.196

노트북 인스턴스 하나가 만들어 지고 그것의 인스턴스 ID와 퍼블릭 IP를 확인할 수 있다. 클러스터의 이름은 따로 지정되지 않으면 프로파일 이름에서 확장자를 제외한 것이 기본으로 쓰인다. 위에서는 `test`가 클러스터 이름이 된다. 다음처럼 클러스터 이름을 명시적으로 줄 수 있다.

    $ bilbo create test.json -n test-cluster

AWS 대쉬보드에서도 생성된 노트북 인스턴스를 볼 수 있다. `'클러스터 이름'-notebook` 형식 이름을 갖는다.

![EC2 대쉬보드 확인](/assets/2020-01-08-17-56-10.png)

### 클러스터 확인 및 노트북 열기

현재 만들어진 클러스터들은 아래와 같이 확인할 수 있다.

    $ bilbo clusters

    test

현재는 `test` 하나만 확인된다. 구체적인 클러스터 정보는 (`create` 후 출력되는 정보) `desc`로 볼 수 있다.

    $ bilbo desc test

    Name: test4
    Ready Time: 2020-01-08 17:20:45

    Notebook:
    [1] instance_id: i-0b90d9d9e7d43ad4e, public_ip: 13.124.174.197

> 주의 : 프로파일과 달리 클러스터는 명령행 인자에서 확장자 `.json` 이 없이 사용한다.

`-d` 옵션을 주면 더 상세한 정보를 JSON 형식으로 표시한다.

    $ bilbo desc test -d

    {
        "instances": [
            "i-0b90d9d9e7d43ad4e"
        ],
        "name": "test4",
        "notebook": {
            "ec2type": "t3.micro",
            "image_id": "ami-0f49fa254e1806b72",
            "instance_id": "i-0b90d9d9e7d43ad4e",
            "key_name": "wzdat-seoul",
            "private_dns_name": "ip-172-31-24-144.ap-northeast-2.compute.internal",
            "public_ip": "13.124.174.197",
            "ssh_private_key": "~/.ssh/wzdat-seoul.pem",
            "ssh_user": "ubuntu"
        },
        "notebook_url": "http://13.124.174.197:8888/?token=281f55d245249d657f59093a95b22e60762192a76c478961",
        "ready_time": "2020-01-08 17:20:45"
    }

`notebook_url` 에서 Jupyter 노트북의 토큰이 포함된 URL이 보인다. 아래와 같이 입력하면 이곳으로 편리하게 접속할 수 있다.

    $ bilbo notebook test

기본 웹브라우저를 통해 생성된 인스턴스의 노트북 페이지가 열린다.

![노트북](/assets/2020-01-08-15-58-31.png)


### 클러스터 제거하기

다음과 같이 클러스터 이름으로 클러스터를 제거할 수 있다.

    $ bilbo destroy test

깜박하고 제거하지 않으면 많은 비용이 부과될 수 있기에, 사용 후에는 꼭 제거하자.

## Dask 클러스터 만들기

이제 본격적으로 Dask를 클러스터를 만들어 보겠다.

### 로컬 노트북에서 Dask 이용하기

이번에는 로컬 머신에 설치된 Jupyter 노트북을 이용하고, Dask 클러스터를 클라우드에 만드는 경우를 예로 들겠다.

> 주의: 로컬 머신에 설치된 패키지(특히 Dask)와 클라우드에 설치된 패키지의 버전이 다르면 이상한 문제가 발생할 수 있다. 최대한 버전을 맞추어 설치하도록 하자.

앞의 `test.json` 프로파일을 아래처럼 수정한다.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "wzdat-seoul",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/wzdat-seoul.pem"
    },
    "dask": {
        "scheduler": {},
        "worker": {
            "count": 2
        }
    }
}
```

* `notebook` 요소가 사라지고, 대신 `dask` 요소가 들어갔다. 여기에 Dask 클러스터 관련 설정을 기입한다.
* Dask 스케쥴러는 반드시 하나가 필요하기에, `scheduler` 요소가 없어도 자동으로 공통 인스턴스 사양으로 만들어진다.
* Dask 워커도 명시하지 않으면 기본으로 하나 만들어진다. `worker`/`count` 요소에 필요한 워커의 개수를 명시할 수 있다.
* `scheduler` 와 `worker` 에도 `instance`를 명시하면 공용 설정이 아닌 전용의 인스턴스 설정을 명시할 수 있다.

이 프로파일을 이용해 클러스터를 `test-cluster` 이름으로 만든다.

    $ bilbo create test.json -n test-cluster

  CRITICAL: Create dask cluster 'test4'.
  CRITICAL: Start dask scheduler & workers.
  CRITICAL: Waiting for Dask dashboard ready.

  Name: test-cluster
  Ready Time: 2020-01-08 17:27:33
  Type: dask

  Scheduler:
    [1] instance_id: i-0c0daef0140104f93, public_ip: 13.125.254.215

  Workers:
    [2] instance_id: i-07a2b454893849d7e, public_ip: 13.125.216.247
    [3] instance_id: i-05f77758fdaa7bd66, public_ip: 15.164.228.145

로컬 노트북에서 이 Dask 클러스터를 이용하려면 스케쥴러의 Public IP가 필요하기에, 위에서 출력된 스케쥴러의 `public_ip` (여기서는 `13.125.254.215`) 를 복사해 두자.

이제 로컬 노트북을 띄우고 아래와 같이 Dask 클러스터를 이용할 수 있다.

![원격 Dask 클러스터 접속](/assets/2020-01-08-17-40-53.png)

확인이 끝나면 클러스터를 제거하자.

    $ bilbo destroy test-cluster

## 클라우드 노트북에서 Dask 이용하기

