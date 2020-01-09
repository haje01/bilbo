# bilbo

bilbo (빌보) 는 AWS 상에서 파이썬 데이터 엔지니어링/과학용 클러스터를 만들고 관리해주는 툴이다. 현재 [Dask](https://dask.org) 를 지원하고, 앞으로 [Ray](https://github.com/ray-project/ray) 도 지원할 예정이다.

다음과 같은 일을 할 수 있다.

* 설정에 맞게 AWS에 클러스터 생성
* 노트북/대쉬보드를 브라우저로 오픈
* 클러스터 정보 보기 및 재시작
* 생성된 클러스터 제거

bilbo 는 Linux, macOS, Windows 에서 사용 가능하며, Python 3.6 이상 버전을 필요로 한다. 또한 AWS 를 기반으로 하기에, 유저는 AWS 계정이 있고 기본적인 대쉬보드 사용이 가능한 것으로 가정하겠다.

---
  - [준비 작업](#%ec%a4%80%eb%b9%84-%ec%9e%91%ec%97%85)
    - [bilbo 설치](#bilbo-%ec%84%a4%ec%b9%98)
    - [AWS 환경 준비](#aws-%ed%99%98%ea%b2%bd-%ec%a4%80%eb%b9%84)
    - [AMI (Amazon Machine Image) 만들기](#ami-amazon-machine-image-%eb%a7%8c%eb%93%a4%ea%b8%b0)
      - [Packer 설정 파일 만들기](#packer-%ec%84%a4%ec%a0%95-%ed%8c%8c%ec%9d%bc-%eb%a7%8c%eb%93%a4%ea%b8%b0)
      - [설치 스크립트](#%ec%84%a4%ec%b9%98-%ec%8a%a4%ed%81%ac%eb%a6%bd%ed%8a%b8)
      - [Packer로 AMI 만들기](#packer%eb%a1%9c-ami-%eb%a7%8c%eb%93%a4%ea%b8%b0)
    - [EC2 보안 그룹 생성하기](#ec2-%eb%b3%b4%ec%95%88-%ea%b7%b8%eb%a3%b9-%ec%83%9d%ec%84%b1%ed%95%98%ea%b8%b0)
  - [시작하기](#%ec%8b%9c%ec%9e%91%ed%95%98%ea%b8%b0)
    - [가장 간단한 프로파일](#%ea%b0%80%ec%9e%a5-%ea%b0%84%eb%8b%a8%ed%95%9c-%ed%94%84%eb%a1%9c%ed%8c%8c%ec%9d%bc)
    - [노트북 인스턴스 명시하기](#%eb%85%b8%ed%8a%b8%eb%b6%81-%ec%9d%b8%ec%8a%a4%ed%84%b4%ec%8a%a4-%eb%aa%85%ec%8b%9c%ed%95%98%ea%b8%b0)
    - [클러스터 만들기](#%ed%81%b4%eb%9f%ac%ec%8a%a4%ed%84%b0-%eb%a7%8c%eb%93%a4%ea%b8%b0)
    - [클러스터 확인 및 노트북 열기](#%ed%81%b4%eb%9f%ac%ec%8a%a4%ed%84%b0-%ed%99%95%ec%9d%b8-%eb%b0%8f-%eb%85%b8%ed%8a%b8%eb%b6%81-%ec%97%b4%ea%b8%b0)
    - [클러스터 제거하기](#%ed%81%b4%eb%9f%ac%ec%8a%a4%ed%84%b0-%ec%a0%9c%ea%b1%b0%ed%95%98%ea%b8%b0)
  - [Dask 클러스터](#dask-%ed%81%b4%eb%9f%ac%ec%8a%a4%ed%84%b0)
    - [로컬 노트북에서 클라우드 Dask 이용하기](#%eb%a1%9c%ec%bb%ac-%eb%85%b8%ed%8a%b8%eb%b6%81%ec%97%90%ec%84%9c-%ed%81%b4%eb%9d%bc%ec%9a%b0%eb%93%9c-dask-%ec%9d%b4%ec%9a%a9%ed%95%98%ea%b8%b0)
    - [클라우드 노트북에서 클라우드 Dask 이용하기](#%ed%81%b4%eb%9d%bc%ec%9a%b0%eb%93%9c-%eb%85%b8%ed%8a%b8%eb%b6%81%ec%97%90%ec%84%9c-%ed%81%b4%eb%9d%bc%ec%9a%b0%eb%93%9c-dask-%ec%9d%b4%ec%9a%a9%ed%95%98%ea%b8%b0)
    - [활용 팁](#%ed%99%9c%ec%9a%a9-%ed%8c%81)
      - [클러스터 재시작](#%ed%81%b4%eb%9f%ac%ec%8a%a4%ed%84%b0-%ec%9e%ac%ec%8b%9c%ec%9e%91)
      - [워커 설정](#%ec%9b%8c%ec%bb%a4-%ec%84%a4%ec%a0%95)
---


## 준비 작업

### bilbo 설치

아래와 같이 코드를 클론하고,

    $ git clone https://github.com/haje01/bilbo.git

클론된 디렉토리로 이동 후 `pip` 로 설치한다.

    $ pip install -e .

설치가 잘 되었으면 bilbo 에 어떤 명령이 있는지 살펴보자.

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

다양한 명령들이 있는 데 하나씩 살펴볼 것이다. 간단히 bilbo 의 버전을 알아보자.

    $ bilbo version

    0.0.1

bilbo 를 최초로 실행하면 설치된 OS의 유저 홈 디렉토리 아래에 `.bilbo` 라는 디렉토리가 생성되는데, 이것을 **빌보 홈 디렉토리**로 부르겠다. OS 별 위치는 다음과 같다.

* Linux - `/home/<UserName>/.bilbo`
* macOS - `/Users/<UserName>/.bilbo`
* Windows - `C:\Users\<UserName>\.bilbo`

빌보 홈 디렉토리는 편의상 `~/.bilbo` 로 칭하겠다. 참고로, 이 안에는 다음과 같은 하위 디렉토리가 있다.

    ~/.bilbo
        clusters/  # 생성된 클러스터 정보
        logs/      # 로그 디렉토리
        profiles/  # 프로파일 디렉토리


### AWS 환경 준비

AWS EC2 인스턴스를 만들고 관리하기 위해 아래의 준비가 필요하다.

* EC2 용 키페어(Key Pair)
* AWS IAM 유저
* AWS 환경변수 설정

키페어는 한 번이라도 EC2 인스턴스를 만들었다면 준비되어 있을 것이다. 아니라면 [관련 글](https://victorydntmd.tistory.com/61)을 참고하여 준비하자.

사용 가능한 IAM 유저가 없다면, [이 글](https://www.44bits.io/ko/post/publishing_and_managing_aws_user_access_key) 을 참고하여 만들자. 이 과정에서 얻은 Access / Secret 키를 기록해두자.

환경변수는 다음과 같은 세 가지를 사용한다:

* `AWS_ACCESS_KEY_ID` - IAM 유저의 Access 키
* `AWS_SECRET_ACCESS_KEY` - IAM 유저의 Secret 키
* `AWS_DEFAULT_REGION` - 기본 AWS 리전 (한국은 `ap-northeast-2`)

위의 환경변수가 명령창(터미널)을 띄울 때마다 활성화되도록 사용하는 OS에 맞게 설정해 두자.

### AMI (Amazon Machine Image) 만들기

데이터 과학에 필요한 여러 패키지를 설치한 AMI를 미리 만들어 두면, 필요할 때 바로 VM 인스턴스를 띄워서 쓸 수 있다. AMI는 인스턴스를 만들어서 직접 설치 후 AWS EC2 대쉬보드의 `이미지 생성` 명령을 통해 만들 수도 있으나, **형상 관리가 힘든 문제**점이 있다.

*재현 가능한 데이터 과학* 을 위해서는 이미지 명세를 텍스트로 작성하고, 이것을 git 등으로 관리하는 것이 좋은데, 이를 위해 HashiCorp의 [Packer](https://www.packer.io)를 사용하는 것을 추천한다. 먼저 [Packer Install](https://www.packer.io/intro/getting-started/install.html) 을 참고하여 Packer 를 설치하자.

설명에서는 기계학습을 위한 이미지를 가정해, AWS에서 제공하는 `Deep Learning AMI (Ubuntu 16.04) Version 26.0` 을 소스 AMI로 하겠다 (CUDA 등이 미리 설치되어있어 향후 GPU 인스턴스를 사용할 때 편리하다). bilbo 를 위해 필수로 설치해야 하는 것들은 다음과 같다.

* 클러스터 - Dask 관련 패키지
* 노트북 - Jupyter Lab을 기준으로 한다.
* 기타 Python 데이터 과학 패키지

노트북/스케쥴러/워커 등 인스턴스 역할별로 AMI 를 따로 만들어 사용할 수 도 있으나, 유지보수의 편의상 모두 하나의 AMI 에 설치하겠다.

#### Packer 설정 파일 만들기

Packer의 설정파일은 `.json` 형식으로 기술한다. 적당한 이미지 생성용 디렉토리에 아래 내용을 `my-image.json`으로 저장한다.

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

* 앞에서 언급한 환경변수 `AWS_ACCESS_KEY_ID`와 `AWS_SECRET_ACCESS_KEY`에 설정되어 있어야 한다.
* `instance-type` 은 이미지를 만들기 위한 VM의 타입이기에 `t2.micro`로 충분하다.
* `provisioners` 아래에 설치 스크립트가 온다.
* 첫 번째 쉘 스크립트에 `sleep 60`은 OS의 초기 작업이 끝나기를 기다리기 위한 것이다.

필요한 패키지의 설치는 `setup.sh` 에 별도로 기술한다.

#### 설치 스크립트

아래의 내용을 참고하여, 필요에 맞게 변경해 `setup.sh` 로 저장한다.

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

추가적으로 필요한 패키지도 등록할 수 있겠다.

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

성공하면 최종적으로 `ami-043c907754421d916` 와 같은 AMI ID가 출력된다. 이 값을 기록해 두고, 이후 bilbo 에서 사용한다.

이미지 만들기에 사용된 인스턴스( `t2.micro`) 는 자동으로 삭제되며, 생성된 이미지는 AWS EC2 대쉬보드 왼쪽 `AMI` 메뉴에서 AMI ID 또는 이름으로 확인할 수 있다.

위 과정에서 생성된 Packer 설정 파일 (`my-image.json`) 및 설치 스크립트(`setup.sh`) 를 코드 저장소에 추가하여 관리하면 될 것이다.

### EC2 보안 그룹 생성하기

클러스터의 용도에 맞도록 VM 에 접근 제한을 하면 더 안전하게 클러스터를 사용할 수 있다. 여기서는 bilbo 를 통해 Dask 클러스터와 Jupyter 노트북을 사용하는 것을 가정하고 보안 그룹을 만들어 보겠다. 필요에 따라 설정을 변경/추가하여 사용하기 바란다.

* AWS EC2 대쉬보드로 이동후 `보안 그룹 생성`을 누른다.
* 보안 그룹 이름과 설명을 적절히 입력 (영어로).
* VPC는 기본값을 선택
* 다음과 같이 인바운드 규칙을 추가한다.
  * 유형: `SSH`, 포트 범위: `22`, 소스 `내 IP`, 설명: `SSH`
  * 유형: `사용자 지정 TCP`, 포트 범위: `8786`, 소스: `내 IP`, 설명: `Dask Scheduler`
  * 유형: `사용자 지정 TCP`, 포트 범위: `8787`, 소스: `내 IP`, 설명: `Dask Dashboard`
  * 유형: `사용자 지정 TCP`, 포트 범위: `8888`, 소스: `내 IP`, 설명: `Jupyter Notebook`
* `생성` 버튼을 눌러 보안 그룹을 만들고 대쉬보드에서 확인

![보안 그룹 생성](/assets/2020-01-09-11-36-00.png)

추가로 클러스터 인스턴스들 사이의 내부 통신을 위해 아래의 작업을 해야 한다. AWS EC2 대쉬보드 아래 보안그룹에서

* AWS EC2 대쉬보드 아래 보안그룹에서 생성된 보안 그룹을 선택한다.
* 아래의 `설명` 탭에서 생성된 보안 그룹의 ID(`sg-` 로 시작하는)를 복사한다.
* `작업` / `인바운드 규칙 편집` 을 누른다.
* `규칙 추가` 를 누른 뒤, 유형: `모든 TCP`, 소스: `사용자 지정` 에서 복사해둔 보안 그룹 ID 기입, 설명: `Dask Inside`

최종적으로는 다음과 같은 모습이 될 것이다.

![최종 보안 그룹](/assets/2020-01-09-11-41-33.png)

 `저장` 을 누르고, 이 보안 그룹의 ID 를 이후 bilbo 에서 사용한다.

## 시작하기

모든 준비가 완료되었으면, 이제 bilbo 의 설정 파일을 만들어 보자. 클러스터를 만들기 위한 이 설정 파일을 **프로파일** 이라고 부르겠다. 다양한 설정의 프로파일을 준비해 두고, 필요에 따라 그것에 기반한 클러스터를 만들어 사용하게 된다.

bilbo 의 프로파일은 `.json` 형식으로 기술하는데, 이를 위한 [JSON 스키마](https://json-schema.org) 를 제공한다. [VS Code](https://code.visualstudio.com) 처럼 JSON 스키마를 지원하는 에디터를 사용하면, 인텔리센스와 검증 기능이 있어 편리하다.

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

* `$schema` 로 bilbo 프로파일의 JSON 스키마를 지정한다 (옵션).
* `instance` 요소에 클러스터에서 사용할 공통 장비의 사양을 명시한다.
  * `ami` - 만들어 둔 AMI 의 ID
  * `security_group` - 만들어 둔 보안 그룹의 ID
  * `ec2type` - 사용할 EC2 인스턴스의 타입
  * `keyname` - 사용할 AWS Key Pair의 이름
  * `ssh_user` - 인스턴스 생성 후 SSH 로그인할 유저 (우분투는 `ubuntu`, Amazon Linux 면 `ec2-user`)
  * `ssh_private_key` - 키페어의 프라이빗키 경로

이 프로파일은 단순히 공통 인스턴스의 정보를 명시할 뿐, 실제로 어떤 인스턴스도 만들지 않는다. 아래와 같은 `plan` 명령으로 프로파일의 생성 계획을 미리 확인할 수 있다.

> bilbo 명령에서 **프로파일은 확장자를 포함한 파일명**(`test.json`)으로 사용한다.

    $ bilbo plan test.json

    Nothing to do.

### 노트북 인스턴스 명시하기

bilbo 는 분석을 위한 1) 노트북이 사용자의 로컬 머신에 있을 수도 있고, 2) 클라우드에 만들어 사용할 수도 있다고 가정한다. 여기에서는 위의 프로파일을 수정하여, 클라우드 (AWS) 에 노트북 인스턴스가 만들어지도록 해보겠다.

> 주의: 분석 노트북은 Jupyter Lab을 기준으로 한다. bilbo 는 AMI 에 Jupyter Lab이 설치되어 있다고 가정한다.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "my-keypair",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/my-keypair.pem"
    },
    "notebook": {}
}
```

`notebook` 요소는 비어있는데, 여기에 `instance` 를 따로 명시하면 공용 인스턴스 설정이 아닌 노트북 전용의 설정을 할 수 있다. 여기에서는 빈 값으로 하여 공용 인스턴스 그대로 노트북 인스턴스를 만들겠다.

계획을 다시 확인해보면 노트북 인스턴스가 만들어질 것을 알 수 있다.

    $ bilbo plan test.json

      Notebook:
        AMI: ami-0f49fa254e1806b72
        Instance Type: t3.micro
        Security Group: sg-0bc538e0a7c089b4d
        Key Name: my-keypair


### 클러스터 만들기

프로파일 정보를 참고하여 만들어진 VM 인스턴스의 그룹을 **클러스터**로 부른다. 앞에서 만든 프로파일을 이용해, 실제 클러스터를 만들어보자.

    $ bilbo create test.json

    CRITICAL: Create notebook.
    CRITICAL: Start notebook.

    Name: test
    Ready Time: 2020-01-08 15:45:29

    Notebook:
    [1] instance_id: i-0a6dc0cca2f9aaa22, public_ip: 13.125.147.196

노트북 인스턴스 하나가 만들어졌고, 그것의 인스턴스 ID 와 퍼블릭 IP 를 확인할 수 있다. 클러스터 이름은 따로 지정되지 않으면, 프로파일 이름에서 확장자를 제외한 것이 기본으로 쓰인다. 위에서는 `test` 가 클러스터 이름이 된다. 예를 들어 다음처럼 클러스터 이름을 명시적으로 줄 수도 있다.

    $ bilbo create test.json -n test-cluster

AWS EC2 대쉬보드에서도 생성된 노트북 인스턴스를 볼 수 있다. `클러스터명-notebook` 형식 이름을 갖는다.

![EC2 대쉬보드 확인](/assets/2020-01-08-17-56-10.png)

### 클러스터 확인 및 노트북 열기

만들어진 클러스터들은 아래와 같이 확인할 수 있다.

    $ bilbo clusters

    test

현재는 `test` 클러스터 하나만 확인된다. 구체적인 클러스터 정보는 (`create` 후 출력되는 정보와 같음) `desc` 명령으로 볼 수 있다.

> 주의 : 프로파일과 달리 클러스터 이름에는 확장자 `.json` 이 없다.

    $ bilbo desc test

    Name: test4
    Ready Time: 2020-01-08 17:20:45

    Notebook:
    [1] instance_id: i-0b90d9d9e7d43ad4e, public_ip: 13.124.174.197


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
            "key_name": "my-keypair",
            "private_dns_name": "ip-172-31-24-144.ap-northeast-2.compute.internal",
            "public_ip": "13.124.174.197",
            "ssh_private_key": "~/.ssh/my-keypair.pem",
            "ssh_user": "ubuntu"
        },
        "notebook_url": "http://13.124.174.197:8888/?token=281f55d245249d657f59093a95b22e60762192a76c478961",
        "ready_time": "2020-01-08 17:20:45"
    }

> 참고: 클러스터가 생성되면 `~/.bilbo/clusters` 아래에 클러스터 정보 파일이 생성되는데, 위 내용은 이 파일의 것과 같다.

위의 `notebook_url` 요소에서 Jupyter 노트북의 토큰이 포함된 URL이 보인다. 아래와 같이 입력하면 이곳으로 편리하게 접속할 수 있다.

    $ bilbo notebook test

기본 웹브라우저를 통해 생성된 인스턴스의 노트북 페이지가 열린다.

![노트북](/assets/2020-01-08-15-58-31.png)

> 만약 별도의 웹브라우저를 사용하고 싶다면, 프로파일에 `"webbrowser" : "C:\\Program Files (x86)\\Google\\Chrome\\Applications\\chrome.exe"` 식으로 경로를 명시하면 된다.


### 클러스터 제거하기

다음과 같이 클러스터 이름으로 클러스터를 제거할 수 있다.

    $ bilbo destroy test

깜박하고 제거하지 않으면 많은 비용이 부과될 수 있기에, **사용 후에는 꼭 제거하자.**

## Dask 클러스터

노트북만으로는 클러스터라 할 수 없겠다. 이제 본격적으로 Dask 클러스터를 만들어 보자.

### 로컬 노트북에서 클라우드 Dask 이용하기

먼저, 로컬 머신에 설치된 Jupyter 노트북에서 클라우드에 생성된 Dask 클러스터를 이용하는 경우를 설명하겠다.

> 주의 : 로컬 머신에 설치된 패키지와 클라우드에 설치된 패키지의 버전이 다르면 이상한 문제가 발생할 수 있다 (특히 Dask와 s3fs). 최대한 버전을 맞추어 설치하도록 하자.

앞의 `test.json` 프로파일을 아래처럼 수정한다.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "my-keypair",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/my-keypair.pem"
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
* `scheduler` 와 `worker` 에도 `instance` 요소를 명시하면 공용 설정이 아닌 전용의 인스턴스 설정을 명시할 수 있다.

변경된 프로파일을 이용해 클러스터를 `test-cluster` 이름으로 만든다.

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

AWS EC2 대쉬보드에서도 생성된 인스턴스를 확인할 수 있다. 스케쥴러는 `클러스터명-dask-scheduler` 로, 워커는 `클러스터명-dask-worker` 형식의 이름을 갖는다.

로컬 노트북에서 이 Dask 클러스터를 이용하려면 스케쥴러의 Public IP가 필요하기에, 위에서 출력된 스케쥴러의 `public_ip` (여기서는 `13.125.254.215`) 를 복사해 두자. 이제 로컬 노트북을 띄우고 아래와 같이 Dask 클러스터를 이용할 수 있다.

![원격 Dask 클러스터 접속](/assets/2020-01-08-17-40-53.png)

워커 2 대가 스케쥴러에 붙은 것을 확인할 수 있다.

또한, 다음 명령으로 Dask의 Dashboard 도 볼 수 있다.

    $ bilbo dashboard test-cluster

노트북과 마찬가지로 기본 브라우저에서 열리게 된다.

![Dask 대쉬보드](/assets/2020-01-09-10-53-03.png)

확인이 끝나면 클러스터를 제거하자.

    $ bilbo destroy test-cluster

### 클라우드 노트북에서 클라우드 Dask 이용하기

이제 Dask 클러스터와 노트북을 모두 클라우드에 만들어 사용하자. GPU 인스턴스 등 필요에 따라 분석 노트북의 사양을 다양하게 선택할 수 있다. 앞의 `test.json` 프로파일을 다음처럼 수정한다.

```json
{
    "$schema": "https://raw.githubusercontent.com/haje01/bilbo/master/schemas/profile-01.schema.json",
    "instance": {
        "ami": "ami-0f49fa254e1806b72",
        "security_group": "sg-0bc538e0a7c089b4d",
        "ec2type": "t3.micro",
        "keyname": "my-keypair",
        "ssh_user": "ubuntu",
        "ssh_private_key": "~/.ssh/my-keypair.pem"
    },
    "notebook": {
        "instance": {
            "ec2type": "g3s.xlarge"
        }
    },
    "dask": {
        "worker": {
            "count": 2
        }
    }
}
```

비교적 저렴한 `g3s.xlarge` GPU 인스턴스를 이용하도록 하였다. 이것으로 클러스터를 만들고,

    $ bilbo create test.json -n test-cluster

    CRITICAL: Create dask cluster 'test-cluster'.
    CRITICAL: Create notebook.
    CRITICAL: Start notebook.
    CRITICAL: Start dask scheduler & workers.
    CRITICAL: Waiting for Dask dashboard ready.

    Name: test-cluster
    Ready Time: 2020-01-09 10:27:46

    Notebook:
    [1] instance_id: i-0c69620cad536bf43, public_ip: 13.124.57.145
    Type: dask

    Scheduler:
    [2] instance_id: i-068c450413c6adf06, public_ip: 54.180.24.43

    Workers:
    [3] instance_id: i-08ae30d974e682df1, public_ip: 52.78.129.196
    [4] instance_id: i-03eb8bd69c8671241, public_ip: 15.165.160.217

AWS EC2 대쉬보드에서 확인하면, 지정된 GPU 타입의 노트북 인스턴스가 잘 생성된 것을 알 수 있다.

![클라우드 노트북](/assets/2020-01-09-10-30-54.png)

로컬 노트북과 다른 점은, 클러스터 정보를 미리 노트북이 알고 있다는 점이다. 예를 Dask 클러스터와 노트북을 함께 클라우드에 만들면 노트북 인스턴스에 `DASK_SCHEDULER_ADDRESS` 환경 변수가 자동으로 설정된다. 앞에서 처럼 Public IP를 붙여넣을 필요없이, 편리하게 Dask 클라이언트를 이용할 수 있다.

클라우드 노트북에 접속하고,

    $ bilbo notebook test-cluster

Dask 클라이언트를 인자 없이 생성할 수 있다.

![주소 없이 Dask 클라이언트 사용](assets/2020-01-09-10-36-48.png)

### 활용 팁

#### 클러스터 재시작

Dask를 사용하다 보면 스케쥴러와 워커 메모리 부족이나, 동작 불안정 등의 이유로 클러스터 재시작이 필요할 수 있다. 이때는 아래와 같이 재시작할 수 있다.

    $ bilbo restart test-cluster

#### 워커 설정

Dask 에서는 워커를 띄울 때, 아래와 같이 옵션으로 몇 개의 프로세스와 스레드를 사용할지와, 메모리 제약을 줄 수 있다.

    $ dask-worker --nprocs 1 --nthreads 2 --memory-limit 1011179520

bilbo 는 이 옵션을 ec2 인스턴스의 CPU 코어 수와 코어당 스레드 수 스펙을 참고하여 자동으로 설정해준다. 예를 들어 코어가 4 개, 코어당 스레드 수가 2, 메모리가 16 GiB 인 `m5.xlarge` 로 워커 하나를 만든다면,

```json
    "dask": {
        "worker": {
            "instance": {
                "ec2type": "m5.xlarge"
            }
        }
    }
```

`nproces`는 코어 수와 같게, `nthreads` 는 코어 당 스레드 수와 같게, `memory-limit`는 전체 메모리 / 코어 수로 설정된다. 즉 다음과 같다.

    $ dask-worker --nprocs 4 --nthreads 2 --memory-limit 4044718080

Dask 대쉬보드에서 확인할 수 있다.

![m5.xlarge 워커](/assets/2020-01-09-16-26-29.png)

작업의 특성에 맞게 커스텀한 값을 사용해야 한다면, 아래와 같이 프로파일에서 할 수 있다.

```json
    "dask": {
        "worker": {
            "instance": {
                "ec2type": "m5.xlarge"
            },
            "nproc": 1,
            "nthread": 4
        }
    }
```

이렇게 하면 스레드를 4개 가진 워커 프로세스 하나가 인스턴스의 메모리를 다 사용하게 된다.