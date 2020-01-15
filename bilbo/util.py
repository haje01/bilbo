"""각종 유틸리티 함수."""
import os
import logging
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler


LOG_FILE = 'bilbo_log.txt'

mod_dir = os.path.dirname(os.path.abspath(__file__))
home_dir = os.path.expanduser('~')
bilbo_dir = os.path.join(home_dir, ".bilbo")
log_dir = os.path.join(bilbo_dir, 'logs')
log_path = os.path.join(log_dir, LOG_FILE)
prof_dir = os.path.join(bilbo_dir, 'profiles')
clust_dir = os.path.join(bilbo_dir, 'clusters')


LOG_FMT = logging.Formatter('%(levelname)s [%(filename)s:%(lineno)d]'
                            ' %(message)s')


def make_dir(dir_name, log=True):
    if log:
        info("make_dir: {}".format(dir_name))
    try:
        os.mkdir(dir_name)
    except PermissionError as e:
        if log:
            error(e)
        error("Can not create {} directory.".format(dir_name))
        sys.exit(-1)


def _check_dirs():
    """필요한 디렉토리 체크."""
    if not os.path.isdir(bilbo_dir):
        make_dir(bilbo_dir, False)
    if not os.path.isdir(log_dir):
        make_dir(log_dir, False)
    if not os.path.isdir(prof_dir):
        make_dir(prof_dir, False)
    if not os.path.isdir(clust_dir):
        make_dir(clust_dir, False)


_check_dirs()


def log_level_from_verbosity(verbosity):
    if verbosity == 0:
        return 40
    elif verbosity == 1:
        return 30
    elif verbosity == 2:
        return 20
    elif verbosity >= 3:
        return 10


def query_stream_log_handler(logger):
    if len(logger.handlers):
        ch = logger.handlers[0]
    else:
        ch = logging.StreamHandler()
        logger.addHandler(ch)
    return ch


def set_log_verbosity(verbosity):
    """Verbosity로 로그 레벨 지정."""
    level = log_level_from_verbosity(verbosity)
    rotfile = RotatingFileHandler(
        log_path,
        maxBytes=1024**2,
        backupCount=5
    )
    rotfile.setLevel(logging.DEBUG)

    console = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)-8s: %(message)s')
    console.setFormatter(formatter)
    console.setLevel(level)

    logging.basicConfig(
        handlers=[rotfile, console],
        level=logging.DEBUG,
        format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def debug(msg):
    """Debug 레벨 로그 메시지."""
    logging.getLogger().debug(msg)


def info(msg):
    """Info 레벨 로그 메시지."""
    logging.getLogger().info(msg)


def warning(msg):
    """Warning 레벨 로그 메시지."""
    logging.getLogger().warning(msg)


def error(msg):
    """Error 로그 메시지."""
    logging.getLogger().error(msg)


def critical(msg):
    """Critical 로그 메시지."""
    logging.getLogger().critical(msg)


def iter_profiles():
    """프로파일을 순회."""
    for prof in os.listdir(prof_dir):
        if prof.endswith('.json'):
            yield prof


def iter_clusters():
    """프로파일을 순회."""
    for cl in os.listdir(clust_dir):
        if cl.endswith('.json'):
            yield cl


def check_aws_envvars():
    """AWS 관련 환경변수를 체크.

    Raises:
        RuntimeError: 환경 변수가 없을 때

    """

    def _check(ev):
        if ev not in os.environ:
            raise RuntimeError("Environment variable not found: '{}'".
                               format(ev))

    _check('AWS_ACCESS_KEY_ID')
    _check('AWS_SECRET_ACCESS_KEY')
    _check('AWS_DEFAULT_REGION')


def get_aws_config():
    """AWS 관련 정보 획득.

    aws cli 의 credential/config 가 있으면 그것을 우선하고, 아니면 환경 변수에서 얻음

    Returns:
        tuple: Access Key, Secret Key, Region

    """
    info("get_aws_config")
    # aws cli 의 credential/config 가 있으면 그것을 우선
    aws_dir = os.path.join(home_dir, '.aws')
    if os.path.isdir(aws_dir):
        cred_path = os.path.join(aws_dir, 'credentials')
        cfg_path = os.path.join(aws_dir, 'config')
        if os.path.isfile(cred_path) and os.path.isfile(cfg_path):
            return get_aws_config_from_cfg(cred_path, cfg_path)
    return get_aws_config_from_envvars()


def get_aws_config_from_cfg(cred_path, cfg_path):
    """AWS CLI Credentials/Config 파일에서 설정을 얻음."""
    info("get_aws_config_from_cfg : {}, {}".format(cred_path, cfg_path))
    cred = ConfigParser()
    cred.read(cred_path)
    cfg = ConfigParser()
    cfg.read(cfg_path)
    ak = cred['default']['aws_access_key_id']
    sa = cred['default']['aws_secret_access_key']
    dr = cfg['default']['region']
    return ak, sa, dr


def get_aws_config_from_envvars():
    """환경 변수에서 AWS 설정을 얻음."""
    info("get_aws_config_from_envvars")
    check_aws_envvars()
    ak = os.environ['AWS_ACCESS_KEY_ID']
    sa = os.environ['AWS_SECRET_ACCESS_KEY']
    dr = os.environ['AWS_DEFAULT_REGION']
    return ak, sa, dr
