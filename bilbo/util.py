"""각종 유틸리티 함수."""
import os
import logging
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
