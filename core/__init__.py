import argparse
import logging
from typing import Optional
from pathlib import Path
import sys

from core.utils import debug, info, error
from core.config import Config
#from core.exceptions import UnreachableError


class CustomFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""

    colors = {}
    colors['black']    = '\033[0;30m'
    colors['bblack']   = '\033[1;30m'
    colors['red']      = '\033[0;31m'
    colors['bred']     = '\033[1;31m'
    colors['green']    = '\033[0;32m'
    colors['bgreen']   = '\033[1;32m'
    colors['yellow']   = '\033[0;33m'
    colors['byellow']  = '\033[1;33m'
    colors['blue']     = '\033[0;34m'
    colors['bblue']    = '\033[1;34m'
    colors['magenta']  = '\033[0;35m'
    colors['bmagenta'] = '\033[1;35m'
    colors['cyan']     = '\033[0;36m'
    colors['bcyan']    = '\033[1;36m'
    colors['white']    = '\033[0;37m'
    colors['bwhite']   = '\033[1;37m'
    colors['reset']    = '\033[0m'
    colors['default']    = '\033[0m'

    format = "%(message)s"

    FORMATS = {
        logging.DEBUG: colors['default'] + format + colors['reset'],
        logging.INFO: colors['default'] + format + colors['reset'],
        logging.WARNING: colors['red'] + format + colors['reset'],
        logging.ERROR: colors['bred'] + format + colors['reset'],
        logging.CRITICAL: colors['bred'] + format + colors['reset']
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def get_arg(args, key: str, default: Optional[str]=None):
    try:
        return getattr(args, key)
    except AttributeError:
        return default

def parse_args(state):
    parser = argparse.ArgumentParser(prog='Kakarot', usage='%(prog)s [OPTIONS]', description='P2p stuff',
             formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=42))

    subparsers = parser.add_subparsers(dest="command")

    test = subparsers.add_parser("test", help="build script")
    test.add_argument('-p', '--peers',  help='amount of peers', metavar="AMOUNT", type=int, required=True)
    test.add_argument('-n', '--net-size',  help='size of network in bits', metavar="SIZE", type=int, default=16)
    test.add_argument('-k', '--bucket-size',  help='amount of peers per bucket', metavar="SIZE", type=int, default=5)
    test.add_argument('-D', '--debug', help='enable debugging', action='store_true')

    run = subparsers.add_parser("run", help="build and run script")
    run.add_argument('-p', '--port',  help='listen port', metavar="PORT", type=int, default=666)
    run.add_argument('-u', '--uuid',  help='uuid', metavar="UUID", type=int)
    run.add_argument('-k', '--bucket-size',  help='amount of peers per bucket', metavar="SIZE", type=int, default=5)
    run.add_argument('-n', '--net-size',  help='size of network in bits', metavar="SIZE", type=int, default=16)
    run.add_argument('-D', '--debug', help='enable debugging', action='store_true')

    args = parser.parse_args()
    
    state.do_test = False
    state.do_run = False

    if args.command == "test":
        state.do_test     = True
        state.peers       = args.peers
        state.bucket_size = args.bucket_size
        state.net_size    = args.net_size

    elif args.command == "run":
        state.do_run      = True
        state.port        = args.port
        state.net_size    = args.net_size
        state.bucket_size = args.bucket_size
        state.uuid        = args.uuid
    else:
        parser.print_help()
        sys.exit()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


# init logging
logger = logging.getLogger("kakarot")
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

# program state, mainly changed by command line arguments
state = Config()
parse_args(state)
