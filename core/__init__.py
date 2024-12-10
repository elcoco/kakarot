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
    default_port = 1024
    default_bucket_size = 20
    default_keyspace = 16
    default_alpha = 3

    parser = argparse.ArgumentParser(prog='Kakarot', usage='%(prog)s [OPTIONS]', description='P2p stuff',
             formatter_class=lambda prog: argparse.HelpFormatter(prog,max_help_position=42))

    subparsers = parser.add_subparsers(dest="command")

    test = subparsers.add_parser("test", help="run a bunch of nodes so we can run tests")
    test.add_argument('-p', '--peers',  help='amount of peers', metavar="AMOUNT", type=int, required=True)
    test.add_argument('-k', '--keyspace',  help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    test.add_argument('-b', '--bucket-size',  help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    test.add_argument('-a', '--alpha',  help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    test.add_argument('-D', '--debug', help='enable debugging', action='store_true')

    run = subparsers.add_parser("run", help="run node")
    run.add_argument('-a', '--alpha',  help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    run.add_argument('-b', '--bucket-size',  help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    run.add_argument('-D', '--debug', help='enable debugging', action='store_true')
    run.add_argument('-k', '--keyspace',  help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    run.add_argument('-p', '--port',  help='listen port', metavar="PORT", type=int, default=default_port)
    run.add_argument('-u', '--uuid',  help='uuid', metavar="UUID", type=int)
    run.add_argument('-B', '--bootstrap',   help='bootstrap node address <uuid@ip:port>', metavar="ADDRESS", type=str)

    ping = subparsers.add_parser("ping",     help="run node, perform a ping and exit")
    ping.add_argument('-a', '--alpha',       help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    ping.add_argument('-b', '--bucket-size', help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    ping.add_argument('-D', '--debug',       help='enable debugging', action='store_true')
    ping.add_argument('-k', '--keyspace',    help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    ping.add_argument('-p', '--port',        help='listen port', metavar="PORT", type=int, default=default_port)
    ping.add_argument('-t', '--target',      help='target address: <uuid@ip:port>', metavar="ADDRESS", type=str, required=True)
    ping.add_argument('-u', '--uuid',        help='uuid', metavar="UUID", type=int)

    find_node = subparsers.add_parser("find_node", help="find node and exit")
    find_node.add_argument('-a', '--alpha',       help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    find_node.add_argument('-b', '--bucket-size', help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    find_node.add_argument('-D', '--debug',       help='enable debugging', action='store_true')
    find_node.add_argument('-k', '--keyspace',    help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    find_node.add_argument('-p', '--port',        help='listen port', metavar="PORT", type=int, default=default_port)
    find_node.add_argument('-t', '--target',      help='target uuid', metavar="ADDRESS", type=int, required=True)
    find_node.add_argument('-u', '--uuid',        help='uuid', metavar="UUID", type=int)
    find_node.add_argument('-B', '--bootstrap',   help='bootstrap node address <uuid@ip:port>', metavar="ADDRESS", type=str)

    store = subparsers.add_parser("store", help="store a key value pair")
    store.add_argument('-a', '--alpha',       help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    store.add_argument('-b', '--bucket-size', help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    store.add_argument('-D', '--debug',       help='enable debugging', action='store_true')
    store.add_argument('-k', '--keyspace',    help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    store.add_argument('-p', '--port',        help='listen port', metavar="PORT", type=int, default=default_port)
    store.add_argument('-u', '--uuid',        help='uuid', metavar="UUID", type=int)
    store.add_argument('-B', '--bootstrap',   help='bootstrap node address <uuid@ip:port>', metavar="ADDRESS", type=str)
    store.add_argument('-K', '--key',         help='key of data', metavar="KEY", type=str, required=True)
    store.add_argument('-V', '--value',       help='value of data', metavar="VALUE", type=str, required=True)

    find_key = subparsers.add_parser("find_key", help="get value by key")
    find_key.add_argument('-a', '--alpha',       help='amount of concurrent peer lookups', metavar="AMOUNT", type=int, default=default_alpha)
    find_key.add_argument('-b', '--bucket-size', help='amount of peers per bucket', metavar="SIZE", type=int, default=default_bucket_size)
    find_key.add_argument('-D', '--debug',       help='enable debugging', action='store_true')
    find_key.add_argument('-k', '--keyspace',    help='size of network in bits', metavar="SIZE", type=int, default=default_keyspace)
    find_key.add_argument('-p', '--port',        help='listen port', metavar="PORT", type=int, default=default_port)
    find_key.add_argument('-u', '--uuid',        help='uuid', metavar="UUID", type=int)
    find_key.add_argument('-B', '--bootstrap',   help='bootstrap node address <uuid@ip:port>', metavar="ADDRESS", type=str)
    find_key.add_argument('-K', '--key',         help='key of data', metavar="KEY", type=str, required=True)

    args = parser.parse_args()
    
    state.do_test = False
    state.do_run = False
    state.do_ping = False
    state.do_find_node = False
    state.do_store = False
    state.do_find_key = False

    if args.command == "test":
        state.do_test     = True
        state.peers       = args.peers
        state.bucket_size = args.bucket_size
        state.keyspace    = args.keyspace
        state.alpha       = args.alpha

    elif args.command == "run":
        state.do_run      = True
        state.port        = args.port
        state.keyspace    = args.keyspace
        state.bucket_size = args.bucket_size
        state.uuid        = args.uuid
        state.alpha       = args.alpha
        state.bootstrap   = args.bootstrap
    elif args.command == "ping":
        state.do_ping     = True
        state.port        = args.port
        state.keyspace    = args.keyspace
        state.bucket_size = args.bucket_size
        state.uuid        = args.uuid
        state.alpha       = args.alpha
        state.target      = args.target
    elif args.command == "find_node":
        state.do_find_node = True
        state.port         = args.port
        state.keyspace     = args.keyspace
        state.bucket_size  = args.bucket_size
        state.uuid         = args.uuid
        state.alpha        = args.alpha
        state.target       = args.target
        state.bootstrap    = args.bootstrap
    elif args.command == "store":
        state.do_store     = True
        state.port         = args.port
        state.keyspace     = args.keyspace
        state.bucket_size  = args.bucket_size
        state.uuid         = args.uuid
        state.alpha        = args.alpha
        state.bootstrap    = args.bootstrap
        state.key          = args.key
        state.value        = args.value
    elif args.command == "find_key":
        state.do_store     = True
        state.port         = args.port
        state.keyspace     = args.keyspace
        state.bucket_size  = args.bucket_size
        state.uuid         = args.uuid
        state.alpha        = args.alpha
        state.bootstrap    = args.bootstrap
        state.key          = args.key
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
