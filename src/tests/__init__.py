import argparse
import logging
from typing import Optional
from pathlib import Path
import sys

from core.utils import debug, info, error
from core.config import Config

logger = logging.getLogger("kakarot")


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

    args = parser.parse_args()
    
    state.do_test = False

    if args.command == "test":
        state.do_test     = True
        state.peers       = args.peers
        state.bucket_size = args.bucket_size
        state.keyspace    = args.keyspace
        state.alpha       = args.alpha
    else:
        parser.print_help()
        sys.exit()

    if args.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

# program state, mainly changed by command line arguments
state = Config()
parse_args(state)
